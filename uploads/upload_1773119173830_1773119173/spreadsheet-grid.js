// ── Spreadsheet Grid (pure JS — no external dependencies) ───────────────────
// Drop-in replacement for the Handsontable-based grid.
// Works entirely offline, no CDN, no tracking prevention issues.

(function () {
  'use strict';

  // ── State ──────────────────────────────────────────────────────────────────
  let _data        = [];
  let _rows        = 0;
  let _cols        = 0;
  let _selection   = null; // { r, c }
  let _container   = null;
  let _table       = null;
  let _activeInput = null;

  const MIN_ROWS = 20;
  const MIN_COLS = 8;
  const CELL_W   = 120; // px

  // ── Public API ─────────────────────────────────────────────────────────────

  window.initSpreadsheetGrid = function (csvContent) {
    _container = document.getElementById('spreadsheetContainer');
    if (!_container) return false;

    _data = parseCSV(csvContent);
    _normalise();
    _render();
    _updateDimensions();
    return true;
  };

  window.addRowToGrid = function () {
    _data.push(Array(_cols).fill(''));
    _rows++;
    _render();
    _updateDimensions();
    _markModified();
  };

  window.addColToGrid = function () {
    _cols++;
    _data = _data.map(r => { r.push(''); return r; });
    _render();
    _updateDimensions();
    _markModified();
  };

  window.gridToCSV = function () {
    _commitActiveInput();
    return _data.map(row =>
      row.map(cell => {
        const s = cell == null ? '' : String(cell);
        return (s.includes(',') || s.includes('"') || s.includes('\n'))
          ? '"' + s.replace(/"/g, '""') + '"'
          : s;
      }).join(',')
    ).join('\n');
  };

  window.saveSpreadsheetFile = async function () {
    if (!window.editorCurrentFile) return;

    const filePath = window.editorCurrentFile.path || window.editorCurrentFile;
    const fileName = window.editorCurrentFile.name || filePath.split(/[\\/]/).pop();

    const saveBtn = document.getElementById('saveBtn');
    if (saveBtn) { saveBtn.disabled = true; saveBtn.innerHTML = '<span class="spinner"></span> SAVING...'; }

    try {
      const csv  = window.gridToCSV();
      const resp = await fetch('/api/files/save-xlsx', {
        method:  'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Username':   window._currentUser || 'Anonymous',
        },
        body: JSON.stringify({ path: filePath, content: csv }),
      });
      const result = await resp.json();

      // Always re-enable save button regardless of outcome
      if (saveBtn) { saveBtn.disabled = false; saveBtn.innerHTML = '💾 SAVE'; }

      if (result.ok) {
        window.editorModified = false;

        // Reset filename display (clear the "modified" colour)
        const fnEl = document.getElementById('editorFilename');
        if (fnEl) {
          fnEl.textContent = fileName;
          fnEl.style.color = 'var(--text-bright)';
        }

        if (result.changed) {
          displaySpreadsheetDiff(result);
          if (typeof toast === 'function')
            toast(`Saved — +${result.lines_added} / -${result.lines_removed} rows`, 'success');
        } else {
          if (typeof toast === 'function') toast('Saved (no changes)', 'info');
        }
      } else {
        if (typeof toast === 'function') toast('Save failed: ' + (result.error || 'unknown'), 'error');
      }
    } catch (e) {
      if (saveBtn) { saveBtn.disabled = false; saveBtn.innerHTML = '💾 SAVE'; }
      if (typeof toast === 'function') toast('Save error: ' + e.message, 'error');
    }
  };

  // ── CSV parser ─────────────────────────────────────────────────────────────

  window.parseCSV = function (csvStr) {
    const rows = [];
    if (!csvStr || !csvStr.trim()) return [[]];
    const lines = csvStr.split(/\r?\n/);
    for (let li = 0; li < lines.length; li++) {
      const line = lines[li];
      if (line === '' && li === lines.length - 1) continue;
      const row = [];
      let field = '', inQ = false;
      for (let i = 0; i < line.length; i++) {
        const ch = line[i];
        if (inQ) {
          if (ch === '"' && line[i + 1] === '"') { field += '"'; i++; }
          else if (ch === '"') inQ = false;
          else field += ch;
        } else {
          if (ch === '"') inQ = true;
          else if (ch === ',') { row.push(field); field = ''; }
          else field += ch;
        }
      }
      row.push(field);
      rows.push(row);
    }
    return rows.length ? rows : [[]];
  };

  // ── Normalise to uniform grid size ─────────────────────────────────────────

  function _normalise() {
    _cols = Math.max(MIN_COLS, ..._data.map(r => r.length));
    _data = _data.map(r => {
      while (r.length < _cols) r.push('');
      return r;
    });
    while (_data.length < MIN_ROWS) _data.push(Array(_cols).fill(''));
    _rows = _data.length;
  }

  // ── Render table ───────────────────────────────────────────────────────────

  function _render() {
    if (!_container) return;
    _container.innerHTML = '';
    _container.style.cssText =
      'overflow:auto;height:100%;font-family:var(--mono,monospace);font-size:12px;' +
      'line-height:1.4;background:var(--bg2,#161b22);' +
      'border:1px solid var(--border,#30363d);border-radius:6px;position:relative';

    _table = document.createElement('table');
    _table.style.cssText = 'border-collapse:collapse;table-layout:fixed;min-width:100%;user-select:none';

    // Header row
    const thead = _table.createTHead();
    const hrow  = thead.insertRow();
    _th(hrow, '#', true, true);
    for (let c = 0; c < _cols; c++) _th(hrow, _colLabel(c), false, true);

    // Data rows
    const tbody = _table.createTBody();
    for (let r = 0; r < _rows; r++) {
      const tr = tbody.insertRow();
      // Row number
      const rn = tr.insertCell();
      rn.style.cssText = _thCSS(true);
      rn.textContent = r + 1;
      // Cells
      for (let c = 0; c < _cols; c++) {
        const td = tr.insertCell();
        td.dataset.r = r;
        td.dataset.c = c;
        td.style.cssText = _tdCSS(r, c, false);
        td.textContent = _data[r][c] || '';
        td.setAttribute('tabindex', '0');
        td.addEventListener('click',    _onCellClick);
        td.addEventListener('dblclick', _onCellDblClick);
        td.addEventListener('keydown',  _onCellKey);
      }
    }

    _container.appendChild(_table);
  }

  function _th(row, label, corner, isHeader) {
    const el = document.createElement(isHeader ? 'th' : 'td');
    el.style.cssText = _thCSS(corner);
    el.textContent = label;
    row.appendChild(el);
  }

  function _thCSS(isCorner) {
    return [
      'position:sticky',
      isCorner ? 'left:0;top:0;z-index:3' : 'top:0;z-index:2',
      'background:var(--bg3,#1c2128)',
      'color:var(--dim,#8b949e)',
      'font-size:10px;font-weight:600;text-align:center',
      'padding:4px 6px',
      'border-right:1px solid var(--border,#30363d)',
      'border-bottom:1px solid var(--border,#30363d)',
      isCorner ? 'min-width:36px;max-width:36px' : `min-width:${CELL_W}px;max-width:${CELL_W}px`,
      'white-space:nowrap;overflow:hidden;box-sizing:border-box',
    ].join(';');
  }

  function _tdCSS(r, c, selected) {
    return [
      `padding:3px 6px`,
      `min-width:${CELL_W}px;max-width:${CELL_W}px`,
      'white-space:nowrap;overflow:hidden;text-overflow:ellipsis',
      'border-right:1px solid var(--border,#30363d)',
      'border-bottom:1px solid rgba(48,54,61,.5)',
      'cursor:cell;outline:none;box-sizing:border-box',
      'color:var(--text,#e6edf3)',
      selected
        ? 'background:rgba(57,217,138,.18);box-shadow:inset 0 0 0 2px var(--green,#39d98a)'
        : (r % 2 === 0 ? 'background:transparent' : 'background:rgba(255,255,255,.02)'),
    ].join(';');
  }

  // ── Events ─────────────────────────────────────────────────────────────────

  function _onCellClick(e) {
    _commitActiveInput();
    _select(+e.currentTarget.dataset.r, +e.currentTarget.dataset.c);
  }

  function _onCellDblClick(e) {
    const td = e.currentTarget;
    _startEdit(+td.dataset.r, +td.dataset.c, td);
  }

  function _onCellKey(e) {
    if (e.target !== e.currentTarget) return;
    const r = +e.currentTarget.dataset.r;
    const c = +e.currentTarget.dataset.c;

    if (e.key === 'Enter' || e.key === 'F2') { e.preventDefault(); _startEdit(r, c, e.currentTarget); return; }
    if (e.key === 'Delete' || e.key === 'Backspace') {
      _data[r][c] = ''; e.currentTarget.textContent = ''; _markModified(); return;
    }
    const nav = { ArrowUp:[-1,0], ArrowDown:[1,0], ArrowLeft:[0,-1], ArrowRight:[0,1], Tab:[0,1] };
    if (nav[e.key]) {
      e.preventDefault();
      const [dr, dc] = nav[e.key];
      _select(Math.max(0, Math.min(_rows-1, r+dr)), Math.max(0, Math.min(_cols-1, c+dc)));
      return;
    }
    if (e.key.length === 1 && !e.ctrlKey && !e.metaKey) _startEdit(r, c, e.currentTarget, e.key);
  }

  // ── Selection ──────────────────────────────────────────────────────────────

  function _select(r, c) {
    if (_selection) {
      const old = _cellEl(_selection.r, _selection.c);
      if (old) old.style.cssText = _tdCSS(_selection.r, _selection.c, false);
    }
    _selection = { r, c };
    const el = _cellEl(r, c);
    if (el) { el.style.cssText = _tdCSS(r, c, true); el.focus(); }
  }

  // ── Inline editing ─────────────────────────────────────────────────────────

  function _startEdit(r, c, td, initChar) {
    _commitActiveInput();
    td.textContent = '';
    const inp = document.createElement('input');
    inp.type  = 'text';
    inp.value = initChar != null ? initChar : (_data[r][c] || '');
    inp.style.cssText =
      'width:100%;height:100%;border:none;outline:none;background:transparent;' +
      'color:var(--text,#e6edf3);font:inherit;padding:0;box-sizing:border-box';
    td.style.cssText = _tdCSS(r, c, true) + ';padding:0 4px;box-shadow:inset 0 0 0 2px var(--accent,#58a6ff)';
    td.appendChild(inp);
    inp.focus();
    if (initChar == null) inp.select();
    _activeInput = { inp, r, c, td };

    inp.addEventListener('keydown', (e) => {
      e.stopPropagation();
      if (e.key === 'Enter' || e.key === 'Tab') {
        e.preventDefault();
        const pr = r, pc = c;
        _commitActiveInput();
        _select(
          e.key === 'Enter' ? Math.min(pr+1, _rows-1) : pr,
          e.key === 'Tab'   ? Math.min(pc+1, _cols-1) : pc
        );
      }
      if (e.key === 'Escape') { inp.value = _data[r][c] || ''; _commitActiveInput(true); }
    });
    inp.addEventListener('blur', () => setTimeout(_commitActiveInput, 80));
  }

  function _commitActiveInput(cancel) {
    if (!_activeInput) return;
    const { inp, r, c, td } = _activeInput;
    _activeInput = null;
    const val = cancel ? (_data[r][c] || '') : inp.value;
    if (!cancel && val !== (_data[r][c] || '')) { _data[r][c] = val; _markModified(); }
    td.textContent = _data[r][c] || '';
    td.style.cssText = _tdCSS(r, c, _selection && _selection.r === r && _selection.c === c);
  }

  // ── Helpers ────────────────────────────────────────────────────────────────

  function _cellEl(r, c) {
    if (!_table || !_table.tBodies[0]) return null;
    const tr = _table.tBodies[0].rows[r];
    return tr ? tr.cells[c + 1] : null; // +1 for row-number cell
  }

  function _colLabel(i) {
    let s = ''; i++;
    while (i > 0) { s = String.fromCharCode(64 + (i % 26 || 26)) + s; i = Math.floor((i-1)/26); }
    return s;
  }

  function _updateDimensions() {
    const el = document.getElementById('gridDimensions');
    if (el) el.textContent = `${_rows} rows × ${_cols} cols`;
  }

  function _markModified() {
    window.editorModified = true;
    const saveBtn = document.getElementById('saveBtn');
    if (saveBtn) saveBtn.disabled = false;
  }

  // ── Diff display ───────────────────────────────────────────────────────────
  //
  // The standard #diffPane lives inside #editorPaneWrap, which is hidden when
  // a spreadsheet is open. So we inject a dedicated #spreadsheetDiffPane into
  // #spreadsheetPaneWrap and use that instead. Falls back to #diffPane if the
  // wrapper element isn't found (e.g. in a custom layout).

  function _getOrCreateDiffPane() {
    // Prefer the dedicated spreadsheet diff pane
    let pane = document.getElementById('spreadsheetDiffPane');
    if (pane) return pane;

    // Inject one at the bottom of the spreadsheet wrapper
    const wrap = document.getElementById('spreadsheetPaneWrap');
    if (wrap) {
      pane = document.createElement('div');
      pane.id = 'spreadsheetDiffPane';
      pane.style.cssText =
        'display:none;flex-direction:column;' +
        'border-top:1px solid var(--border,#30363d);' +
        'background:var(--bg1,#0d1117);flex-shrink:0;max-height:260px;overflow:hidden';

      // Header bar
      const header = document.createElement('div');
      header.style.cssText =
        'display:flex;align-items:center;justify-content:space-between;' +
        'padding:8px 14px;border-bottom:1px solid var(--border,#30363d);' +
        'background:var(--bg2,#161b22);flex-shrink:0;gap:10px';
      header.innerHTML =
        '<span style="font-family:var(--mono,monospace);font-size:10px;' +
        'letter-spacing:2px;color:var(--dim,#8b949e);flex-shrink:0">SPREADSHEET DIFF</span>' +
        '<span id="spreadsheetDiffStats" style="font-family:var(--mono,monospace);' +
        'font-size:11px;display:flex;gap:6px;align-items:center;flex:1"></span>' +
        '<button onclick="document.getElementById(\'spreadsheetDiffPane\').style.display=\'none\'" ' +
        'style="background:none;border:none;cursor:pointer;color:var(--dim,#8b949e);' +
        'font-size:16px;line-height:1;padding:0;flex-shrink:0">✕</button>';

      const content = document.createElement('div');
      content.id = 'spreadsheetDiffContent';
      content.style.cssText = 'overflow-y:auto;flex:1';

      pane.appendChild(header);
      pane.appendChild(content);
      wrap.appendChild(pane);
      return pane;
    }

    // Last resort: fall back to the standard text-editor diffPane
    return document.getElementById('diffPane');
  }

  window.displaySpreadsheetDiff = function (result) {
    const diffPane = _getOrCreateDiffPane();
    if (!diffPane) return;

    // Resolve stats/content child elements based on which pane we got
    const isDedicated = diffPane.id === 'spreadsheetDiffPane';
    const diffStats   = isDedicated
      ? document.getElementById('spreadsheetDiffStats')
      : document.getElementById('diffStats');
    const diffContent = isDedicated
      ? document.getElementById('spreadsheetDiffContent')
      : document.getElementById('diffContent');

    if (!diffStats || !diffContent) return;

    const diff    = result.diff    || [];
    const added   = result.lines_added   || 0;
    const removed = result.lines_removed || 0;
    const oldRows = result.old_rows ?? '?';
    const newRows = result.new_rows ?? '?';

    if (!result.changed && diff.length === 0) {
      diffPane.style.display = 'none';
      return;
    }

    diffStats.innerHTML =
      `<span style="color:var(--green,#39d98a)">+${added} row${added!==1?'s':''} added</span>` +
      `<span style="color:var(--dim,#8b949e);margin:0 6px">·</span>` +
      `<span style="color:var(--red,#ff5c5c)">-${removed} row${removed!==1?'s':''} removed</span>` +
      `<span style="color:var(--dim,#8b949e);margin:0 6px">·</span>` +
      `<span style="color:var(--dim,#8b949e)">${oldRows} → ${newRows} total rows</span>`;

    const hasChanges = diff.some(e => e.type === 'added' || e.type === 'removed');
    if (!hasChanges) {
      diffContent.innerHTML =
        `<div style="padding:12px 14px;font-family:var(--mono,monospace);` +
        `font-size:11px;color:var(--dim,#8b949e)">No cell changes detected.</div>`;
      diffPane.style.display = 'flex';
      return;
    }

    // Context windowing — show MAX_CTX equal rows around each changed row
    const MAX_CTX = 2;
    const visible = new Set();
    diff.forEach((e, i) => {
      if (e.type !== 'equal') {
        for (let j = Math.max(0, i-MAX_CTX); j <= Math.min(diff.length-1, i+MAX_CTX); j++) visible.add(j);
      }
    });

    let html = '';
    let prevVis = true;
    let collapseCount = 0;

    for (let i = 0; i <= diff.length; i++) {
      const entry = i < diff.length ? diff[i] : null;
      const isVis = entry && visible.has(i);

      if (entry && !isVis) {
        collapseCount++;
        prevVis = false;
        continue;
      }

      if (!prevVis && collapseCount > 0) {
        html +=
          `<div style="padding:2px 14px;font-family:var(--mono,monospace);font-size:10px;` +
          `font-style:italic;color:var(--dim,#8b949e)">` +
          `  ··· ${collapseCount} unchanged row${collapseCount!==1?'s':''} ···</div>`;
        collapseCount = 0;
      }

      if (!entry) break;
      prevVis = true;

      const cells  = _parseDiffRow(entry.text || '');
      const ln     = entry.type === 'added'   ? entry.new_ln
                   : entry.type === 'removed' ? entry.old_ln
                   : entry.new_ln;
      const lnStr  = String(ln || '').padStart(4, '\u00a0');
      const prefix = entry.type === 'added' ? '+' : entry.type === 'removed' ? '−' : '\u00a0';
      const rowBg  = entry.type === 'added'   ? 'rgba(57,217,138,.1)'
                   : entry.type === 'removed' ? 'rgba(255,92,92,.1)'
                   : 'transparent';
      const fgCol  = entry.type === 'added'   ? 'var(--green,#39d98a)'
                   : entry.type === 'removed' ? 'var(--red,#ff5c5c)'
                   : 'var(--dim,#8b949e)';

      html +=
        `<div style="display:flex;align-items:center;background:${rowBg};` +
        `font-family:var(--mono,monospace);font-size:11px;line-height:1.6;` +
        `border-bottom:1px solid rgba(255,255,255,.04);min-height:24px">` +
          `<span style="color:var(--dim,#8b949e);min-width:40px;padding:0 8px;` +
          `text-align:right;user-select:none;flex-shrink:0;font-size:10px">${lnStr}</span>` +
          `<span style="color:${fgCol};min-width:14px;flex-shrink:0;` +
          `text-align:center;font-weight:bold">${prefix}</span>` +
          `<span style="flex:1;overflow:hidden;padding:0 8px 0 4px;` +
          `display:flex;gap:6px;align-items:center">` +
            _renderCells(cells, entry.type) +
          `</span>` +
        `</div>`;
    }

    diffContent.innerHTML = html;
    diffPane.style.display = 'flex';
  };

  function _parseDiffRow(rowStr) {
    const cells = [];
    let field = '', inQ = false;
    for (let i = 0; i < rowStr.length; i++) {
      const ch = rowStr[i];
      if (inQ) {
        if (ch === '"' && rowStr[i+1] === '"') { field += '"'; i++; }
        else if (ch === '"') inQ = false;
        else field += ch;
      } else {
        if (ch === '"') inQ = true;
        else if (ch === ',') { cells.push(field); field = ''; }
        else field += ch;
      }
    }
    cells.push(field);
    while (cells.length > 1 && cells[cells.length-1] === '') cells.pop();
    return cells;
  }

  function _renderCells(cells, type) {
    if (!cells || !cells.length) return `<span style="opacity:.3;font-style:italic">(empty)</span>`;
    const accent = type === 'added'   ? 'var(--green,#39d98a)'
                 : type === 'removed' ? 'var(--red,#ff5c5c)'
                 : 'var(--dim,#8b949e)';
    return cells.map(cell => {
      const safe    = (cell||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
      const isEmpty = !cell.trim();
      return `<span style="display:inline-block;max-width:160px;overflow:hidden;` +
        `text-overflow:ellipsis;white-space:nowrap;` +
        `${isEmpty ? 'opacity:.3;font-style:italic' : ''}` +
        `${type !== 'equal' ? `;border-bottom:1px solid ${accent}` : ''}` +
        `" title="${safe}">${isEmpty ? '∅' : safe}</span>`;
    }).join('');
  }

})();