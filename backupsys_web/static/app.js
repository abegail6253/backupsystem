'use strict';

let currentPage   = 'dashboard';
let settings      = { auto_backup: false, interval_min: 30, destination: './backups', storage_type: 'local', retention_days: 30 };
let newWatchType  = 'local';
let selectedBackup = null;
let pollTimers    = {};

let editorCurrentFile       = null;
let editorOriginalContent   = '';
let editorCurrentWatchId    = null;
let editorCurrentBrowsePath = null;
let editorModified          = false;

const _allPending = {};
let   _allBackups = [];
let   _historyTotal = 0;
let   _historyPage  = 1;
let   _historyPages = 1;
let   _findMatches = [], _findCurrent = -1;
let   _allFileItems = [];

// Preserve history filter across loadHistory() calls
let _historyFilterWatch  = '';
let _historyFilterStatus = '';
let _historyFilterQ      = '';
let _historyFilterFrom   = '';
let _historyFilterTo     = '';

// Dashboard error back-off
let _dashErrorCount = 0;
const _DASH_MAX_ERRORS = 5;

// Dashboard auto-refresh countdown
let _dashRefreshCountdown = 5;
let _dashCountdownTimer   = null;
let _dashPollTimer        = null;

// Context menu state
let _ctxTarget     = null;
let _watchCtxTarget = null;

// Track running backups for cancel support
const _runningBackups = new Set();

// History bulk-select
let _historySelected = new Set();

// Audio notification
let _audioEnabled = localStorage.getItem('bsys_audio') !== 'off';

// ── Desktop Notifications ─────────────────────────────────────────────────────

let _desktopNotifEnabled = false;


const _backupStartTimes = {};

function exportHistoryCSV() {
  if (!_allBackups.length) { toast('No backups to export', 'warn'); return; }
  if (_historyTotal > _allBackups.length) {
    toast(`Exporting ${_allBackups.length} of ${_historyTotal} — scroll down and load more to get all`, 'warn');
  }
  const headers = ['Backup ID','Watch','Timestamp','Status','Type','Files Copied','Size','Duration (s)','Notes'];
  const rows = _allBackups.map(b => [
    b.backup_id || '',
    (b.watch_name || '').replace(/,/g, ';'),
    b.timestamp || '',
    b.status || '',
    b.incremental ? 'incremental' : 'full',
    b.files_copied || 0,
    b.total_size || '',
    b.duration_s || '',
    (b.user_notes || '').replace(/,/g, ';').replace(/\n/g, ' '),
  ]);
  const csv = [headers, ...rows].map(r => r.join(',')).join('\n');
  const blob = new Blob([csv], { type: 'text/csv' });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  a.href     = url;
  a.download = `backup_history_${new Date().toISOString().slice(0,10)}.csv`;
  document.body.appendChild(a); a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
  toast(`Exported ${_allBackups.length} records as CSV`, 'success');
}

async function requestDesktopNotifications() {
  if (!('Notification' in window)) { toast('Browser notifications not supported', 'warn'); return; }
  if (Notification.permission === 'granted') {
    _desktopNotifEnabled = true;
    toast('Desktop notifications enabled', 'success');
    return;
  }
  const perm = await Notification.requestPermission();
  _desktopNotifEnabled = perm === 'granted';
  toast(_desktopNotifEnabled ? 'Desktop notifications enabled' : 'Permission denied', _desktopNotifEnabled ? 'success' : 'warn');
}

function _desktopNotify(title, body) {
  if (!_desktopNotifEnabled || Notification.permission !== 'granted') return;
  if (document.hasFocus()) return; // only notify when window is in background
  try { new Notification(title, { body, icon: '' }); } catch (_) {}
}


// History live-poll for running backups
let _historyPollTimer = null;

const _backupCache = {};


// ── Favicon ───────────────────────────────────────────────────────────────────

(function() {
  const canvas = document.createElement('canvas');
  canvas.width = canvas.height = 32;
  const ctx = canvas.getContext('2d');
  ctx.font = '24px serif';
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  ctx.fillText('🛡', 16, 16);
  const link = document.createElement('link');
  link.rel = 'icon';
  link.href = canvas.toDataURL();
  document.head.appendChild(link);
})();


// ── Theme ─────────────────────────────────────────────────────────────────────

function _applyTheme(dark) {
  document.documentElement.setAttribute('data-theme', dark ? 'dark' : 'light');
  const btn = document.getElementById('themeToggle');
  if (btn) btn.textContent = dark ? '☀' : '🌙';
}

function toggleTheme() {
  const isDark = document.documentElement.getAttribute('data-theme') !== 'light';
  localStorage.setItem('bsys_theme', isDark ? 'light' : 'dark');
  _applyTheme(!isDark);
  if (currentPage === 'dashboard') _renderActivityChart();
}

(function() {
  const saved = localStorage.getItem('bsys_theme');
  _applyTheme(saved !== 'light');
})();


// ── Audio notification ────────────────────────────────────────────────────────

function _playBeep(type = 'success') {
  if (!_audioEnabled) return;
  try {
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.connect(gain);
    gain.connect(ctx.destination);
    osc.frequency.value = type === 'success' ? 880 : 440;
    osc.type = 'sine';
    gain.gain.setValueAtTime(0.15, ctx.currentTime);
    gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.4);
    osc.start(ctx.currentTime);
    osc.stop(ctx.currentTime + 0.4);
  } catch (_) {}
}

function toggleAudioNotifications() {
  _audioEnabled = !_audioEnabled;
  localStorage.setItem('bsys_audio', _audioEnabled ? 'on' : 'off');
  const btn = document.getElementById('audioToggleBtn');
  if (btn) btn.textContent = _audioEnabled ? '🔔 SOUND ON' : '🔕 SOUND OFF';
  toast(_audioEnabled ? 'Sound notifications on' : 'Sound notifications off', 'info');
}

function toggleEmailNotif() {
  const on = document.getElementById('emailToggle').classList.toggle('on');
  document.getElementById('emailLabel').textContent = on ? 'ENABLED' : 'DISABLED';
  document.getElementById('emailLabel').style.color = on ? 'var(--green)' : 'var(--dim)';
  document.getElementById('emailFields').style.display = on ? 'flex' : 'none';
}


// ── beforeunload guard ────────────────────────────────────────────────────────

window.addEventListener('beforeunload', e => {
  if (editorModified) {
    e.preventDefault();
    e.returnValue = '';
  }
});

document.addEventListener('visibilitychange', () => {
  if (!document.hidden && currentPage === 'dashboard') {
    _dashErrorCount = 0; // allow a fresh attempt after returning to tab
    loadDashboard();
  }
});


// ── Navigation ────────────────────────────────────────────────────────────────

function showPage(n, btn) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-tab').forEach(t => t.classList.remove('active'));
  document.getElementById('page-' + n).classList.add('active');
  if (btn) btn.classList.add('active');

  if (currentPage === 'history' && n !== 'history') _stopHistoryPoll();

  currentPage = n;
  if (n === 'dashboard') loadDashboard();
  if (n === 'watches')   loadWatches();
  if (n === 'editor')    loadEditorSidebar();
  if (n === 'history')   loadHistory();
  if (n === 'settings')  loadSettings();
}

document.addEventListener('keydown', e => {
  if (e.altKey && !e.ctrlKey && !e.metaKey) {
    const pages = ['dashboard', 'watches', 'editor', 'history', 'settings'];
    const idx   = parseInt(e.key) - 1;
    if (idx >= 0 && idx < pages.length) {
      e.preventDefault();
      const tabs = document.querySelectorAll('.nav-tab');
      showPage(pages[idx], tabs[idx] || null);
    }
  }
});


// ── Toast ─────────────────────────────────────────────────────────────────────

let _lastToastMsg  = '';
let _lastToastTime = 0;
function toast(msg, type = 'success') {
  const now = Date.now();
  if (msg === _lastToastMsg && now - _lastToastTime < 4000) return;
  _lastToastMsg  = msg;
  _lastToastTime = now;

  const t = document.getElementById('toast');
  t.className = `toast ${type}`;
  const icons = { success: '✓', error: '✗', info: 'ℹ', warn: '⚠' };
  t.innerHTML = (icons[type] || 'ℹ') + ' ' + msg;
  t.classList.add('show');
  clearTimeout(t._timer);
  t._timer = setTimeout(() => t.classList.remove('show'), 3500);
}


// ── Formatters ────────────────────────────────────────────────────────────────

function fmtTs(iso) {
  if (!iso) return 'Never';
  return new Date(iso).toLocaleString('en-US', { month: 'short', day: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit' });
}

function changeBadge(t) {
  const m = { added: 'A', modified: 'M', deleted: 'D', renamed: 'R' };
  const c = { added: 'badge-A', modified: 'badge-M', deleted: 'badge-D', renamed: 'badge-R' };
  return `<span class="change-badge ${c[t] || ''}">${m[t] || '?'}</span>`;
}

function fmtSize(b) {
  if (!b || b < 1024) return (b || 0) + 'B';
  if (b < 1048576) return (b / 1024).toFixed(1) + 'KB';
  if (b < 1073741824) return (b / 1048576).toFixed(1) + 'MB';
  return (b / 1073741824).toFixed(1) + 'GB';
}

function fmtDuration(s) {
  if (!s) return '0s';
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60), sec = s % 60;
  return `${m}m ${sec}s`;
}

async function _renderActivityChart() {
  const canvas = document.getElementById('activityChart');
  if (!canvas) return;
  try {
    const r = await fetch('/api/history/activity?days=14');
    const data = await r.json();
    const activity = data.activity || {};
    const W = canvas.offsetWidth || canvas.parentElement?.offsetWidth || 800;
    canvas.width  = W;
    canvas.height = 64;
    const ctx = canvas.getContext('2d');
    if (!ctx.roundRect) {
      ctx.roundRect = function(x, y, w, h, r) {
        this.beginPath();
        this.moveTo(x + r, y);
        this.lineTo(x + w - r, y);
        this.quadraticCurveTo(x + w, y, x + w, y + r);
        this.lineTo(x + w, y + h - r);
        this.quadraticCurveTo(x + w, y + h, x + w - r, y + h);
        this.lineTo(x + r, y + h);
        this.quadraticCurveTo(x, y + h, x, y + h - r);
        this.lineTo(x, y + r);
        this.quadraticCurveTo(x, y, x + r, y);
        this.closePath();
      };
    }
    ctx.clearRect(0, 0, W, 64);
    const isDark = document.documentElement.getAttribute('data-theme') !== 'light';

    // Build 14-day date list
    const days = 14;
    const sortedDays = [];
    for (let i = days - 1; i >= 0; i--) {
      const d = new Date();
      d.setDate(d.getDate() - i);
      sortedDays.push(d.toISOString().slice(0, 10));
    }

    const maxVal = Math.max(1, ...sortedDays.map(d => {
      const b = activity[d] || {};
      return (b.success || 0) + (b.failed || 0) + (b.cancelled || 0);
    }));

    const barW  = Math.floor((W - 20) / days) - 3;
    const maxH  = 42;
    const baseY = 52;

    sortedDays.forEach((day, i) => {
      const b     = activity[day] || { success: 0, failed: 0, cancelled: 0 };
      const total = b.success + b.failed + b.cancelled;
      const x     = 10 + i * (barW + 3);

      // background
      ctx.fillStyle = isDark ? '#111720' : '#dce8f0';
      ctx.beginPath();
      ctx.roundRect(x, baseY - maxH, barW, maxH, 3);
      ctx.fill();

      if (total > 0) {
        let y = baseY;
        const draw = (count, color) => {
          if (!count) return;
          const h = Math.round((count / maxVal) * maxH);
          ctx.fillStyle = color;
          ctx.fillRect(x, y - h, barW, h);
          y -= h;
        };
        draw(b.failed,    isDark ? '#ff5c5c' : '#c02020');
        draw(b.cancelled, isDark ? '#f5a623' : '#c07800');
        draw(b.success,   isDark ? '#39d98a' : '#1a8a4a');
      }

      // date label every 2 bars
      if (i % 2 === 0 || i === days - 1) {
        ctx.fillStyle   = isDark ? '#4a6070' : '#7a9ab0';
        ctx.font        = '8px monospace';
        ctx.textAlign   = 'center';
        ctx.fillText(day.slice(5), x + barW / 2, 63);
      }
    });
  } catch (_) {}
}

function selectBackupById(id) {
  const b = _allBackups.find(x => x.backup_id === id) || _backupCache[id];
  if (b) selectBackup(b);
}

function escapeHtml(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function fileIcon(item) {
  if (item.is_dir) return '📁';

  const e = (item.ext || '').toLowerCase();

  // Programming files
  if (['.js', '.ts', '.jsx', '.tsx'].includes(e)) return '🟨';
  if (e === '.py') return '🐍';
  if (['.html', '.htm'].includes(e)) return '🌐';
  if (e === '.css') return '🎨';
  if (e === '.json') return '{}';
  if (e === '.md') return '📝';
  if (['.sh', '.bat', '.ps1'].includes(e)) return '⚙';

  // Config / markup files
  if (['.yml', '.yaml', '.toml', '.ini', '.cfg', '.lock'].includes(e)) return '📝';
  if (item.name === '.env') return '🔑';

  // Database files
  if (['.sql', '.sqlite', '.db'].includes(e)) return '🗄️';

  // Images
  if (['.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp'].includes(e)) return '🖼';

  // Documents
  if (['.docx', '.doc'].includes(e)) return '📘';
  if (e === '.pdf') return '📕';

  // Archives
  if (['.zip', '.rar', '.7z'].includes(e)) return '🗜';

  // Git / tooling
  if (['.gitignore', '.dockerignore', '.editorconfig'].includes(e)) return '🔧';

  // Default fallback
  return '📄';
}


// ── Dashboard ─────────────────────────────────────────────────────────────────

function _startDashCountdown() {
  clearInterval(_dashCountdownTimer);
  _dashRefreshCountdown = 5;
  _updateCountdownUI();
  _dashCountdownTimer = setInterval(() => {
    _dashRefreshCountdown = Math.max(0, _dashRefreshCountdown - 1);
    _updateCountdownUI();
  }, 1000);
}

function _updateCountdownUI() {
  const el = document.getElementById('dashRefreshIn');
  if (el) el.textContent = `Refresh in ${_dashRefreshCountdown}s`;
}

// FIX #5: Show loading spinners on cold start before first fetch completes
async function loadDashboard() {
  // Show spinner on every refresh, not just initial load
  const _isFirstLoad = document.getElementById('statWatches').textContent === '—';
  if (_isFirstLoad) {
    ['statWatches','statBackups','statSuccess','statFailed'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.innerHTML = '<span class="spinner" style="width:18px;height:18px;border-width:2px"></span>';
    });
  }
  let d;
  try {
    const r = await fetch('/api/dashboard');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    d = await r.json();
    _dashErrorCount = 0;
    _startDashCountdown();
  } catch (err) {
    _dashErrorCount++;
    if (_dashErrorCount >= _DASH_MAX_ERRORS) {
      document.getElementById('dashWatches').innerHTML =
        `<div class="empty-state"><div style="color:var(--red);font-size:13px">⚠ Server unreachable (${err.message}). Auto-refresh paused.</div>
         <button class="btn btn-amber btn-sm" style="margin-top:10px" onclick="_dashErrorCount=0;loadDashboard()">🔄 Retry</button></div>`;
      clearInterval(_dashCountdownTimer);
      return;
    }
    document.getElementById('dashWatches').innerHTML =
      `<div class="empty-state"><div style="color:var(--red);font-size:13px">⚠ Failed to load dashboard: ${err.message}</div></div>`;
    return;
  }

  document.getElementById('statWatches').textContent = d.watches.length;
  document.getElementById('statBackups').textContent = d.total_backups;
  document.getElementById('statSuccess').textContent =
    d.success_count !== undefined
      ? d.success_count
      : d.recent_backups.filter(b => b.status === 'success').length;

  const failEl = document.getElementById('statFailed');
  if (failEl) {
    failEl.textContent = d.failed_count || 0;
    failEl.parentElement.style.setProperty('--accent', (d.failed_count > 0) ? 'var(--red)' : 'var(--dim)');
  }

  const cancelledEl = document.getElementById('statCancelled');
  if (cancelledEl) {
    cancelledEl.textContent = d.cancelled_count || 0;
    const cancelledCard = document.getElementById('cancelledStatCard');
    if (cancelledCard) cancelledCard.style.display = (d.cancelled_count > 0) ? 'block' : 'none';
  }

  document.getElementById('statDest').textContent =
    (d.settings.destination || './backups').split(/[/\\]/).pop() || 'backups';

  const storEl = document.getElementById('statStorSize');
  if (storEl && d.dest_size_human) storEl.textContent = d.dest_size_human;

  const warnEl = document.getElementById('destWarnBanner');
  if (warnEl) {
    warnEl.style.display = d.dest_writable === false ? 'flex' : 'none';
  }

  const diskLowEl = document.getElementById('diskLowBanner');
  if (diskLowEl) {
    diskLowEl.style.display = d.dest_low_disk ? 'flex' : 'none';
    if (d.dest_low_disk && d.dest_free_human) {
      const msg = document.getElementById('diskLowMsg');
      if (msg) msg.textContent = `only ${d.dest_free_human} free at backup destination`;
    }
  }

  // 🟡 NEW: Storage capacity warning
  const storageHighEl = document.getElementById('storageHighBanner');
  if (storageHighEl) {
    storageHighEl.style.display = (d.dest_backup_pct > 80) ? 'flex' : 'none';
    if (d.dest_backup_pct) {
      document.getElementById('storagePct').textContent = Math.round(d.dest_backup_pct);
    }
  }
  // END NEW

  const ab = document.getElementById('autoBadge');
  ab.className = 'auto-badge' + (d.settings.auto_backup ? ' on' : '');
  ab.textContent = d.settings.auto_backup ? '● AUTO ON' : '○ AUTO OFF';

  const nextRunEl = document.getElementById('daemonNextRun');
  if (nextRunEl) {
    if (d.daemon_next_run && d.settings.auto_backup) {
      const secsLeft = Math.max(0, Math.round(d.daemon_next_run - Date.now() / 1000));
      const m = Math.floor(secsLeft / 60), s = secsLeft % 60;
      nextRunEl.textContent = `Next auto: ${m}m ${s}s`;
      nextRunEl.style.display = 'inline';
    } else {
      nextRunEl.style.display = 'none';
    }
  }

  const runningCount = Object.values(d.backup_statuses || {}).filter(s => s.running).length;
  const runBadge = document.getElementById('runningBadge');
  if (runBadge) {
    runBadge.style.display = runningCount > 0 ? 'inline-flex' : 'none';
    runBadge.textContent = `${runningCount} running`;
  }

  const cancelAllBtn = document.getElementById('cancelAllBtn');
  if (cancelAllBtn) cancelAllBtn.style.display = runningCount > 0 ? 'inline-flex' : 'none';

  Object.entries(d.backup_statuses || {}).forEach(([wid, s]) => {
    if (s.running && !pollTimers[wid]) _reconnectBackupPoll(wid);
  });

  const dw = document.getElementById('dashWatches');
  if (!d.watches.length) {
    dw.innerHTML = '<div class="empty-state"><div style="font-size:28px;margin-bottom:8px">📁</div><div>No watch targets yet</div></div>';
  } else {
    const lastStatus  = d.watch_last_status  || {};
    const diskUsage   = d.watch_disk_usage   || {};
    dw.innerHTML = d.watches.map(w => {
      const hadError  = lastStatus[w.id] && lastStatus[w.id] !== 'success' && lastStatus[w.id] !== 'cancelled';
      const isRunning = (d.backup_statuses || {})[w.id]?.running;
      const pct       = (d.backup_statuses || {})[w.id]?.progress || 0;
      const diskStr   = diskUsage[w.id] || '';
      return `
      <div style="display:flex;align-items:center;gap:10px;padding:9px 0;border-bottom:1px solid var(--border)"
           oncontextmenu="showWatchContextMenu(event,'${w.id}','${w.name.replace(/'/g,"\\'")}',${!!w.paused})">
        <div style="font-size:18px">${w.type === 'cloud' ? '☁' : '📁'}</div>
        <div style="flex:1;min-width:0">
          ${(() => {
            const _wp = w.path || '';
            const _isDriveRoot = /^[A-Za-z]:[\\\/]?$/.test(_wp) || _wp === '/';
            return _isDriveRoot
              ? `<div style="background:#2a0d0d;border:1px solid var(--red);border-radius:6px;padding:5px 10px;margin-top:4px;font-family:var(--mono);font-size:10px;color:var(--red)">⚠ This watch points to a drive root (${escapeHtml(_wp)}). Backups are blocked. Go to Watches → ✎ META to set a specific subfolder instead.</div>` 
              : '';
          })()}
          <div style="color:var(--text-bright);font-size:13px;font-weight:500;display:flex;align-items:center;gap:6px;flex-wrap:wrap">
            ${escapeHtml(w.name)}
            ${w.paused ? '<span class="tag" style="color:var(--amber);border-color:#503010;background:#2a1800;font-size:8px">PAUSED</span>' : ''}
            ${hadError ? `<span class="tag" style="color:var(--red);border-color:#501a1a;background:#2a0d0d;font-size:8px" title="${escapeHtml(w.last_error || 'Unknown error')}">LAST FAILED ⓘ</span>` : ''}
            ${isRunning ? '<span class="tag" style="color:var(--green);border-color:#1a5030;background:#0d2a1a;font-size:8px">● RUNNING</span>' : ''}
            ${w.path_exists === false ? '<span class="tag" style="color:var(--red);border-color:#501a1a;background:#2a0d0d;font-size:8px">⚠ PATH MISSING</span>' : ''}
          </div>
          <div style="font-family:var(--mono);color:var(--dim);font-size:10px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">
            ${escapeHtml(w.path)}
            ${!w.path_exists ? '<span style="color:var(--red);margin-left:8px">⚠ PATH MISSING</span>' : '<span style="color:var(--green);margin-left:8px">✓</span>'}
          </div>
          <div style="font-family:var(--mono);font-size:10px;color:var(--dim);display:flex;gap:10px;margin-top:2px">
            ${(d.watch_backup_counts || {})[w.id] ? `<span>${d.watch_backup_counts[w.id]} backup${d.watch_backup_counts[w.id] !== 1 ? 's' : ''}</span>` : ''}
            ${(d.watch_last_ts || {})[w.id] ? `<span>Last: ${fmtTs(d.watch_last_ts[w.id])}</span>` : '<span style="color:var(--amber)">Never backed up</span>'}
            ${w.pending_count > 0 ? `<span style="color:var(--amber)">⚠ ${w.pending_count} pending</span>` : ''}
            ${diskStr ? `<span>💾 ${diskStr}</span>` : ''}
          </div>
        </div>
        <div style="min-width:110px">
          <div id="dash_progress_${w.id}" style="display:${isRunning ? 'block' : 'none'};margin-bottom:4px">
            <div style="background:var(--bg3);border-radius:4px;height:4px;overflow:hidden">
              <div id="dash_pbar_${w.id}" style="height:4px;background:var(--green);width:${pct}%;transition:width .3s"></div>
            </div>
            <div id="dash_ppct_${w.id}" style="font-family:var(--mono);font-size:9px;color:var(--dim);text-align:right;margin-top:2px">${pct}%</div>
          </div>
        </div>
        ${isRunning
          ? `<button class="btn btn-red btn-sm" onclick="cancelBackup('${w.id}')">✕ CANCEL</button>`
          : `<button class="btn btn-green btn-sm" onclick="runBackup('${w.id}')" id="btn_${w.id}" ${w.paused ? 'disabled' : ''} title="Run Backup">▶ BACKUP</button>
             <button class="btn btn-amber btn-sm" onclick="runDryRun('${w.id}')" ${w.paused ? 'disabled' : ''} title="Dry Run (Preview)">🔍 PREVIEW</button>`}
        <button class="btn btn-purple btn-sm" onclick="openEditorForWatch('${w.id}','${w.name}','${w.path.replace(/\\/g, '\\\\').replace(/'/g, "\\'")}')">✏️ EDIT</button>
      </div>`;
    }).join('');
  }

  const dr = document.getElementById('dashRecent');
  if (!d.recent_backups.length) {
    dr.innerHTML = '<div class="empty-state"><div style="font-size:28px;margin-bottom:8px">📋</div><div>No backups yet</div></div>';
    _renderActivityChart();
    return;
  }
  d.recent_backups.forEach(b => { _backupCache[b.backup_id] = b; });
  const _cacheKeys = Object.keys(_backupCache);
  if (_cacheKeys.length > 200) _cacheKeys.slice(0, _cacheKeys.length - 200).forEach(k => delete _backupCache[k]);
  dr.innerHTML = d.recent_backups.map(b => `
    <div class="history-row" onclick="showPage('history',null);setTimeout(()=>selectBackupById('${b.backup_id}'),300)">
      <div class="status-led ${b.status === 'success' ? 'led-green' : b.status === 'cancelled' ? 'led-amber' : 'led-red'}"></div>
      <div style="flex:1;min-width:0">
        <div style="color:var(--text-bright);font-size:12px;font-weight:500">${escapeHtml(b.watch_name || '?')}</div>
        <div style="font-family:var(--mono);color:var(--dim);font-size:10px">${fmtTs(b.timestamp)}</div>
        ${b.user_notes ? `<div style="font-size:10px;color:var(--amber);font-style:italic;margin-top:2px">📝 ${escapeHtml(b.user_notes)}</div>` : ''}
      </div>
      <div style="font-family:var(--mono);font-size:10px;color:var(--dim);display:flex;align-items:center;gap:8px">
        <span>${b.files_copied || 0} files · ${b.total_size || '—'}</span>
        ${b.status === 'success' ? `<button class="btn btn-ghost btn-sm" style="padding:2px 7px;font-size:9px" onclick="event.stopPropagation();exportBackup('${b.backup_id}','${(b.watch_name||'backup').replace(/'/g,'')}')">⬇</button>` : ''}
      </div>
    </div>`).join('');

  _renderActivityChart();
}


// ── Watch Context Menu ────────────────────────────────────────────────────────

function showWatchContextMenu(e, watchId, watchName, isPaused) {
  e.preventDefault();
  e.stopPropagation();
  _watchCtxTarget = { id: watchId, name: watchName, paused: isPaused };
  hideAllContextMenus();

  let menu = document.getElementById('watchContextMenu');
  if (!menu) {
    menu = document.createElement('div');
    menu.id = 'watchContextMenu';
    menu.className = 'context-menu';
    document.body.appendChild(menu);
  }
  menu.innerHTML = `
    <div class="ctx-item" onclick="watchCtxBackup()">▶ Backup Now</div>
    <div class="ctx-item" onclick="watchCtxFullBackup()">⬛ Full Backup (all files)</div>
    <div class="ctx-item" onclick="watchCtxPauseToggle()">${isPaused ? '▶ Resume' : '⏸ Pause'}</div>
    <div class="ctx-item" onclick="watchCtxStats()">📊 View Stats</div>
    <div class="ctx-item" onclick="watchCtxDuplicate()">⧉ Duplicate</div>
    <div class="ctx-separator"></div>
    <div class="ctx-item ctx-danger" onclick="watchCtxRemove()">🗑 Remove</div>`;
  menu.style.display = 'block';
  menu.style.left    = Math.min(e.clientX, window.innerWidth  - 180) + 'px';
  menu.style.top     = Math.min(e.clientY, window.innerHeight - 160) + 'px';
}

function watchCtxBackup()      { hideAllContextMenus(); if (_watchCtxTarget) runBackup(_watchCtxTarget.id); }
function watchCtxFullBackup()  {
  hideAllContextMenus();
  if (!_watchCtxTarget) return;
  if (confirm(`Run FULL backup of "${_watchCtxTarget.name}"?\n\nAll files will be copied regardless of changes.`))
    runBackup(_watchCtxTarget.id, false);
}
function watchCtxPauseToggle() { hideAllContextMenus(); if (_watchCtxTarget) togglePauseWatch(_watchCtxTarget.id, !_watchCtxTarget.paused); }
function watchCtxStats()       { hideAllContextMenus(); if (_watchCtxTarget) openWatchStats(_watchCtxTarget.id, _watchCtxTarget.name); }
function watchCtxRemove()      { hideAllContextMenus(); if (_watchCtxTarget) removeWatch(_watchCtxTarget.id); }
function watchCtxDuplicate() {
  hideAllContextMenus();
  if (!_watchCtxTarget) return;
  fetch('/api/watches').then(r => r.json()).then(watches => {
    const w = watches.find(x => x.id === _watchCtxTarget.id);
    if (w) openDuplicateWatchModal(w.id, w.name, w.path);
  });
}


// ── Watches ───────────────────────────────────────────────────────────────────

async function loadWatches() {
  // Clear stale pending entries from removed watches
  Object.keys(_allPending).forEach(k => delete _allPending[k]);
  const el = document.getElementById('watchList');
  el.innerHTML = '<div style="padding:20px;text-align:center;color:var(--dim);font-family:var(--mono);font-size:11px">Loading…</div>';
  let watches;
  try {
    const r = await fetch('/api/watches');
    watches = await r.json();
  } catch (err) {
    el.innerHTML = `<div class="empty-state"><div style="color:var(--red);font-size:13px">⚠ Failed to load watches: ${err.message}</div></div>`;
    return;
  }
  if (!watches.length) {
    el.innerHTML = '<div class="empty-state"><div style="font-size:36px;margin-bottom:12px">👁</div><div>No watch targets. Click ADD TARGET to begin.</div></div>';
    return;
  }

  el.innerHTML = watches.map(w => {
    const p        = w.pending_changes || [];
    _allPending[w.id] = p;
    const isPaused  = !!(w.paused);
    const isRunning = !!(pollTimers[w.id]);

    return `<div class="watch-item ${isRunning ? 'running' : ''} ${w.path_exists === false ? 'path-missing' : ''}" id="watchcard_${w.id}" oncontextmenu="showWatchContextMenu(event,'${w.id}','${w.name.replace(/'/g,"\\'")}',${isPaused})">
      <div class="watch-header">
        <div class="watch-icon ${w.type}">${w.type === 'cloud' ? '☁' : '📁'}</div>
        <div class="watch-meta">
          <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
            <div class="watch-name">${escapeHtml(w.name)}</div>
            ${isPaused ? '<span class="tag" style="color:var(--amber);border-color:#503010;background:#2a1800">PAUSED</span>' : '<span class="tag tag-green">● WATCHING</span>'}
            <span class="tag ${w.type === 'cloud' ? 'tag-blue' : 'tag-purple'}">${w.type.toUpperCase()}</span>
            ${w.path_exists === false ? '<span class="tag" style="color:var(--red);border-color:#501a1a;background:#2a0d0d;font-size:8px">⚠ PATH MISSING</span>' : ''}
            ${w.skip_auto_backup ? '<span class="tag" style="color:var(--dim);border-color:var(--border2);background:var(--bg3);font-size:8px" title="Excluded from scheduled auto-backup">⛔ NO AUTO</span>' : ''}
            ${w.max_backups > 0 ? `<span class="tag" style="color:var(--blue);border-color:#1a3050;background:#0d1a30;font-size:8px" title="Keeps only ${w.max_backups} most recent backups">MAX ${w.max_backups}</span>` : ''}
            ${(w.tags || []).map(t => `<span class="tag" style="color:var(--tag-color);border-color:var(--tag-border);background:var(--tag-bg);font-size:9px">#${t}</span>`).join('')}
            <span style="font-family:var(--mono);font-size:9px;color:var(--dim)" title="Right-click for quick actions">⋮</span>
          </div>
          <div class="watch-path" style="display:flex;align-items:center;gap:6px">
            <span style="flex:1;overflow:hidden;text-overflow:ellipsis">${escapeHtml(w.path)}</span>
            <button onclick="event.stopPropagation();navigator.clipboard.writeText('${w.path.replace(/'/g,"\\'")}');toast('Path copied!','info')"
              style="background:none;border:none;cursor:pointer;color:var(--dim);font-size:10px;flex-shrink:0;padding:0 4px"
              title="Copy full path">⧉</button>
          </div>
          ${w.notes ? `<div style="font-size:11px;color:var(--dim);margin-top:4px;font-style:italic">${escapeHtml(w.notes)}</div>` : ''}
          <div style="font-family:var(--mono);font-size:10px;color:var(--dim);margin-top:4px;display:flex;gap:12px;flex-wrap:wrap">
            ${(() => {
              if (!w.last_backup) return '<span style="color:var(--amber)">⚠ Never backed up</span>';
              const daysSince = Math.floor((Date.now() - new Date(w.last_backup)) / 86400000);
              const color = daysSince > 7 ? 'var(--red)' : daysSince > 3 ? 'var(--amber)' : 'var(--dim)';
              const warn  = daysSince > 7 ? ' ⚠' : '';
              return `<span style="color:${color}">Last: ${fmtTs(w.last_backup)}${warn}</span>`;
            })()}
            ${w.pending_count > 0 ? `<span style="color:var(--amber)">⚠ ${w.pending_count} pending</span>` : ''}
            ${w.disk_usage_human ? `<span style="color:var(--purple)">💾 ${w.disk_usage_human}</span>` : ''}
          </div>
          ${p.length && !isPaused ? `
          <div style="margin-top:12px;background:var(--bg0);border:1px solid #2a3a1a;border-radius:8px;overflow:hidden">
            <div style="display:flex;align-items:center;justify-content:space-between;padding:10px 14px;border-bottom:1px solid #1a2a0f">
              <div style="font-family:var(--mono);font-size:9px;letter-spacing:2px;color:var(--amber)">⚠ ${p.length} PENDING CHANGE${p.length > 1 ? 'S' : ''}</div>
              <button class="btn btn-amber btn-sm" onclick="viewChanges('${w.id}',${JSON.stringify(JSON.stringify(p))})">👁 VIEW CHANGES</button>
            </div>
            <div style="padding:8px 14px 4px">
              <input type="text" placeholder="🔍 Search files..." oninput="filterPending('${w.id}',this.value)"
                style="width:100%;padding:7px 12px;background:var(--bg1);border:1px solid var(--border2);border-radius:6px;color:var(--text);font-family:var(--mono);font-size:11px;outline:none;margin-bottom:8px"/>
              <div id="pendinglist_${w.id}">
                ${p.slice(0, 8).map(c => `<div style="display:flex;align-items:center;gap:8px;padding:4px 0;border-bottom:1px solid #1a2a0f11">
                  ${changeBadge(c.type)}
                  <span style="font-family:var(--mono);font-size:11px;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;cursor:pointer;color:var(--blue)"
                    onclick="viewFileDiff('${w.id}','${c.path.replace(/\\/g, '\\\\').replace(/'/g, "\\'")}','${c.type}')"
                    title="${c.path}">${c.path}</span>
                </div>`).join('')}
                ${p.length > 8 ? `<div style="font-family:var(--mono);font-size:10px;color:var(--dim);padding:6px 0">...and ${p.length - 8} more — use search above to filter</div>` : ''}
              </div>
            </div>
          </div>` : ''}
          <div id="diffpanel_${w.id}" style="display:none;margin-top:10px"></div>
          <div id="watch_progress_${w.id}" style="display:${isRunning ? 'block' : 'none'};margin-top:10px">
            <div style="background:var(--bg3);border-radius:4px;height:5px;overflow:hidden">
              <div id="watch_pbar_${w.id}" style="height:5px;background:var(--green);width:0%;transition:width .3s"></div>
            </div>
            <div id="watch_ppct_${w.id}" style="font-family:var(--mono);font-size:9px;color:var(--dim);margin-top:3px">Backing up… 0%</div>
          </div>
        </div>

        <div class="watch-actions">
          ${isRunning
            ? `<button class="btn btn-red btn-sm" onclick="cancelBackup('${w.id}')">✕ CANCEL</button>`
            : `<button class="btn btn-green btn-sm" onclick="runBackup('${w.id}')" id="btn_${w.id}" ${isPaused ? 'disabled' : ''} title="Run Backup">▶ BACKUP</button>
               <button class="btn btn-amber btn-sm" onclick="runDryRun('${w.id}')" ${isPaused ? 'disabled' : ''} title="Dry Run (Preview)">🔍 PREVIEW</button>`}
          <button class="btn btn-purple btn-sm" onclick="openEditorForWatch('${w.id}','${w.name}','${w.path.replace(/\\/g, '\\\\').replace(/'/g, "\\'")}')">✏️ EDIT</button>
          <button class="btn btn-blue btn-sm" onclick="scanWatch('${w.id}')" ${isPaused ? 'disabled' : ''}>🔎 SCAN</button>
          <button class="btn btn-blue btn-sm" onclick="openWatchStats('${w.id}','${w.name}')">📊 STATS</button>
          <button class="btn ${isPaused ? 'btn-green' : 'btn-amber'} btn-sm" onclick="togglePauseWatch('${w.id}',${!isPaused})">${isPaused ? '▶ RESUME' : '⏸ PAUSE'}</button>
          <button class="btn btn-ghost btn-sm" onclick="openEditWatchModal('${w.id}','${w.name}','${(w.tags || []).join(',')}','${(w.notes || '').replace(/'/g, "\\'")}','${w.path.replace(/\\/g, '\\\\').replace(/'/g, "\\'")}')">✎ META</button>
          <button class="btn btn-amber btn-sm" onclick="openDuplicateWatchModal('${w.id}','${w.name}','${w.path.replace(/\\/g, '\\\\').replace(/'/g, "\\'")}')">⧉ DUPLICATE</button>
          <button class="btn btn-blue btn-sm" onclick="restoreLatestBackup('${w.id}','${w.name.replace(/'/g, "\\'")}',this)" ${isPaused ? 'disabled' : ''} title="Restore from latest successful backup">♻ LAST</button>
          <button class="btn btn-red btn-sm" onclick="removeWatch('${w.id}')">✕ REMOVE</button>
<button class="btn btn-ghost btn-sm" onclick="deleteWatchBackups('${w.id}','${w.name.replace(/'/g,"\\'")}')" title="Delete all backups for this watch (keeps the watch)">🗑 PURGE</button>
        </div>
      </div>
    </div>`;
  }).join('');

  // Reconnect progress polling for any backups that were already running
  // (handles navigating away and back mid-backup)
  try {
    const sr = await fetch('/api/backup/all/status');
    const allStatus = await sr.json();
    Object.entries(allStatus.statuses || {}).forEach(([wid, s]) => {
      if (s.running && !pollTimers[wid]) _reconnectBackupPoll(wid);
    });
  } catch (_) {}
}

async function deleteWatchBackups(watchId, watchName) {
  const r = await fetch(`/api/history?watch_id=${encodeURIComponent(watchId)}&per_page=200`);
  const d = await r.json();
  const backups = d.backups || [];
  if (!backups.length) { toast(`No backups found for "${watchName}"`, 'info'); return; }
  if (!confirm(`Delete ALL ${backups.length} backup(s) for "${watchName}"?\n\nThe watch itself is kept. This cannot be undone.`)) return;
  let deleted = 0;
  for (const b of backups) {
    const dr = await fetch(`/api/history/${b.backup_id}`, { method: 'DELETE' });
    const dd = await dr.json();
    if (dd.ok) deleted++;
  }
  toast(`Deleted ${deleted} backup(s) for "${watchName}"`, 'info');
  loadWatches(); loadDashboard();
}

async function runDryRun(watchId) {
  toast('Calculating backup preview…', 'info');
  try {
    const res  = await fetch(`/api/watches/${watchId}/dry-run`);
    const data = await res.json();
    if (data.error) { toast(`Error: ${data.error}`, 'error'); return; }

    let modal = document.getElementById('dryRunModal');
    if (!modal) {
      modal = document.createElement('div');
      modal.id = 'dryRunModal';
      modal.className = 'modal-overlay';
      modal.innerHTML = `<div class="modal" style="max-width:420px">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:18px">
          <div class="modal-title" style="margin:0">🔍 BACKUP PREVIEW</div>
          <button onclick="document.getElementById('dryRunModal').classList.remove('open')"
            style="background:none;border:none;cursor:pointer;color:var(--dim);font-size:18px">✕</button>
        </div>
        <div id="dryRunContent"></div>
        <div class="modal-actions"><button class="btn btn-ghost" onclick="document.getElementById('dryRunModal').classList.remove('open')">CLOSE</button></div>
      </div>`;
      document.body.appendChild(modal);
    }

    document.getElementById('dryRunContent').innerHTML = `
      <div class="meta-row"><span class="meta-key">TARGET</span><span class="meta-value">${escapeHtml(data.watch_name)}</span></div>
      <div class="meta-row"><span class="meta-key">FILES TO COPY</span><span class="meta-value" style="color:var(--green)">${data.files_to_copy}</span></div>
      <div class="meta-row"><span class="meta-key">ESTIMATED SIZE</span><span class="meta-value" style="color:var(--amber)">${data.total_size_human}</span></div>
      <div class="meta-row"><span class="meta-key">OLD BACKUPS TO PRUNE</span><span class="meta-value" style="color:var(--red)">${data.files_to_delete}</span></div>
      <div class="meta-row"><span class="meta-key">TOTAL FILES TRACKED</span><span class="meta-value">${data.total_files}</span></div>
      ${data.files_to_copy === 0 ? '<div style="margin-top:12px;font-family:var(--mono);font-size:11px;color:var(--dim);text-align:center">✓ Nothing to back up — all files are up to date</div>' : ''}
      ${data.files_to_copy > 0 ? `<div style="margin-top:14px;text-align:center"><button class="btn btn-green btn-sm" onclick="closeDryRunModal();runBackup('${watchId}')">▶ RUN BACKUP NOW</button></div>` : ''}`;

    modal.classList.add('open');
  } catch (e) {
    toast('Failed to fetch preview', 'error');
  }
}

function closeDryRunModal() {
  const modal = document.getElementById('dryRunModal');
  if (modal) modal.classList.remove('open');
}

function clearAllHistoryFilters() {
  _historyFilterQ = _historyFilterStatus = _historyFilterWatch = _historyFilterFrom = _historyFilterTo = '';
  const ids = ['historySearch','historyFromDate','historyToDate'];
  ids.forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });
  const sf = document.getElementById('historyStatusFilter'); if (sf) sf.value = '';
  const wf = document.getElementById('historyWatchFilter');  if (wf) wf.value = '';
  loadHistory(1);
}

function sortWatchList(by) {
  const list = document.getElementById('watchList');
  if (!list) return;
  const items = [...list.querySelectorAll('.watch-item')];
  items.sort((a, b) => {
    const getText = el => el.querySelector('.watch-name')?.textContent.trim() || '';
    const getTime = el => el.querySelector('[style*="Last:"]')?.textContent || '';
    const getPending = el => parseInt(el.querySelector('[style*="pending"]')?.textContent || '0') || 0;
    if (by === 'name')         return getText(a).localeCompare(getText(b));
    if (by === 'last_backup')  return getTime(b).localeCompare(getTime(a));
    if (by === 'pending')      return getPending(b) - getPending(a);
    return 0;
  });
  items.forEach(el => list.appendChild(el));
}

function filterWatchList(query) {
  const q = query.trim().toLowerCase();
  document.querySelectorAll('#watchList .watch-item').forEach(item => {
    item.style.display = (!q || item.textContent.toLowerCase().includes(q)) ? '' : 'none';
  });
}

function filterPending(watchId, query) {
  const list = document.getElementById('pendinglist_' + watchId);
  if (!list) return;
  const all = _allPending[watchId] || [];
  const q   = query.trim().toLowerCase();
  const filtered = q ? all.filter(c => c.path.toLowerCase().includes(q)) : all;
  if (!filtered.length) {
    list.innerHTML = `<div style="font-family:var(--mono);font-size:11px;color:var(--dim);padding:8px 0">No files matching "${query}"</div>`;
    return;
  }
  const show = filtered.slice(0, 20);
  list.innerHTML = show.map(c => `
    <div style="display:flex;align-items:center;gap:8px;padding:4px 0;border-bottom:1px solid #1a2a0f11">
      ${changeBadge(c.type)}
      <span style="font-family:var(--mono);font-size:11px;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;cursor:pointer;color:var(--blue)"
        onclick="viewFileDiff('${watchId}','${c.path.replace(/\\/g, '\\\\').replace(/'/g, "\\'")}','${c.type}')"
        title="${c.path}">${c.path}</span>
    </div>`).join('')
    + (filtered.length > 20 ? `<div style="font-family:var(--mono);font-size:10px;color:var(--dim);padding:6px 0">${filtered.length - 20} more match — keep typing to narrow</div>` : '');
}

async function viewChanges(watchId, pendingJson) {
  const pending = JSON.parse(pendingJson);
  const panel   = document.getElementById('diffpanel_' + watchId);
  if (panel.style.display !== 'none') { panel.style.display = 'none'; return; }
  panel.style.display = 'block';
  panel.innerHTML = `<div style="background:var(--bg1);border:1px solid var(--border);border-radius:10px;overflow:hidden">
    <div style="display:flex;align-items:center;justify-content:space-between;padding:12px 16px;border-bottom:1px solid var(--border);background:var(--bg2)">
      <div style="font-family:var(--mono);font-size:11px;letter-spacing:2px;color:var(--amber)">📊 CHANGES COMPARISON</div>
      <button onclick="document.getElementById('diffpanel_${watchId}').style.display='none'" style="background:none;border:none;cursor:pointer;color:var(--dim);font-size:16px">✕</button>
    </div>
    <div style="padding:14px;font-family:var(--mono);font-size:11px;color:var(--dim)">
      ${pending.map(c => `
      <div style="margin-bottom:10px;background:var(--bg0);border:1px solid var(--border);border-radius:7px;overflow:hidden">
        <div style="display:flex;align-items:center;gap:10px;padding:9px 14px;border-bottom:1px solid var(--border);background:var(--bg2)">
          ${changeBadge(c.type)}
          <span style="color:var(--text-bright);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${c.path}</span>
          <button class="btn btn-blue btn-sm" onclick="viewFileDiff('${watchId}','${c.path.replace(/\\/g, '\\\\').replace(/'/g, "\\'")}','${c.type}','inpanel_${watchId}_${btoa(c.path).replace(/[^a-zA-Z0-9]/g, '')}')">
            🔍 LINE DIFF
          </button>
        </div>
        <div id="inpanel_${watchId}_${btoa(c.path).replace(/[^a-zA-Z0-9]/g, '')}" style="display:none"></div>
      </div>`).join('')}
    </div>
  </div>`;
}

async function viewFileDiff(watchId, filePath, changeType, targetId) {
  const id       = targetId || ('quickdiff_' + watchId);
  const existing = document.getElementById(id);
  if (existing && existing.style.display !== 'none') { existing.style.display = 'none'; return; }
  toast('Loading diff...', 'info');
  const r = await fetch(`/api/watches/${watchId}/filediff`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ file_path: filePath }),
  });
  const d = await r.json();
  if (d.error) { toast(d.error, 'error'); return; }

  const CONTEXT    = 4;
  const diffLines  = d.diff;
  const changed    = new Set();
  diffLines.forEach((l, i) => {
    if (l.type !== 'equal') {
      for (let j = Math.max(0, i - CONTEXT); j <= Math.min(diffLines.length - 1, i + CONTEXT); j++) changed.add(j);
    }
  });

  let html = `<div style="padding:8px 14px;font-family:var(--mono);font-size:11px;display:flex;gap:14px;border-bottom:1px solid var(--border);background:var(--bg1)">
    <span style="color:var(--green)">+${d.added} added</span>
    <span style="color:var(--red)">-${d.removed} removed</span>
    ${!d.has_backup ? '<span style="color:var(--amber)">⚠ No previous backup — showing full file as new</span>' : ''}
  </div><div style="overflow-x:auto;max-height:340px;overflow-y:auto">`;

  let skipping = false;
  diffLines.forEach((line, i) => {
    if (!changed.has(i)) {
      if (!skipping) { html += '<div style="padding:1px 10px;font-family:var(--mono);font-size:11px;color:var(--dim);background:var(--bg0)">  ···</div>'; skipping = true; }
      return;
    }
    skipping = false;
    const type   = line.type;
    const prefix = type === 'added' ? '+' : type === 'removed' ? '-' : ' ';
    const bg     = type === 'added' ? '#0a2018' : type === 'removed' ? '#200a0a' : 'transparent';
    const col    = type === 'added' ? 'var(--green)' : type === 'removed' ? 'var(--red)' : 'var(--dim)';
    const textCol = type === 'added' ? 'var(--green)' : type === 'removed' ? '#ff8888' : 'var(--text)';
    const oln = line.old_ln ? `<span style="width:36px;text-align:right;padding-right:10px;color:var(--dim);font-size:10px;flex-shrink:0;user-select:none;display:inline-block">${line.old_ln}</span>` : '<span style="width:36px;display:inline-block;flex-shrink:0"></span>';
    const nln = line.new_ln ? `<span style="width:36px;text-align:right;padding-right:10px;color:var(--dim);font-size:10px;flex-shrink:0;user-select:none;display:inline-block">${line.new_ln}</span>` : '<span style="width:36px;display:inline-block;flex-shrink:0"></span>';
    const text = (line.text || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    html += `<div style="display:flex;align-items:flex-start;padding:1px 0;background:${bg}">
      ${oln}${nln}<span style="width:14px;color:${col};font-weight:700;flex-shrink:0;font-family:var(--mono);font-size:12px">${prefix}</span>
      <span style="flex:1;font-family:var(--mono);font-size:12px;color:${textCol};padding:0 10px;white-space:pre;overflow-x:hidden;text-overflow:ellipsis">${text}</span>
    </div>`;
  });
  html += '</div>';

  if (existing) { existing.innerHTML = html; existing.style.display = 'block'; }
  else {
    const panel = document.getElementById('diffpanel_' + watchId);
    if (panel) {
      panel.style.display = 'block';
      panel.innerHTML = `<div style="background:var(--bg1);border:1px solid var(--border);border-radius:10px;overflow:hidden">
        <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 14px;border-bottom:1px solid var(--border);background:var(--bg2)">
          <div style="font-family:var(--mono);font-size:11px;color:var(--amber)">📊 ${d.file}</div>
          <button onclick="document.getElementById('diffpanel_${watchId}').style.display='none'" style="background:none;border:none;cursor:pointer;color:var(--dim);font-size:16px">✕</button>
        </div>
        <div id="${id}">${html}</div>
      </div>`;
    }
  }
  toast(`Diff loaded for ${d.file}`, 'info');
}


// ── Watch Stats Modal ─────────────────────────────────────────────────────────

async function openWatchStats(watchId, watchName) {
  const modal   = document.getElementById('watchStatsModal');
  const content = document.getElementById('watchStatsContent');
  content.innerHTML = '<div style="text-align:center;padding:20px;font-family:var(--mono);color:var(--dim)">Loading…</div>';
  document.getElementById('watchStatsTitle').textContent = watchName + ' — Stats';
  modal.classList.add('open');

  const r = await fetch(`/api/watches/${watchId}/stats`);
  const s = await r.json();

  const successRate = s.total_backups > 0 ? Math.round(s.success_count / s.total_backups * 100) : 0;
  content.innerHTML = `
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:16px">
      <div style="background:var(--bg0);border:1px solid var(--border);border-radius:8px;padding:14px;text-align:center">
        <div style="font-family:var(--mono);font-size:28px;color:var(--blue);font-weight:700">${s.total_backups}</div>
        <div style="font-size:10px;color:var(--dim);letter-spacing:2px;margin-top:4px">TOTAL BACKUPS</div>
      </div>
      <div style="background:var(--bg0);border:1px solid var(--border);border-radius:8px;padding:14px;text-align:center">
        <div style="font-family:var(--mono);font-size:28px;color:${successRate >= 80 ? 'var(--green)' : successRate >= 50 ? 'var(--amber)' : 'var(--red)'};font-weight:700">${successRate}%</div>
        <div style="font-size:10px;color:var(--dim);letter-spacing:2px;margin-top:4px">SUCCESS RATE</div>
      </div>
      <div style="background:var(--bg0);border:1px solid var(--border);border-radius:8px;padding:14px;text-align:center">
        <div style="font-family:var(--mono);font-size:28px;color:var(--green);font-weight:700">${s.success_count}</div>
        <div style="font-size:10px;color:var(--dim);letter-spacing:2px;margin-top:4px">SUCCESSFUL</div>
      </div>
      <div style="background:var(--bg0);border:1px solid var(--border);border-radius:8px;padding:14px;text-align:center">
        <div style="font-family:var(--mono);font-size:28px;color:${s.fail_count > 0 ? 'var(--red)' : 'var(--dim)'};font-weight:700">${s.fail_count}</div>
        <div style="font-size:10px;color:var(--dim);letter-spacing:2px;margin-top:4px">FAILED</div>
      </div>
      ${(s.cancelled_count || 0) > 0 ? `
      <div style="background:var(--bg0);border:1px solid var(--border);border-radius:8px;padding:14px;text-align:center">
        <div style="font-family:var(--mono);font-size:28px;color:var(--amber);font-weight:700">${s.cancelled_count}</div>
        <div style="font-size:10px;color:var(--dim);letter-spacing:2px;margin-top:4px">CANCELLED</div>
      </div>` : ''}
    </div>
    <div class="meta-row"><span class="meta-key">FILES COPIED (TOTAL)</span><span class="meta-value">${s.total_files_copied.toLocaleString()}</span></div>
    <div class="meta-row"><span class="meta-key">DISK USAGE (ALL BACKUPS)</span><span class="meta-value" style="color:var(--amber)">${s.disk_usage_human || '—'}</span></div>
    <div class="meta-row"><span class="meta-key">LAST BACKUP</span><span class="meta-value">${fmtTs(s.last_backup)}</span></div>
    ${s.path ? `<div class="meta-row"><span class="meta-key">PATH</span><span class="meta-value" style="font-family:var(--mono);font-size:10px;word-break:break-all">${s.path}</span></div>` : ''}
    ${s.paused ? `<div style="margin-top:8px"><span class="tag" style="color:var(--amber);border-color:#503010;background:#2a1800">⏸ CURRENTLY PAUSED</span></div>` : ''}
    <div style="margin-top:14px">
      <div style="font-family:var(--mono);font-size:10px;color:var(--dim);margin-bottom:6px">SUCCESS RATE</div>
      <div style="background:var(--bg3);border-radius:4px;height:8px;overflow:hidden">
        <div style="height:8px;background:${successRate >= 80 ? 'var(--green)' : successRate >= 50 ? 'var(--amber)' : 'var(--red)'};width:${successRate}%;transition:width .5s"></div>
      </div>
    </div>`;
}

function closeWatchStats() {
  document.getElementById('watchStatsModal').classList.remove('open');
}


// ── Duplicate Watch Modal ─────────────────────────────────────────────────────

function openDuplicateWatchModal(watchId, watchName, watchPath) {
  document.getElementById('dupWatchId').value   = watchId;
  document.getElementById('dupWatchName').value = watchName + ' (copy)';
  document.getElementById('dupWatchPath').value = watchPath;
  document.getElementById('duplicateWatchModal').classList.add('open');
  document.getElementById('dupWatchName').focus();
  document.getElementById('dupWatchName').select();
}
function closeDuplicateWatchModal() { document.getElementById('duplicateWatchModal').classList.remove('open'); }

async function saveDuplicateWatch() {
  const watchId = document.getElementById('dupWatchId').value;
  const name    = document.getElementById('dupWatchName').value.trim();
  const path    = document.getElementById('dupWatchPath').value.trim();
  if (!name || !path) { toast('Name and path required', 'error'); return; }
  const btn = document.querySelector('#duplicateWatchModal .btn-green');
  if (btn) { btn.disabled = true; btn.innerHTML = '<span class="spinner"></span>'; }
  const r = await fetch(`/api/watches/${watchId}/duplicate`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, path }),
  });
  const d = await r.json();
  if (btn) { btn.disabled = false; btn.innerHTML = 'DUPLICATE'; }
  if (d.error) { toast(d.error, 'error'); return; }
  closeDuplicateWatchModal();
  toast(`"${name}" duplicated!`);
  loadWatches(); loadDashboard();
}


// ── Backup ────────────────────────────────────────────────────────────────────

async function cancelBackup(watchId) {
  const r = await fetch(`/api/backup/${watchId}/cancel`, { method: 'POST' });
  const d = await r.json();
  if (d.error) { toast(d.error, 'warn'); return; }
  toast('Cancel requested — backup will stop after current file', 'warn');
}

async function cancelAllBackups() {
  const r = await fetch('/api/backup/all/cancel', { method: 'POST' });
  const d = await r.json();
  if (d.count === 0) { toast('No backups running', 'info'); return; }
  toast(`Cancel requested for ${d.count} backup${d.count !== 1 ? 's' : ''}`, 'warn');
}

async function runBackup(watchId, incremental = true) {

  const btn = document.getElementById('btn_' + watchId);

  if (btn) {
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span>';
  }

  _runningBackups.add(watchId);

  let r;
  try {
    r = await fetch(`/api/backup/${watchId}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ incremental }),
    });
  } catch (err) {
    if (btn) { btn.disabled = false; btn.innerHTML = '▶ BACKUP'; }
    _runningBackups.delete(watchId);
    toast('Server unreachable: ' + err.message, 'error');
    return;
  }

  if (!r.ok) {
    const d = await r.json();

    if (btn) {
      btn.disabled = false;
      btn.innerHTML = btn.textContent.includes('BACKUP') ? '▶ BACKUP' : '▶';
    }

    _runningBackups.delete(watchId);

    const isLowDisk = r.status === 507;

    toast(
      d.error || 'Failed to start backup',
      isLowDisk ? 'warn' : (r.status === 429 ? 'warn' : 'error')
    );

    return;
  }

  _showProgressBar(watchId, true);

  let _pollErrors = 0;
  pollTimers[watchId] = setInterval(async () => {
    try {
      const pr = await fetch(`/api/backup/${watchId}/status`);
      _pollErrors = 0;
      const s  = await pr.json();

      const pct = s.progress || 0;

      if (!_backupStartTimes[watchId])
        _backupStartTimes[watchId] = Date.now();

      let eta = '';

      if (pct > 3 && pct < 98) {
        const elapsed = (Date.now() - _backupStartTimes[watchId]) / 1000;
        const rem     = Math.max(1, Math.round(elapsed / (pct / 100) - elapsed));

        eta = rem >= 60
          ? ` · ~${Math.floor(rem / 60)}m ${rem % 60}s left`
          : ` · ~${rem}s left`;
      }

      _updateProgressBar(watchId, pct, s.current_file || '', eta);

      if (currentPage === 'history')
        _updateHistoryProgress(watchId, pct, s.running);

      if (!s.running && s.last_result) {

        clearInterval(pollTimers[watchId]);
        delete pollTimers[watchId];
        delete _backupStartTimes[watchId];

        _showProgressBar(watchId, false);
        document.title = 'BackupSys';
        _runningBackups.delete(watchId);

        const wasCancelled = s.cancel_requested || s.last_result.status === 'cancelled';
        const ok = s.last_result.status === 'success' && !wasCancelled;

        if (btn) {
          btn.disabled = false;
          btn.innerHTML =
            document.getElementById('watchList') &&
            document.getElementById('watchList').contains(btn)
              ? '▶ BACKUP'
              : '▶';
        }

        _playBeep(ok ? 'success' : 'error');

        if (wasCancelled) {

          toast('Backup cancelled', 'warn');

          _desktopNotify(
            '⚠ Backup Cancelled',
            `Backup stopped after current file`
          );

        } else {

          toast(
            ok
              ? `Backup complete — ${s.last_result.files_copied} files copied`
              : `Failed: ${s.last_result.error}`,
            ok ? 'success' : 'error'
          );

          _desktopNotify(
            ok ? '✓ Backup Complete' : '✗ Backup Failed',
            ok
              ? `${s.last_result.files_copied} files copied`
              : (s.last_result.error || 'Unknown error')
          );
        }

        if (currentPage === 'dashboard')
          loadDashboard();

        if (currentPage === 'watches')
          loadWatches();

        if (currentPage === 'history') {
          _updateHistoryProgress(watchId, 100, false);
          loadHistory();
        }
      }

    } catch (err) {
      _pollErrors++;
      console.warn('Status poll error:', err);
      if (_pollErrors >= 8) {
        clearInterval(pollTimers[watchId]);
        delete pollTimers[watchId];
        _showProgressBar(watchId, false);
        _runningBackups.delete(watchId);
        toast('Lost connection to server — backup status unknown', 'warn');
      }
    }

  }, 1000);
}

function _reconnectBackupPoll(watchId) {
  if (pollTimers[watchId]) return; // already polling
  _showProgressBar(watchId, true);
  _runningBackups.add(watchId);

  pollTimers[watchId] = setInterval(async () => {
    try {
      const pr = await fetch(`/api/backup/${watchId}/status`);
      const s  = await pr.json();
      const pct = s.progress || 0;
      if (!_backupStartTimes[watchId]) _backupStartTimes[watchId] = Date.now();
      let eta = '';
      if (pct > 3 && pct < 98) {
        const elapsed = (Date.now() - _backupStartTimes[watchId]) / 1000;
        const rem = Math.max(1, Math.round(elapsed / (pct / 100) - elapsed));
        eta = rem >= 60 ? ` · ~${Math.floor(rem/60)}m ${rem%60}s left` : ` · ~${rem}s left`;
      }
      _updateProgressBar(watchId, pct, s.current_file || '', eta);
      if (currentPage === 'history') _updateHistoryProgress(watchId, s.progress || 0, s.running);

      if (!s.running && s.last_result) {
        clearInterval(pollTimers[watchId]);
        delete pollTimers[watchId];
        _showProgressBar(watchId, false);
        _runningBackups.delete(watchId);

        const wasCancelled = s.last_result.status === 'cancelled';
        const ok = s.last_result.status === 'success' && !wasCancelled;
        _playBeep(ok ? 'success' : 'error');

        if (wasCancelled)   toast('Backup cancelled', 'warn');
        else toast(ok
          ? `Backup complete — ${s.last_result.files_copied} files copied`
          : `Failed: ${s.last_result.error}`,
          ok ? 'success' : 'error');

        if (currentPage === 'dashboard') loadDashboard();
        if (currentPage === 'watches')   loadWatches();
        if (currentPage === 'history')   loadHistory();
      }
    } catch (err) { console.warn('Reconnected poll error:', err); }
  }, 1500);
}

function _showProgressBar(watchId, show) {
  const wp = document.getElementById('watch_progress_' + watchId);
  if (wp) wp.style.display = show ? 'block' : 'none';
  const dp = document.getElementById('dash_progress_' + watchId);
  if (dp) dp.style.display = show ? 'block' : 'none';
}

function _updateProgressBar(watchId, pct, currentFile, eta = '') {
  const anyRunning = Object.values(pollTimers).length > 0;
  document.title = anyRunning ? `⏳ ${pct}% — BackupSys` : 'BackupSys';

  ['watch_pbar_', 'dash_pbar_'].forEach(prefix => {
    const bar = document.getElementById(prefix + watchId);
    if (bar) bar.style.width = pct + '%';
  });
  ['watch_ppct_', 'dash_ppct_'].forEach(prefix => {
    const lbl = document.getElementById(prefix + watchId);
    if (!lbl) return;
    if (prefix.startsWith('watch')) {
      const fname = currentFile ? currentFile.replace(/.*[/\\]/, '') : '';
      lbl.textContent = fname ? `${pct}%${eta} — ${fname}` : `Backing up… ${pct}%${eta}`;
    } else {
      lbl.textContent = `${pct}%`;
    }
  });
}

function _updateHistoryProgress(watchId, pct, running) {
  const bar  = document.getElementById(`hist_pbar_${watchId}`);
  const lbl  = document.getElementById(`hist_ppct_${watchId}`);
  const wrap = document.getElementById(`hist_progress_${watchId}`);
  if (wrap) wrap.style.display = running ? 'block' : 'none';
  if (bar)  bar.style.width    = pct + '%';
  if (lbl)  lbl.textContent    = running ? `${pct}%` : '';
}

async function backupAll() {
  if (!confirm('Start backup for all active watches?\n\nThis will run them all simultaneously.')) return;

  ['backupAllBtn', 'backupAllBtnWatches'].forEach(id => {
    const el = document.getElementById(id);
    if (el) { el.disabled = true; el.innerHTML = '<span class="spinner"></span> BACKING UP...'; }
  });

  let d;
  try {
    const r = await fetch('/api/backup/all', { method: 'POST' });
    d = await r.json();
  } catch (err) {
    toast('Backup all failed: ' + err.message, 'error');
    _resetBackupAllBtns();
    return;
  }

  if (!d.ok) { toast(d.error || 'Backup all failed', 'error'); _resetBackupAllBtns(); return; }
  const count = d.started.length;
  if (!count) { toast('No active watches to back up', 'info'); _resetBackupAllBtns(); return; }
  toast(`Started ${count} backup${count > 1 ? 's' : ''}...`, 'info');
  d.started.forEach(id => _showProgressBar(id, true));

  const poll = setInterval(async () => {
    try {
      const sr = await fetch('/api/backup/all/status');
      const allStatus = await sr.json();
      const statuses  = d.started.map(id => allStatus.statuses[id] || {});

      statuses.forEach((s, i) => {
        if (s.running) _updateProgressBar(d.started[i], s.progress || 0);
      });

      if (!statuses.some(s => s.running)) {
        clearInterval(poll);
        d.started.forEach(id => _showProgressBar(id, false));
        _resetBackupAllBtns();
        const ok = statuses.filter(s => s.last_result && s.last_result.status === 'success').length;
        _playBeep(ok === count ? 'success' : 'error');
        toast(`${ok}/${count} backups successful`, ok === count ? 'success' : 'warn');
        if (currentPage === 'dashboard') loadDashboard();
        if (currentPage === 'watches')   loadWatches();
        if (currentPage === 'history')   loadHistory();
      }
    } catch (err) {
      clearInterval(poll);
      d.started.forEach(id => _showProgressBar(id, false));
      _resetBackupAllBtns();
      toast('Poll error: ' + err.message, 'error');
    }
  }, 1500);
}

function _resetBackupAllBtns() {
  ['backupAllBtn', 'backupAllBtnWatches'].forEach(id => {
    const el = document.getElementById(id);
    if (el) { el.disabled = false; el.innerHTML = '▶ BACKUP ALL'; }
  });
}

async function pauseAllWatches() {
  const r = await fetch('/api/watches');
  const watches = await r.json();
  const active = watches.filter(w => !w.paused);
  const paused = watches.filter(w => w.paused);
  const allPaused = active.length === 0;
  if (allPaused) {
    if (!confirm(`Resume all ${paused.length} paused watches?`)) return;
    for (const w of paused) await fetch(`/api/watches/${w.id}/pause`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({paused:false}) });
    toast(`Resumed ${paused.length} watches`, 'success');
  } else {
    if (!confirm(`Pause all ${active.length} active watches?`)) return;
    for (const w of active) await fetch(`/api/watches/${w.id}/pause`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({paused:true}) });
    toast(`Paused ${active.length} watches`, 'warn');
  }
  loadWatches(); loadDashboard();
}

async function togglePauseWatch(watchId, pause) {
  const r = await fetch(`/api/watches/${watchId}/pause`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ paused: pause }),
  });
  const d = await r.json();
  if (d.error) { toast(d.error, 'error'); return; }
  toast(pause ? 'Watch paused' : 'Watch resumed', pause ? 'info' : 'success');
  loadWatches(); loadDashboard();
}

async function backupCurrentWatch() { if (editorCurrentWatchId) await runBackup(editorCurrentWatchId); }

async function scanWatch(id) {
  const btn = document.querySelector(`[onclick="scanWatch('${id}')"]`);
  if (btn) { btn.disabled = true; btn.innerHTML = '<span class="spinner"></span>'; }
  toast('Scanning...', 'info');
  const r = await fetch(`/api/watches/${id}/scan`, { method: 'POST' });
  const d = await r.json();
  if (btn) { btn.disabled = false; btn.innerHTML = '🔎 SCAN'; }
  toast(`Scan done — ${d.total} change${d.total !== 1 ? 's' : ''} found`);
  loadWatches();
}


// ── Add Watch Modal ───────────────────────────────────────────────────────────

function openAddModal()  { document.getElementById('addModal').classList.add('open'); document.getElementById('newName').focus(); }
function closeAddModal() {
  document.getElementById('addModal').classList.remove('open');
  ['newName', 'newPath', 'newTags', 'newNotes', 'newExclude'].forEach(id => { document.getElementById(id).value = ''; });
}
function setWatchType(t) {
  newWatchType = t;
  document.getElementById('typeLocal').className = 'option-btn' + (t === 'local' ? ' active' : '');
  document.getElementById('typeCloud').className = 'option-btn' + (t === 'cloud' ? ' active' : '');
}

async function addWatch() {
  const name  = document.getElementById('newName').value.trim();
  const path  = document.getElementById('newPath').value.trim();
  const tags  = document.getElementById('newTags').value.trim().split(',').map(s => s.trim()).filter(Boolean);
  const notes = document.getElementById('newNotes').value.trim();
  const excludePatterns = document.getElementById('newExclude').value
    .split('\n').map(s => s.trim()).filter(Boolean);
  if (!name || !path) { toast('Name and path required', 'error'); return; }
  const btn = document.querySelector('#addModal .btn-green');
  if (btn) { btn.disabled = true; btn.innerHTML = '<span class="spinner"></span>'; }
  const r = await fetch('/api/watches', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, path, type: newWatchType, tags, notes, exclude_patterns: excludePatterns }),
  });
  const d = await r.json();
  if (btn) { btn.disabled = false; btn.innerHTML = 'ADD TARGET'; }
  if (d.error) { toast(d.error, 'error'); return; }
  closeAddModal();
  toast(`"${name}" added!`);
  loadWatches(); loadDashboard();
}

async function removeWatch(id) {
  if (!confirm('Remove this watch target?')) return;
  await fetch(`/api/watches/${id}`, { method: 'DELETE' });
  // Clean up any running poll for this watch
  if (pollTimers[id]) {
    clearInterval(pollTimers[id]);
    delete pollTimers[id];
  }
  _runningBackups.delete(id);
  toast('Removed', 'info');
  loadWatches(); loadDashboard();
}


// ── Edit Watch Meta Modal ─────────────────────────────────────────────────────

function openEditWatchModal(watchId, currentName, currentTags, currentNotes, currentPath) {
  document.getElementById('editWatchId').value    = watchId;
  document.getElementById('editWatchName').value  = currentName;
  document.getElementById('editWatchTags').value  = currentTags;
  document.getElementById('editWatchNotes').value = currentNotes;
  const pathInput = document.getElementById('editWatchPath');
  if (pathInput) pathInput.value = currentPath || '';
  const excludeEl = document.getElementById('editWatchExclude');
  if (excludeEl) {
    fetch('/api/watches').then(r => r.json()).then(watches => {
      const w = watches.find(x => x.id === watchId);
      if (w) excludeEl.value = (w.exclude_patterns || []).join('\n');
      const mbEl = document.getElementById('editWatchMaxBackups');
      if (mbEl && w) mbEl.value = w.max_backups || 0;
      const skipEl = document.getElementById('editWatchSkipAuto');
      if (skipEl && w) skipEl.checked = !!w.skip_auto_backup;
    });
  }
  document.getElementById('editWatchModal').classList.add('open');
}
function closeEditWatchModal() { document.getElementById('editWatchModal').classList.remove('open'); }

async function saveWatchMeta() {
  const watchId   = document.getElementById('editWatchId').value;
  const name      = document.getElementById('editWatchName').value.trim();
  const tags      = document.getElementById('editWatchTags').value.trim().split(',').map(s => s.trim()).filter(Boolean);
  const notes     = document.getElementById('editWatchNotes').value.trim();
  const pathInput = document.getElementById('editWatchPath');
  const newPath   = pathInput ? pathInput.value.trim() : '';
  if (!name) { toast('Name required', 'error'); return; }
  const btn = document.querySelector('#editWatchModal .btn-green');
  if (btn) { btn.disabled = true; btn.innerHTML = '<span class="spinner"></span>'; }
  const excludeEl = document.getElementById('editWatchExclude');
  const excludePatterns = excludeEl
    ? excludeEl.value.split('\n').map(s => s.trim()).filter(Boolean)
    : undefined;

  const mbEl      = document.getElementById('editWatchMaxBackups');
  const maxBackups = mbEl ? Math.max(0, parseInt(mbEl.value) || 0) : undefined;

  const skipEl = document.getElementById('editWatchSkipAuto');
  const skipAuto = skipEl ? skipEl.checked : undefined;

  const r = await fetch(`/api/watches/${watchId}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      name, tags, notes,
      ...(excludePatterns !== undefined && { exclude_patterns:  excludePatterns }),
      ...(maxBackups      !== undefined && { max_backups:       maxBackups }),
      ...(skipAuto        !== undefined && { skip_auto_backup:  skipAuto }),
    }),
  });
  const d = await r.json();
  if (newPath) {
    const rp = await fetch(`/api/watches/${watchId}/rename-path`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path: newPath }),
    });
    const dp = await rp.json();
    if (dp.error) {
      if (btn) { btn.disabled = false; btn.innerHTML = 'SAVE'; }
      toast('Meta saved but path update failed: ' + dp.error, 'warn');
      closeEditWatchModal();
      loadWatches(); loadDashboard();
      return;
    }
  }
  if (btn) { btn.disabled = false; btn.innerHTML = 'SAVE'; }
  if (d.error) { toast(d.error, 'error'); return; }
  closeEditWatchModal();
  toast('Watch updated');
  loadWatches(); loadDashboard();
}

function toggleFileBrowser() {
  const fb = document.querySelector('.file-browser');
  if (!fb) return;
  fb.classList.toggle('mobile-visible');
}

function handleEditorKeydown(e) {
  if (e.key === 'Tab') {
    e.preventDefault();
    const ta    = e.target;
    const start = ta.selectionStart;
    const end   = ta.selectionEnd;
    ta.value = ta.value.substring(0, start) + '  ' + ta.value.substring(end);
    ta.selectionStart = ta.selectionEnd = start + 2;
    onEditorInput();
  }
}

async function uploadFiles(files) {
  if (!editorCurrentBrowsePath) { toast('Select a watch target first', 'error'); return; }
  if (!files || !files.length) return;
  const form = new FormData();
  form.append('path', editorCurrentBrowsePath);
  for (const f of files) form.append('files', f);
  toast(`Uploading ${files.length} file${files.length !== 1 ? 's' : ''}…`, 'info');
  try {
    const r = await fetch('/api/files/upload', { method: 'POST', body: form });
    const d = await r.json();
    if (!d.ok) { toast(d.error || 'Upload failed', 'error'); return; }
    if (d.errors && d.errors.length) toast(`⚠ ${d.errors[0]}`, 'warn');
    if (d.uploaded.length) toast(`Uploaded: ${d.uploaded.join(', ')}`, 'success');
    browseFiles(editorCurrentBrowsePath);
  } catch (e) {
    toast('Upload failed: ' + e.message, 'error');
  }
  const inp = document.getElementById('fileUploadInput');
  if (inp) inp.value = '';
}

// ── File Editor ───────────────────────────────────────────────────────────────

async function loadEditorSidebar() {
  const r = await fetch('/api/watches');
  const watches = await r.json();
  const sel = document.getElementById('watchSelector');
  if (!watches.length) {
    sel.innerHTML = '<div style="font-family:var(--mono);font-size:11px;color:var(--dim);padding:6px 0">No watch targets yet</div>';
    return;
  }
  sel.innerHTML = '<div style="font-family:var(--mono);font-size:9px;letter-spacing:2px;color:var(--dim);margin-bottom:6px">WATCH TARGETS</div>'
    + watches.map(w => `<button class="watch-select-btn ${editorCurrentWatchId === w.id ? 'active' : ''}" onclick="openEditorForWatch('${w.id}','${w.name}','${w.path.replace(/\\/g, '\\\\').replace(/'/g, "\\'")}')">
      ${w.type === 'cloud' ? '☁' : '📁'} ${w.name}${w.paused ? ' ⏸' : ''}</button>`).join('');
}

function openEditorForWatch(watchId, watchName, watchPath) {
  editorCurrentWatchId    = watchId;
  editorCurrentBrowsePath = watchPath;
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-tab').forEach(t => t.classList.remove('active'));
  document.getElementById('page-editor').classList.add('active');
  document.querySelectorAll('.nav-tab')[2].classList.add('active');
  currentPage = 'editor';
  loadEditorSidebar();
  browseFiles(watchPath);
}

async function browseFiles(path) {
  editorCurrentBrowsePath = path;
  document.getElementById('browserPath').textContent = path;
  const r = await fetch('/api/files/browse', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path }),
  });
  if (!r.ok) {
    const d = await r.json();
    document.getElementById('fileList').innerHTML = `<div style="padding:14px;font-family:var(--mono);font-size:11px;color:var(--red)">${d.error || 'Error'}</div>`;
    return;
  }
  const d = await r.json();
  _allFileItems = d.items;
  renderFileList(d.items, d.parent && d.parent !== path ? d.parent : null);
  const fs = document.getElementById('fileSearch');
  if (fs) fs.value = '';
}

function renderFileList(items, parentPath) {
  let html = '';
  if (parentPath) html += `<div class="back-btn" onclick="browseFiles('${parentPath.replace(/\\/g, '\\\\').replace(/'/g, "\\'")}')">← ..</div>`;
  if (!items.length) html += '<div style="padding:14px;font-family:var(--mono);font-size:11px;color:var(--dim)">Empty folder</div>';
  items.forEach(item => {
    const icon = fileIcon(item);
    const safePath = item.path.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
    let cls, action, titleAttr = '', contextAttr = '';
    if (item.is_dir) {
      cls    = 'is-dir';
      action = `onclick="browseFiles('${safePath}')"`;
    } else if (item.too_large) {
      cls       = 'not-editable';
      action    = '';
      titleAttr = `title="Too large to edit (${item.size ? (item.size / 1048576).toFixed(1) + ' MB' : '?'}). Limit is 5 MB."`;
    } else if (item.editable) {
      cls    = 'is-file';
      action = `onclick="openFile('${safePath}')"`;
      contextAttr = `oncontextmenu="showFileContextMenu(event,'${safePath}','${item.name.replace(/'/g, "\\'")}')"`;
    } else {
      cls    = 'not-editable';
      action = '';
      contextAttr = `oncontextmenu="showFileContextMenu(event,'${safePath}','${item.name.replace(/'/g, "\\'")}')"`;
    }
    html += `<div class="file-item ${cls}" ${action} ${titleAttr} ${contextAttr}>
      <span class="file-icon">${icon}</span>
      <span class="file-name">${item.name}</span>
      ${item.is_file ? `<span class="file-size" title="${item.mtime ? new Date(item.mtime*1000).toLocaleString() : ''}">${item.too_large ? '⊘' : fmtSize(item.size)}</span>` : ''}
      ${item.is_file && item.mtime ? `<span class="file-size" style="font-size:9px;color:var(--dim);margin-left:2px">${new Date(item.mtime*1000).toLocaleDateString('en-US',{month:'short',day:'numeric'})}</span>` : ''}
    </div>`;
  });
  document.getElementById('fileList').innerHTML = html;
}

function filterFiles(query) {
  const q = query.trim().toLowerCase();
  if (!q) { renderFileList(_allFileItems, null); return; }
  renderFileList(_allFileItems.filter(i => i.name.toLowerCase().includes(q)), null);
}

async function openFile(path) {
  if (editorModified && !confirm('Unsaved changes. Discard?')) return;
  const r = await fetch('/api/files/read', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path }),
  });
  if (!r.ok) {
    const d = await r.json();
    if (d.too_large) toast(d.error || 'File too large to edit', 'warn');
    else             toast(d.error || 'Cannot open file', 'error');
    return;
  }
  const d = await r.json();
  editorCurrentFile     = d;
  editorOriginalContent = d.content;
  editorModified        = false;
  document.getElementById('editorEmpty').style.display    = 'none';
  document.getElementById('editorPaneWrap').style.display = 'flex';
  closeDiff(); closeFindBar();
  renderEditorWithLineNumbers(d.content);
  document.getElementById('editorFilename').textContent  = d.name;
  document.getElementById('editorFilename').style.color  = 'var(--text-bright)';
  document.getElementById('editorMeta').textContent      = `${d.lines} lines · ${fmtSize(d.size)}`;
  document.getElementById('saveBtn').disabled            = false;
  document.getElementById('backupAfterSave').style.display = editorCurrentWatchId ? 'inline-flex' : 'none';
  toast(`Opened ${d.name}`, 'info');
}


// ── File Context Menu ─────────────────────────────────────────────────────────

function showFileContextMenu(e, filePath, fileName) {
  e.preventDefault();
  e.stopPropagation();
  _ctxTarget = { path: filePath, name: fileName };
  hideAllContextMenus();

  let menu = document.getElementById('fileContextMenu');
  if (!menu) {
    menu = document.createElement('div');
    menu.id = 'fileContextMenu';
    menu.className = 'context-menu';
    document.body.appendChild(menu);
  }
  menu.innerHTML = `
    <div class="ctx-item" onclick="ctxRenameFile()">✎ Rename</div>
    <div class="ctx-item ctx-danger" onclick="ctxDeleteFile()">🗑 Delete</div>`;
  menu.style.display = 'block';
  menu.style.left    = Math.min(e.clientX, window.innerWidth  - 160) + 'px';
  menu.style.top     = Math.min(e.clientY, window.innerHeight - 80)  + 'px';
}

function hideAllContextMenus() {
  ['fileContextMenu', 'watchContextMenu'].forEach(id => {
    const m = document.getElementById(id);
    if (m) m.style.display = 'none';
  });
}

async function ctxRenameFile() {
  hideAllContextMenus();
  if (!_ctxTarget) return;
  const newName = prompt(`Rename "${_ctxTarget.name}" to:`, _ctxTarget.name);
  if (!newName || newName === _ctxTarget.name) return;
  const r = await fetch('/api/files/rename', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ old_path: _ctxTarget.path, new_name: newName }),
  });
  const d = await r.json();
  if (d.error) { toast(d.error, 'error'); return; }
  toast(`Renamed to ${newName}`);
  if (editorCurrentFile && editorCurrentFile.path === _ctxTarget.path) {
    document.getElementById('editorEmpty').style.display    = 'flex';
    document.getElementById('editorPaneWrap').style.display = 'none';
    editorCurrentFile = null; editorModified = false;
    toast('Renamed — file closed. Reopen from browser.', 'info');
  }
  browseFiles(editorCurrentBrowsePath);
}

async function ctxDeleteFile() {
  hideAllContextMenus();
  if (!_ctxTarget) return;
  if (!confirm(`Delete "${_ctxTarget.name}"? This cannot be undone.`)) return;
  const r = await fetch('/api/files/delete', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path: _ctxTarget.path }),
  });
  const d = await r.json();
  if (d.error) { toast(d.error, 'error'); return; }
  toast(`Deleted ${_ctxTarget.name}`, 'info');
  if (editorCurrentFile && editorCurrentFile.path === _ctxTarget.path) {
    document.getElementById('editorEmpty').style.display    = 'flex';
    document.getElementById('editorPaneWrap').style.display = 'none';
    editorCurrentFile = null; editorModified = false;
  }
  browseFiles(editorCurrentBrowsePath);
}

document.addEventListener('click', hideAllContextMenus);


// ── Line-number editor ────────────────────────────────────────────────────────

function renderEditorWithLineNumbers(content) {
  const lines = content.split('\n');
  const nums  = document.getElementById('lineNumbers');
  const ta    = document.getElementById('editorTextarea');
  if (nums) nums.innerHTML = lines.map((_, i) => `<div>${i + 1}</div>`).join('');
  ta.value = content;
}

function syncLineNumbers() {
  const ta   = document.getElementById('editorTextarea');
  const nums = document.getElementById('lineNumbers');
  if (!nums) return;
  const lines = (ta.value || '').split('\n');
  nums.innerHTML = lines.map((_, i) => `<div>${i + 1}</div>`).join('');
  nums.scrollTop = ta.scrollTop;
}

function onEditorInput() {
  syncLineNumbers();
  if (!editorCurrentFile) return;
  editorModified = document.getElementById('editorTextarea').value !== editorOriginalContent;
  const fn = document.getElementById('editorFilename');
  fn.textContent = editorCurrentFile.name + (editorModified ? ' ●' : '');
  fn.style.color = editorModified ? 'var(--amber)' : 'var(--text-bright)';
  document.getElementById('editorMeta').textContent = editorModified ? 'unsaved changes' : `${editorCurrentFile.lines} lines`;
}

async function saveFile() {
  if (!editorCurrentFile) return;
  const content = document.getElementById('editorTextarea').value;
  const btn     = document.getElementById('saveBtn');
  btn.disabled  = true;
  btn.innerHTML = '<span class="spinner"></span> SAVING...';
  const r = await fetch('/api/files/save', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path: editorCurrentFile.path, content }),
  });
  const d = await r.json();
  btn.disabled  = false;
  btn.innerHTML = '💾 SAVE';
  if (d.error) { toast(d.error, 'error'); return; }
  editorOriginalContent = content;
  editorModified        = false;
  document.getElementById('editorFilename').textContent = editorCurrentFile.name;
  document.getElementById('editorFilename').style.color = 'var(--text-bright)';
  editorCurrentFile.hash = d.new_hash;
  if (d.changed) {
    toast(`Saved — +${d.lines_added} / -${d.lines_removed} lines`);
    showDiff(d.diff, d.lines_added, d.lines_removed);
  } else {
    toast('Saved (no changes)', 'info');
  }
}

function showDiff(diffLines, added, removed) {
  document.getElementById('diffPane').style.display = 'flex';
  document.getElementById('diffStats').innerHTML =
    `<span style="color:var(--green)">+${added} added</span><span style="color:var(--red)">-${removed} removed</span><span style="color:var(--dim)">vs last save</span>`;
  const CONTEXT = 3;
  const changed = new Set();
  diffLines.forEach((l, i) => {
    if (l.type !== 'equal') {
      for (let j = Math.max(0, i - CONTEXT); j <= Math.min(diffLines.length - 1, i + CONTEXT); j++) changed.add(j);
    }
  });
  let html = '', skipping = false;
  diffLines.forEach((line, i) => {
    if (!changed.has(i)) { if (!skipping) { html += '<div style="padding:1px 10px;font-family:var(--mono);font-size:11px;color:var(--dim)">...</div>'; skipping = true; } return; }
    skipping = false;
    const type   = line.type;
    const prefix = type === 'added' ? '+' : type === 'removed' ? '-' : ' ';
    const cls    = type === 'added' ? 'added' : type === 'removed' ? 'removed' : '';
    const pcls   = type === 'added' ? 'p-add' : type === 'removed' ? 'p-rem' : 'p-eq';
    const oln    = line.old_ln ? `<span class="diff-ln">${line.old_ln}</span>` : '<span class="diff-ln" style="opacity:0">0</span>';
    const nln    = line.new_ln ? `<span class="diff-ln">${line.new_ln}</span>` : '<span class="diff-ln" style="opacity:0">0</span>';
    const text   = (line.text || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    html += `<div class="diff-line ${cls}">${oln}${nln}<span class="diff-prefix ${pcls}">${prefix}</span><span class="diff-text">${text}</span></div>`;
  });
  document.getElementById('diffContent').innerHTML = html || '<div style="padding:18px;font-family:var(--mono);font-size:11px;color:var(--dim)">No changes</div>';
}

function closeDiff() { document.getElementById('diffPane').style.display = 'none'; }


// ── Find in editor ────────────────────────────────────────────────────────────

function openFindBar() {
  if (!editorCurrentFile) { toast('Open a file first', 'warn'); return; }
  const bar = document.getElementById('findBar');
  bar.classList.add('open');
  document.getElementById('findInput').focus();
  document.getElementById('findInput').select();
}

function closeFindBar() {
  document.getElementById('findBar').classList.remove('open');
  _findMatches  = [];
  _findCurrent  = -1;
  document.getElementById('findCount').textContent = '';
}

function findInEditor() {
  const ta      = document.getElementById('editorTextarea');
  const q       = document.getElementById('findInput').value;
  if (!q) { document.getElementById('findCount').textContent = ''; _findMatches = []; return; }
  const content = ta.value;
  const lower   = content.toLowerCase();
  const ql      = q.toLowerCase();
  _findMatches = [];
  let i = 0;
  while ((i = lower.indexOf(ql, i)) !== -1) { _findMatches.push(i); i += ql.length; }
  if (_findMatches.length && _findCurrent === -1) { _findCurrent = 0; _scrollToMatch(ta, q); }
  document.getElementById('findCount').textContent = _findMatches.length
    ? `${Math.min(_findCurrent + 1, _findMatches.length)}/${_findMatches.length} matches`
    : 'No matches';
}

function findNext() {
  if (!_findMatches.length) return;
  _findCurrent = (_findCurrent + 1) % _findMatches.length;
  _scrollToMatch(document.getElementById('editorTextarea'), document.getElementById('findInput').value);
}

function findPrev() {
  if (!_findMatches.length) return;
  _findCurrent = (_findCurrent - 1 + _findMatches.length) % _findMatches.length;
  _scrollToMatch(document.getElementById('editorTextarea'), document.getElementById('findInput').value);
}

function _scrollToMatch(ta, q) {
  const pos   = _findMatches[_findCurrent];
  ta.focus();
  ta.setSelectionRange(pos, pos + q.length);
  const lines = ta.value.substring(0, pos).split('\n');
  const lineH = parseFloat(getComputedStyle(ta).lineHeight) || 22;
  ta.scrollTop = Math.max(0, (lines.length - 3) * lineH);
  document.getElementById('findCount').textContent = `${_findCurrent + 1}/${_findMatches.length} matches`;
}

function replaceOne() {
  const ta   = document.getElementById('editorTextarea');
  const find = document.getElementById('findInput').value;
  const repl = document.getElementById('replaceInput').value;
  if (!find || _findMatches.length === 0) return;
  const pos     = _findMatches[_findCurrent];
  const content = ta.value;
  ta.value      = content.substring(0, pos) + repl + content.substring(pos + find.length);
  onEditorInput();
  _findCurrent = -1;
  findInEditor();
  findNext();
  toast(`Replaced 1 occurrence`, 'info');
}

function replaceAll() {
  const ta   = document.getElementById('editorTextarea');
  const find = document.getElementById('findInput').value;
  const repl = document.getElementById('replaceInput').value;
  if (!find) return;
  const count = (ta.value.split(find).length - 1);
  ta.value    = ta.value.split(find).join(repl);
  onEditorInput();
  _findMatches = []; _findCurrent = -1;
  document.getElementById('findCount').textContent = '';
  toast(`Replaced ${count} occurrence${count !== 1 ? 's' : ''}`, count > 0 ? 'success' : 'info');
}

async function promptNewFile() {
  if (!editorCurrentBrowsePath) { toast('Select a watch target first', 'error'); return; }
  const name = prompt('New file name (e.g. notes.txt):');
  if (!name) return;
  const fullPath = editorCurrentBrowsePath.replace(/[/\\]$/, '') + '/' + name;
  const r = await fetch('/api/files/create', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path: fullPath }),
  });
  const d = await r.json();
  if (d.error) { toast(d.error, 'error'); return; }
  toast(`Created ${name}`);
  browseFiles(editorCurrentBrowsePath);
  setTimeout(() => openFile(d.path), 300);
}

async function promptNewFolder() {
  if (!editorCurrentBrowsePath) { toast('Select a watch target first', 'error'); return; }
  const name = prompt('New folder name:');
  if (!name) return;
  if (name.includes('/') || name.includes('\\')) { toast('Folder name cannot contain slashes', 'error'); return; }
  const fullPath = editorCurrentBrowsePath.replace(/[/\\]$/, '') + '/' + name;
  const r = await fetch('/api/files/mkdir', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path: fullPath }),
  });
  const d = await r.json();
  if (d.error) { toast(d.error, 'error'); return; }
  toast(`Created folder: ${name}`);
  browseFiles(editorCurrentBrowsePath);
}


// ── History ───────────────────────────────────────────────────────────────────

async function loadHistory(page = 1) {
  if (page === 1) {
    _allBackups = [];
    _historySelected.clear();
  }

  // STABILITY UPGRADE: Use ID anchoring for pagination
  const params = new URLSearchParams({
    page,
    per_page: 100,
    ...(_historyFilterQ      && { q:        _historyFilterQ }),
    ...(_historyFilterStatus && { status:   _historyFilterStatus }),
    ...(_historyFilterWatch  && { watch_id: _historyFilterWatch }),
    ...(_historyFilterFrom   && { from:     _historyFilterFrom }),
    ...(_historyFilterTo     && { to:       _historyFilterTo }),
  });

  // Only fetch watches + stats on the first page — skip on "load more"
  const fetches = [fetch('/api/history?' + params)];
  if (page === 1) {
    fetches.push(fetch('/api/history/watches'), fetch('/api/history/stats?' + params));
  }
  const results = await Promise.all(fetches);
  const data    = await results[0].json();
  const watches = page === 1 ? await results[1].json() : [];
  const stats   = page === 1 ? await results[2].json() : null;

  // Render aggregate stats bar
  const aggEl = document.getElementById('historyAggregateStats');
  if (aggEl && stats) {
    aggEl.style.display = 'flex';
    document.getElementById('aggTotal').textContent     = stats.total     || 0;
    document.getElementById('aggSuccess').textContent   = stats.success   || 0;
    document.getElementById('aggFailed').textContent    = stats.failed    || 0;
    document.getElementById('aggCancelled').textContent = stats.cancelled || 0;
    document.getElementById('aggFiles').textContent     = (stats.total_files || 0).toLocaleString();

    const aggSizeEl = document.getElementById('aggSize');
    if (aggSizeEl) aggSizeEl.textContent = stats.total_size_human || '—';
  }

  // Handle both paginated { backups, total, page, pages } and legacy flat array
  let pageBackups, total, pages;
  if (Array.isArray(data)) {
    pageBackups = data;
    total = data.length;
    pages = 1;
  } else {
    pageBackups = data.backups || [];
    total       = data.total || 0;
    pages       = data.pages || 1;
  }

  // Merge new page results with existing backups
  if (page === 1) {
    _allBackups = pageBackups;
  } else {
    _allBackups = [..._allBackups, ...pageBackups];
  }

  _historyTotal = total;
  _historyPage  = page;
  _historyPages = pages;

  // Update history subtitle
  document.getElementById('historySubtitle').textContent =
    total > _allBackups.length
      ? `Showing ${_allBackups.length} of ${total} backups`
      : `${total} total backups`;

  // Populate watch filter dropdown (only on first page)
  const wf = document.getElementById('historyWatchFilter');
  if (wf && page === 1) {
    wf.innerHTML = '<option value="">All watches</option>'
      + watches.map(w => `<option value="${w.id}">${w.name}</option>`).join('');
    wf.value = _historyFilterWatch;
  }

  // Restore status, search, and date filters (first page only)
  if (page === 1) {
    const sf = document.getElementById('historyStatusFilter');
    if (sf) sf.value = _historyFilterStatus;

    const sq = document.getElementById('historySearch');
    if (sq) sq.value = _historyFilterQ;

    const fromEl = document.getElementById('historyFromDate');
    if (fromEl) fromEl.value = _historyFilterFrom;

    const toEl = document.getElementById('historyToDate');
    if (toEl) toEl.value = _historyFilterTo;
  }

  // Render the list of backups
  renderHistoryList(_allBackups);

  // Start polling for live updates
  _startHistoryPoll();
}

function _startHistoryPoll() {
  _stopHistoryPoll();
  let _prevRunningCount = -1;
  const check = async () => {
    if (currentPage !== 'history') { _stopHistoryPoll(); return; }
    try {
      const r = await fetch('/api/backup/all/status');
      const s = await r.json();
      if (s.running_count > 0) {
        const runningList = s.running_ids.map(wid => {
          const st     = s.statuses[wid] || {};
          const recent = _allBackups.find(b => b.watch_id === wid);
          _updateHistoryProgress(wid, st.progress || 0, true);
          return { watchId: wid, watchName: recent ? recent.watch_name : wid, pct: st.progress || 0 };
        });
        _showHistoryRunningBanner(runningList);
      } else {
        _hideHistoryRunningBanner();
        // Reload history list when backups finish (transition from running → idle)
        if (_prevRunningCount > 0) {
          loadHistory(1);
        }
      }
      _prevRunningCount = s.running_count;
    } catch (_) {}
  };
  _historyPollTimer = setInterval(check, 2000);
}

function _stopHistoryPoll() {
  if (_historyPollTimer) { clearInterval(_historyPollTimer); _historyPollTimer = null; }
}

function _showHistoryRunningBanner(runningList) {
  // runningList: [{watchId, watchName, pct}]
  let banner = document.getElementById('historyRunningBanner');
  if (!banner) {
    banner = document.createElement('div');
    banner.id = 'historyRunningBanner';
    banner.style.cssText = 'background:#0d2a1a;border:1px solid var(--green);border-radius:8px;padding:10px 16px;margin-bottom:14px;font-family:var(--mono);font-size:12px;color:var(--green)';
    const heading = document.querySelector('#page-history .section-heading');
    if (heading) heading.insertAdjacentElement('afterend', banner);
  }
  banner.innerHTML = runningList.map(({ watchId, watchName, pct }) => `
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:4px">
      <span style="animation:pulse 1s infinite;font-size:10px">●</span>
      <span style="flex:1">${escapeHtml(watchName)} — in progress</span>
      <div style="background:var(--bg3);border-radius:4px;height:5px;overflow:hidden;width:140px">
        <div id="hist_pbar_${watchId}" style="height:5px;background:var(--green);width:${pct}%;transition:width .3s"></div>
      </div>
      <span id="hist_ppct_${watchId}" style="min-width:34px">${pct}%</span>
    </div>`).join('');
}

function _hideHistoryRunningBanner() {
  const banner = document.getElementById('historyRunningBanner');
  if (banner) banner.remove();
}

function _ledClass(status) {
  if (status === 'success')   return 'led-green';
  if (status === 'cancelled') return 'led-amber';
  return 'led-red';
}

function renderHistoryList(backups) {
  const el = document.getElementById('historyList');
  const bulkBar = document.getElementById('historyBulkBar');
  if (bulkBar) {
    bulkBar.style.display = _historySelected.size > 0 ? 'flex' : 'none';
    const countEl = document.getElementById('historyBulkCount');
    if (countEl) countEl.textContent = `${_historySelected.size} selected`;
  }

  if (!backups.length) {
    const hasFilter = _historyFilterQ || _historyFilterStatus || _historyFilterWatch || _historyFilterFrom || _historyFilterTo;
    el.innerHTML = `<div class="empty-state">
      <div style="font-size:36px;margin-bottom:12px">📋</div>
      <div>${hasFilter
        ? 'No backups match your filters.'
        : 'No backups yet.'}
      </div>
      ${hasFilter ? `<button class="btn btn-ghost btn-sm" style="margin-top:12px" onclick="clearAllHistoryFilters()">✕ Clear Filters</button>` : ''}
    </div>`;
    return;
  }

  const rows = backups.map(b => `
    <div class="history-row ${selectedBackup && selectedBackup.backup_id === b.backup_id ? 'selected' : ''} ${_historySelected.has(b.backup_id) ? 'bulk-selected' : ''}">
      <input type="checkbox" class="history-checkbox" ${_historySelected.has(b.backup_id) ? 'checked' : ''}
        onclick="event.stopPropagation();toggleHistorySelect('${b.backup_id}')"
        style="margin:0;cursor:pointer;accent-color:var(--amber);flex-shrink:0"/>
      <div onclick="selectBackupById('${b.backup_id}')" style="display:flex;align-items:center;gap:14px;flex:1;cursor:pointer">
        <div class="status-led ${_ledClass(b.status)}"></div>
        <div style="flex:1;min-width:0">
          <div style="color:var(--text-bright);font-size:13px;font-weight:500">${escapeHtml(b.watch_name || '?')}</div>
          <div style="font-family:var(--mono);color:var(--dim);font-size:10px">
            ${fmtTs(b.timestamp)} · ${b.incremental ? 'incremental' : 'full'}
            ${b.status === 'cancelled' ? ' · <span style="color:var(--amber)">cancelled</span>' : ''}
          </div>
          ${b.user_notes ? `<div style="font-size:10px;color:var(--amber);margin-top:2px;font-style:italic">📝 ${escapeHtml(b.user_notes)}</div>` : ''}
        </div>
        <div style="text-align:right;flex-shrink:0">
          <div style="font-family:var(--mono);font-size:11px">${b.files_copied || 0} files</div>
          <div style="font-family:var(--mono);font-size:10px;color:var(--dim)">${b.total_size || '—'}</div>
        </div>
      </div>
    </div>`).join('');

  // FIX #4: Load more button with stable class for scroll-preserving append
  const loadMoreHtml = (_historyPage < _historyPages)
    ? `<div class="load-more-btn" style="text-align:center;padding:16px">
        <button class="btn btn-ghost" onclick="loadHistory(${_historyPage + 1})">
          Load more (${_historyTotal - _allBackups.length} remaining)
        </button>
       </div>`
    : '';

  // FIX #4: Preserve scroll on load-more (page > 1), full replace on fresh load (page 1)
  const prevScroll = _historyPage > 1 ? el.scrollTop : 0;
  el.innerHTML = rows + loadMoreHtml;
  if (_historyPage > 1) {
    el.scrollTop = prevScroll;
  } else if (selectedBackup) {
    // Scroll to selected backup
    setTimeout(() => {
      const sel = el.querySelector('.history-row.selected');
      if (sel) sel.scrollIntoView({ block: 'center', behavior: 'smooth' });
    }, 50);
  }
}

function toggleHistorySelect(backupId) {
  if (_historySelected.has(backupId)) _historySelected.delete(backupId);
  else                                _historySelected.add(backupId);
  renderHistoryList(_allBackups);
}

function toggleSelectAllHistory() {
  if (_historySelected.size === _allBackups.length) _historySelected.clear();
  else                                              _allBackups.forEach(b => _historySelected.add(b.backup_id));
  renderHistoryList(_allBackups);
}

async function bulkDeleteSelected() {
  if (!_historySelected.size) return;
  if (!confirm(`Delete ${_historySelected.size} selected backup${_historySelected.size !== 1 ? 's' : ''}? This cannot be undone.`)) return;
  const ids = [..._historySelected];
  let deleted = 0, notFound = 0;
  for (const id of ids) {
    const r = await fetch(`/api/history/${id}`, { method: 'DELETE' });
    if (r.status === 404) { notFound++; }
    else { const d = await r.json(); if (d.ok) deleted++; }
  }
  const suffix = notFound > 0 ? ` (${notFound} already gone)` : '';
  toast(`Deleted ${deleted} backup${deleted !== 1 ? 's' : ''}${suffix}`, 'info');
  _historySelected.clear();
  if (selectedBackup && ids.includes(selectedBackup.backup_id)) closeDetail();
  loadHistory(); loadDashboard();
}

async function bulkExportSelected() {
  if (!_historySelected.size) return;
  const ids = [..._historySelected];
  toast(`Preparing bulk export of ${ids.length} backup${ids.length !== 1 ? 's' : ''}...`, 'info');
  const r = await fetch('/api/history/export-bulk', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ backup_ids: ids }),
  });
  if (!r.ok) {
    const d = await r.json();
    toast(d.error || 'Export failed', 'error');
    return;
  }
  const blob = await r.blob();
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  a.href     = url;
  a.download = `backupsys_bulk_export.zip`;
  document.body.appendChild(a); a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
  toast(`Exported ${ids.length} backup${ids.length !== 1 ? 's' : ''}`, 'success');
}

function _getFilteredBackups() { return _allBackups; }

let _historySearchTimer = null;
function filterHistory(query) {
  _historyFilterQ = query;
  clearTimeout(_historySearchTimer);
  _historySearchTimer = setTimeout(() => loadHistory(1), 350);
}
function filterHistoryStatus(status)  { _historyFilterStatus = status;  loadHistory(1); }
function filterHistoryWatch(watchId)  { _historyFilterWatch  = watchId; loadHistory(1); }
function filterHistoryDate() {
  _historyFilterFrom = (document.getElementById('historyFromDate') || {}).value || '';
  _historyFilterTo   = (document.getElementById('historyToDate')   || {}).value || '';
  loadHistory(1);
}

async function selectBackup(encoded) {
  let b;
  try {
    b = (typeof encoded === 'object' && encoded !== null)
      ? encoded
      : JSON.parse(decodeURIComponent(encoded));
  } catch { return; }
  selectedBackup = b;
  document.getElementById('historyLayout').classList.remove('no-detail');
  document.getElementById('detailPanel').style.display = 'block';
  // ADD this line
  const _dp = document.querySelector('.detail-panel');
  if (_dp) _dp.scrollTop = 0;
  const changes   = b.changes || [];
  const isSuccess = b.status === 'success' || (b.files_copied && b.files_copied > 0);
  const isCancelled = b.status === 'cancelled';

  let statusBg    = isSuccess ? '#0d2a1a' : isCancelled ? '#2a1800' : '#2a0d0d';
  let statusBord  = isSuccess ? '#2a5040' : isCancelled ? '#5a4000' : '#5a1a1a';
  let statusIcon  = isSuccess ? '✓'       : isCancelled ? '⚠'       : '✗';
  let statusColor = isSuccess ? 'var(--green)' : isCancelled ? 'var(--amber)' : 'var(--red)';
  let statusText  = isSuccess ? 'BACKUP SUCCESSFUL' : isCancelled ? 'BACKUP CANCELLED' : 'BACKUP FAILED';

  document.getElementById('detailContent').innerHTML = `
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px">
      <div style="font-family:var(--mono);font-size:11px;letter-spacing:3px;color:var(--dim)">BACKUP DETAIL</div>
      <button onclick="closeDetail()" style="background:none;border:none;cursor:pointer;color:var(--dim);font-size:18px">✕</button>
    </div>
    <div style="background:${statusBg};border:1px solid ${statusBord};border-radius:8px;padding:12px 14px;margin-bottom:14px;display:flex;align-items:center;gap:10px">
      <span style="font-size:20px">${statusIcon}</span>
      <span style="font-family:var(--mono);font-weight:700;font-size:13px;color:${statusColor}">${statusText}</span>
    </div>
    <div class="meta-row"><span class="meta-key">TARGET</span><span class="meta-value">${b.watch_name || '?'}</span></div>
    <div class="meta-row">
      <span class="meta-key">BACKUP ID</span>
      <span class="meta-value" style="display:flex;align-items:center;gap:8px">
        <span style="font-size:10px;color:var(--dim)">${b.backup_id}</span>
        <button onclick="navigator.clipboard.writeText('${b.backup_id}');toast('ID copied!','info')"
          style="background:none;border:1px solid var(--border2);border-radius:4px;cursor:pointer;color:var(--dim);font-size:9px;padding:1px 6px;font-family:var(--mono)"
          title="Copy backup ID">⧉</button>
      </span>
    </div>
    <div class="meta-row"><span class="meta-key">TIMESTAMP</span><span class="meta-value">${fmtTs(b.timestamp)}</span></div>
    <div class="meta-row"><span class="meta-key">TYPE</span><span class="meta-value">${b.incremental ? 'INCREMENTAL' : 'FULL'}</span></div>
    <div class="meta-row"><span class="meta-key">FILES</span><span class="meta-value">${b.files_copied || 0} copied</span></div>
    <div class="meta-row"><span class="meta-key">DURATION</span><span class="meta-value">${fmtDuration(b.duration_s)}</span></div>
    <div class="meta-row"><span class="meta-key">SIZE</span><span class="meta-value">${b.total_size || '—'}</span></div>
    ${b.throughput_mbs ? `<div class="meta-row"><span class="meta-key">SPEED</span><span class="meta-value" style="color:var(--blue)">${b.throughput_mbs} MB/s</span></div>` : ''}
    ${b.compressed ? `<div class="meta-row"><span class="meta-key">COMPRESSION</span><span class="meta-value" style="color:var(--purple)">✓ gzip (${b.compression_ratio || 0}% saved)</span></div>` : ''}

    <!-- Annotation / Notes -->
    <div style="margin-top:14px">
      <div style="font-family:var(--mono);font-size:9px;letter-spacing:2px;color:var(--dim);margin-bottom:6px">📝 NOTES</div>
      <div style="display:flex;gap:8px;align-items:flex-start">
        <textarea id="backupNotes_${b.backup_id}"
          style="flex:1;padding:8px 12px;background:var(--bg0);border:1px solid var(--border2);border-radius:6px;color:var(--text);font-family:var(--mono);font-size:11px;outline:none;resize:vertical;min-height:52px;line-height:1.5"
          placeholder="Add notes about this backup…"
          onfocus="this.style.borderColor='var(--amber)'" onblur="this.style.borderColor='var(--border2)'"
        >${b.user_notes || ''}</textarea>
        <button class="btn btn-amber btn-sm" onclick="saveBackupNotes('${b.backup_id}')">💾</button>
      </div>
    </div>

    <div class="hash-box" style="position:relative">
      <div class="hash-label">🔒 SHA-256 HASH</div>
      <div class="hash-value" id="hashValue_${b.backup_id}">${b.backup_hash || 'N/A'}</div>
      ${b.backup_hash ? `<button onclick="(()=>{navigator.clipboard.writeText('${b.backup_hash}');toast('Hash copied!','info')})()"
        style="position:absolute;top:10px;right:10px;background:none;border:1px solid var(--border2);border-radius:4px;cursor:pointer;color:var(--dim);font-size:10px;padding:2px 7px;font-family:var(--mono)"
        title="Copy hash">⧉</button>` : ''}
    </div>
    <div style="margin-top:12px;display:flex;gap:8px;flex-wrap:wrap">
      <button class="btn btn-blue btn-sm" onclick="validateBackup('${encodeURIComponent(b.backup_dir || '')}')">🔍 VALIDATE</button>
      <button class="btn btn-ghost btn-sm" onclick="browseBackupFiles('${b.backup_id}')">📂 BROWSE FILES</button>
      ${isSuccess ? `<button class="btn btn-green btn-sm" id="restoreBtn_${b.backup_id}" onclick="restoreBackup('${encodeURIComponent(b.backup_dir || '')}','${encodeURIComponent(b.source || '')}')">♻ RESTORE</button>` : ''}
      ${isSuccess ? `<button class="btn btn-ghost btn-sm" onclick="restoreToCustomPath('${b.backup_id}','${encodeURIComponent(b.backup_dir || '')}')">📁 RESTORE TO…</button>` : ''}
      ${isSuccess ? `<button class="btn btn-amber btn-sm" onclick="exportBackup('${b.backup_id}','${b.watch_name || 'backup'}')">⬇ EXPORT ZIP</button>` : ''}
      ${!isSuccess && b.watch_id ? `<button class="btn btn-green btn-sm" onclick="showPage('watches',document.querySelectorAll('.nav-tab')[1]);setTimeout(()=>runBackup('${b.watch_id}'),400)">🔄 RETRY BACKUP</button>` : ''}
      <button class="btn btn-red btn-sm" onclick="deleteBackup('${b.backup_id}')">🗑 DELETE</button>
    </div>
    <div id="backupBrowsePanel_${b.backup_id}" style="display:none;margin-top:12px"></div>
    <div id="validResult_${b.backup_id}"></div>
    ${changes.length ? `<div class="diff-list" style="margin-top:14px">
      <div style="font-family:var(--mono);font-size:10px;letter-spacing:2px;color:var(--dim);padding:8px 12px;border-bottom:1px solid var(--border)">CHANGED FILES (${changes.length})</div>
      ${changes.slice(0, 40).map(c => `<div style="display:flex;align-items:center;gap:8px;padding:5px 12px;border-bottom:1px solid #0f1a0f11">
        ${changeBadge(c.type)}<span style="font-family:var(--mono);font-size:11px;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${c.path}</span>
        <span style="font-family:var(--mono);font-size:9px;color:var(--dim)">${c.size ? fmtSize(c.size) : ''}</span>
      </div>`).join('')}
      ${changes.length > 40 ? `<div style="font-family:var(--mono);font-size:10px;color:var(--dim);padding:8px 12px">+${changes.length - 40} more files</div>` : ''}
    </div>` : ''}
    ${b.failed_files && b.failed_files.length ? `<div style="background:#2a0d0d;border:1px solid var(--red);border-radius:8px;padding:12px 14px;margin-top:12px">
      <div style="font-family:var(--mono);font-size:10px;letter-spacing:2px;color:var(--red);margin-bottom:6px">⚠ ${b.failed_files.length} FILES FAILED TO BACKUP</div>
      ${b.failed_files.slice(0,5).map(f => `<div style="font-family:var(--mono);font-size:9px;color:var(--dim);margin:3px 0">📁 ${f.path}</div>`).join('')}
      ${b.failed_files.length > 5 ? `<div style="font-family:var(--mono);font-size:9px;color:var(--dim);margin-top:6px">...and ${b.failed_files.length - 5} more</div>` : ''}
    </div>` : ''}`;
  renderHistoryList(_allBackups);
}

async function restoreToCustomPath(backupId, encodedDir) {
  const backup_dir = decodeURIComponent(encodedDir);
  const target = prompt('Restore to which folder?\n\nEnter full path (e.g. C:\\Temp\\restored):');
  if (!target || !target.trim()) return;
  if (!confirm(`Restore backup files to:\n${target}\n\nThis will overwrite files at that location. Continue?`)) return;
  const r = await fetch('/api/restore', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ backup_dir, target_path: target.trim(), custom_target: true }),
  });
  const d = await r.json();
  if (d.error) { toast(`Restore failed: ${d.error}`, 'error'); return; }
  toast(`Restored ${d.files_restored} file${d.files_restored !== 1 ? 's' : ''} to ${target.trim()}`, 'success');
}

// ── Backup Browse (preview before restore) ────────────────────────────────────

async function browseBackupFiles(backupId) {
  const panel = document.getElementById(`backupBrowsePanel_${backupId}`);
  if (!panel) return;
  if (panel.style.display !== 'none') { panel.style.display = 'none'; return; }

  panel.style.display = 'block';
  panel.innerHTML = '<div style="padding:12px;font-family:var(--mono);font-size:11px;color:var(--dim)">Loading backup contents…</div>';

  const r = await fetch(`/api/backup/${backupId}/browse`);
  const d = await r.json();

  if (d.error) {
    panel.innerHTML = `<div style="padding:12px;color:var(--red);font-family:var(--mono);font-size:11px">${d.error}</div>`;
    return;
  }

  const filesHtml = d.files.length
    ? d.files.map(f => `
      <div style="display:flex;align-items:center;gap:8px;padding:4px 10px;border-bottom:1px solid var(--border)">
        <span style="font-size:12px">📄</span>
        <span style="font-family:var(--mono);font-size:10px;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--text)"
              title="${escapeHtml(f.path)}">${escapeHtml(f.path)}</span>
        <span style="font-family:var(--mono);font-size:9px;color:var(--dim);flex-shrink:0;margin-right:4px">${f.size_human}</span>
        <a class="btn btn-blue btn-sm" style="padding:2px 7px;font-size:9px;text-decoration:none"
          href="/api/backup/${backupId}/download-file?path=${encodeURIComponent(f.path)}" download title="Download file">⬇</a>
        <button class="btn btn-green btn-sm" style="padding:2px 7px;font-size:9px"
          onclick="restoreSingleFile('${backupId}','${f.path.replace(/\\/g,'\\\\').replace(/'/g,"\\'")}',this)">♻</button>
      </div>`).join('')
    : '<div style="padding:10px;font-family:var(--mono);font-size:11px;color:var(--dim)">No files in this backup</div>';

  const deletedHtml = d.deleted.length
    ? `<div style="padding:6px 10px;background:#2a0d0d;border-top:1px solid var(--border)">
        <div style="font-family:var(--mono);font-size:9px;color:var(--red);letter-spacing:1px;margin-bottom:4px">DELETED IN THIS BACKUP</div>
        ${d.deleted.map(f => `<div style="font-family:var(--mono);font-size:10px;color:var(--red);text-decoration:line-through;padding:2px 0">${f}</div>`).join('')}
       </div>`
    : '';

  panel.innerHTML = `
    <div style="background:var(--bg0);border:1px solid var(--border);border-radius:8px;overflow:hidden;max-height:300px;overflow-y:auto">
      <div style="display:flex;align-items:center;justify-content:space-between;padding:8px 12px;border-bottom:1px solid var(--border);background:var(--bg2);position:sticky;top:0">
        <span style="font-family:var(--mono);font-size:9px;letter-spacing:2px;color:var(--dim)">${d.total} FILES IN BACKUP</span>
        <button onclick="document.getElementById('backupBrowsePanel_${backupId}').style.display='none'"
          style="background:none;border:none;cursor:pointer;color:var(--dim)">✕</button>
      </div>
      ${filesHtml}
      ${deletedHtml}
    </div>`;
}

async function resetWatchSnapshot(watchId) {
  if (!confirm('Clear snapshot for this watch?\nNext backup will copy ALL files (full backup).')) return;
  const r = await fetch(`/api/watches/${watchId}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ reset_snapshot: true }),
  });
  const d = await r.json();
  if (d.error) { toast(d.error, 'error'); return; }
  toast('Snapshot cleared — next backup will be full', 'info');
}


async function saveBackupNotes(backupId) {
  const ta = document.getElementById('backupNotes_' + backupId);
  if (!ta) return;
  const notes = ta.value.trim();
  const r = await fetch(`/api/history/${backupId}/annotate`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ notes }),
  });
  const d = await r.json();
  if (d.error) { toast(d.error, 'error'); return; }
  const idx = _allBackups.findIndex(b => b.backup_id === backupId);
  if (idx !== -1) _allBackups[idx].user_notes = notes;
  if (selectedBackup && selectedBackup.backup_id === backupId) selectedBackup.user_notes = notes;
  toast('Notes saved', 'success');
  renderHistoryList(_allBackups);
}

async function deleteBackup(backupId) {
  if (!confirm('Delete this backup permanently?')) return;
  const r = await fetch(`/api/history/${backupId}`, { method: 'DELETE' });
  const d = await r.json();
  if (d.error) { toast(d.error, 'error'); return; }
  toast('Backup deleted', 'info');
  closeDetail(); loadHistory(); loadDashboard();
}

async function exportBackup(backupId, watchName) {
  toast('Preparing zip export...', 'info');
  try {
    const r = await fetch(`/api/export/${backupId}`);
    if (!r.ok) {
      const d = await r.json().catch(() => ({}));
      toast(d.error || `Export failed (HTTP ${r.status})`, 'error');
      return;
    }
    const blob = await r.blob();
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href     = url;
    a.download = `backup_${(watchName || 'backup').replace(/[^a-z0-9_\-]/gi, '_')}.zip`;
    document.body.appendChild(a); a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    toast('Export downloaded', 'success');
  } catch (e) {
    toast('Export failed: ' + e.message, 'error');
  }
}

async function restoreBackup(encodedDir, encodedSource) {
  const backup_dir = decodeURIComponent(encodedDir);
  const source     = decodeURIComponent(encodedSource);
  if (!confirm(`Restore backup to:\n${source}\n\nThis will overwrite existing files. Continue?`)) return;
  const btn = selectedBackup ? document.getElementById(`restoreBtn_${selectedBackup.backup_id}`) : null;
  if (btn) { btn.disabled = true; btn.innerHTML = '<span class="spinner"></span> RESTORING...'; }
  const r = await fetch('/api/restore', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ backup_dir, target_path: source }),
  });
  const d = await r.json();
  if (btn) { btn.disabled = false; btn.innerHTML = '♻ RESTORE'; }
  if (d.error) { toast(`Restore failed: ${d.error}`, 'error'); return; }
  const msg = `Restored ${d.files_restored} file${d.files_restored !== 1 ? 's' : ''}${d.skipped ? ' (' + d.skipped + ' skipped)' : ''}`;
  toast(msg, 'success');
  const id = selectedBackup && selectedBackup.backup_id;
  const el = document.getElementById(`validResult_${id}`);
  if (el) el.innerHTML = `<div style="background:var(--bg0);border:1px solid var(--green);border-radius:8px;padding:12px;margin-top:10px;font-family:var(--mono);font-size:12px;color:var(--green)">✅ ${msg}</div>`;
}

async function restoreLatestBackup(watchId, watchName, triggerBtn) {
  toast('Finding latest backup…', 'info');
  const r = await fetch(`/api/history?watch_id=${encodeURIComponent(watchId)}&per_page=1&status=success`);
  const d = await r.json();
  const backups = Array.isArray(d) ? d : (d.backups || []);
  if (!backups.length) { toast(`No successful backups found for "${watchName}"`, 'warn'); return; }
  const b = backups[0];
  if (!confirm(`Restore latest backup of "${watchName}"?\n\nTimestamp: ${fmtTs(b.timestamp)}\nFiles: ${b.files_copied || 0}\n\nThis will overwrite existing files.`)) return;
  const btn = triggerBtn || null;
  if (btn) { btn.disabled = true; btn.innerHTML = '<span class="spinner"></span>'; }
  const r2 = await fetch('/api/restore', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ backup_dir: b.backup_dir, target_path: b.source }),
  });
  const d2 = await r2.json();
  if (btn) { btn.disabled = false; btn.innerHTML = '♻ LAST'; }
  if (d2.error) { toast(`Restore failed: ${d2.error}`, 'error'); return; }
  toast(`Restored ${d2.files_restored} file${d2.files_restored !== 1 ? 's' : ''} from ${fmtTs(b.timestamp)}`, 'success');
}

async function restoreSingleFile(backupId, filePath, btn) {
  if (!confirm(`Restore "${filePath}" to its original location?\nThis will overwrite the current version.`)) return;
  if (btn) { btn.disabled = true; btn.innerHTML = '<span class="spinner"></span>'; }
  const r = await fetch('/api/restore/file', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ backup_id: backupId, file_path: filePath }),
  });
  const d = await r.json();
  if (btn) { btn.disabled = false; btn.innerHTML = '♻'; }
  if (d.error) { toast(d.error, 'error'); return; }
  toast(`Restored → ${d.restored_to}`, 'success');
}

function closeDetail() {
  selectedBackup = null;
  document.getElementById('detailPanel').style.display = 'none';
  document.getElementById('historyLayout').classList.add('no-detail');
  renderHistoryList(_allBackups);
}

async function validateBackup(encodedDir) {
  const dir = decodeURIComponent(encodedDir);
  const r   = await fetch('/api/validate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ backup_dir: dir }),
  });
  const v  = await r.json();
  const id = selectedBackup && selectedBackup.backup_id;
  const el = document.getElementById(`validResult_${id}`);
  if (!el) return;

  const missingHtml  = v.missing_files && v.missing_files.length
    ? `<div style="margin-top:6px;font-size:10px;color:var(--red)">Missing: ${v.missing_files.join(', ')}</div>` : '';
  const corruptHtml  = v.corrupted_files && v.corrupted_files.length
    ? `<div style="margin-top:6px;font-size:10px;color:var(--amber)">Corrupted: ${v.corrupted_files.join(', ')}</div>` : '';

  el.innerHTML = `<div style="background:var(--bg0);border:1px solid var(--border);border-radius:8px;padding:12px;margin-top:10px">
    <div style="font-family:var(--mono);font-size:12px;color:${v.valid ? 'var(--green)' : 'var(--red)'};margin-bottom:6px">${v.valid ? '✅ BACKUP IS VALID' : '❌ BACKUP CORRUPTED'}</div>
    <div style="font-family:var(--mono);font-size:12px;color:${v.manifest_ok ? 'var(--green)' : 'var(--amber)'}">${v.manifest_ok ? '✅ MANIFEST OK' : '⚠ MANIFEST ISSUE'}</div>
    ${missingHtml}${corruptHtml}
    ${v.error ? `<div style="color:var(--red);font-size:11px;margin-top:6px">Error: ${v.error}</div>` : ''}
  </div>`;
  toast(v.valid ? 'Integrity confirmed ✓' : 'Validation failed!', v.valid ? 'success' : 'error');
}


// ── Settings ──────────────────────────────────────────────────────────────────

async function loadSettings() {
  let r;
  try {
    r = await fetch('/api/settings');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    settings = await r.json();
  } catch (err) {
    toast('Failed to load settings: ' + err.message, 'error');
    return;
  }
  const at = document.getElementById('autoToggle');
  at.className = 'toggle' + (settings.auto_backup ? ' on' : '');
  document.getElementById('autoToggleLabel').textContent = settings.auto_backup ? 'ENABLED' : 'DISABLED';
  document.getElementById('autoToggleLabel').style.color = settings.auto_backup ? 'var(--green)' : 'var(--dim)';
  document.getElementById('destInput').value = settings.destination;
  const webhookEl = document.getElementById('webhookInput');
  if (webhookEl) webhookEl.value = settings.webhook_url || '';
  const webhookSuccessEl = document.getElementById('webhookOnSuccess');
  if (webhookSuccessEl) webhookSuccessEl.checked = !!settings.webhook_on_success;
  document.querySelectorAll('#intervalOptions .option-btn').forEach(b => {
    b.className = 'option-btn' + (parseInt(b.dataset.val) === settings.interval_min ? ' active-amber' : '');
  });

  // Highlight retention & storage buttons to match saved config
  document.querySelectorAll('[onclick*="setOption(\'retention\'"]').forEach(b => {
    const val = parseInt(b.getAttribute('onclick').match(/setOption\('retention',(\d+)/)[1]);
    b.className = 'option-btn' + (val === settings.retention_days ? ' active' : '');
  });
  document.querySelectorAll('[onclick*="setOption(\'storage\'"]').forEach(b => {
    const val = b.getAttribute('onclick').match(/setOption\('storage','(\w+)'/)[1];
    b.className = 'option-btn' + (val === settings.storage_type ? ' active' : '');
  });
  const audioBtn = document.getElementById('audioToggleBtn');
  if (audioBtn) audioBtn.textContent = _audioEnabled ? '🔔 SOUND ON' : '🔕 SOUND OFF';

  // ── Compression toggle ────────────────────────────────────────────────────
  const compToggle = document.getElementById('compressionToggle');
  if (compToggle) {
    const on = !!settings.compression_enabled;
    compToggle.className = 'toggle' + (on ? ' on' : '');
    document.getElementById('compressionLabel').textContent = on ? 'ENABLED' : 'DISABLED';
    document.getElementById('compressionLabel').style.color = on ? 'var(--green)' : 'var(--dim)';
  }

  // ── Auto-retry toggle ─────────────────────────────────────────────────────
  const arToggle = document.getElementById('autoRetryToggle');
  if (arToggle) {
    const on = !!settings.auto_retry;
    arToggle.className = 'toggle' + (on ? ' on' : '');
    document.getElementById('autoRetryLabel').textContent  = on ? 'ENABLED' : 'DISABLED';
    document.getElementById('autoRetryLabel').style.color  = on ? 'var(--green)' : 'var(--dim)';
    document.querySelectorAll('#retryDelayOptions .option-btn').forEach(b => {
      b.className = 'option-btn' + (parseInt(b.dataset.val) === settings.retry_delay_min ? ' active-amber' : '');
    });
  }

  // ── Bandwidth throttle ────────────────────────────────────────────────────
  const throttleEl = document.getElementById('throttleInput');
  if (throttleEl) throttleEl.value = settings.max_backup_mbps || 0;

  const ec = settings.email_config || {};
  const emailOn = !!ec.enabled;
  const emailToggle = document.getElementById('emailToggle');
  if (emailToggle) {
    emailToggle.className = 'toggle' + (emailOn ? ' on' : '');
    document.getElementById('emailLabel').textContent = emailOn ? 'ENABLED' : 'DISABLED';
    document.getElementById('emailLabel').style.color = emailOn ? 'var(--green)' : 'var(--dim)';
    document.getElementById('emailFields').style.display = emailOn ? 'flex' : 'none';
    document.getElementById('emailSmtpHost').value  = ec.smtp_host  || '';
    document.getElementById('emailSmtpPort').value  = ec.smtp_port  || 587;
    document.getElementById('emailUsername').value  = ec.username   || '';
    document.getElementById('emailPassword').value  = ec.password   || '';
    document.getElementById('emailFrom').value      = ec.from_addr  || '';
    document.getElementById('emailTo').value        = ec.to_addr    || '';
  }

  try {
    const si = await fetch('/api/system/info');
    const info = await si.json();
    const sysEl = document.getElementById('systemInfoContent');
    if (sysEl) {
      sysEl.innerHTML = `
        <div class="meta-row"><span class="meta-key">VERSION</span><span class="meta-value">v${info.version}</span></div>
        <div class="meta-row"><span class="meta-key">UPTIME</span><span class="meta-value">${info.uptime_human}</span></div>
        <div class="meta-row"><span class="meta-key">PYTHON</span><span class="meta-value">${info.python}</span></div>
        <div class="meta-row"><span class="meta-key">PLATFORM</span><span class="meta-value">${info.platform}</span></div>
        ${info.dest_free ? `<div class="meta-row"><span class="meta-key">DISK FREE</span><span class="meta-value" style="color:${info.dest_used_pct > 90 ? 'var(--red)' : info.dest_used_pct > 75 ? 'var(--amber)' : 'var(--green)'}">${info.dest_free} (${info.dest_used_pct}% used)</span></div>` : ''}`;
    }
  } catch (_) {}

  // Show current destination disk usage in settings
  try {
    const dr = await fetch('/api/dashboard');
    const dd = await dr.json();
    const destCard = document.getElementById('destInput');
    const existing = document.getElementById('settingsDestSize');
    if (destCard && !existing && dd.dest_size_human) {
      const hint = document.createElement('div');
      hint.id = 'settingsDestSize';
      hint.className = 'form-hint';
      hint.style.marginTop = '6px';
      hint.innerHTML = `💾 Current usage: <strong style="color:var(--amber)">${dd.dest_size_human}</strong> across ${dd.total_backups} backup${dd.total_backups !== 1 ? 's' : ''}`;
      destCard.parentElement.appendChild(hint);
    }
  } catch (_) {}
}

async function importConfig(file) {
  if (!file) return;
  if (!confirm('Import config? This will overwrite all current settings and watches.')) return;
  const form = new FormData();
  form.append('file', file);
  const r = await fetch('/api/settings/import', { method: 'POST', body: form });
  const d = await r.json();
  if (d.error) { toast('Import failed: ' + d.error, 'error'); return; }
  toast('Config imported — reloading…', 'success');
  setTimeout(() => location.reload(), 1200);
}

function toggleCompression() {
  settings.compression_enabled = !settings.compression_enabled;
  const t = document.getElementById('compressionToggle');
  t.className = 'toggle' + (settings.compression_enabled ? ' on' : '');
  document.getElementById('compressionLabel').textContent = settings.compression_enabled ? 'ENABLED' : 'DISABLED';
  document.getElementById('compressionLabel').style.color = settings.compression_enabled ? 'var(--green)' : 'var(--dim)';
}

function toggleSettingAuto() {
  settings.auto_backup = !settings.auto_backup;
  const at = document.getElementById('autoToggle');
  at.className = 'toggle' + (settings.auto_backup ? ' on' : '');
  document.getElementById('autoToggleLabel').textContent = settings.auto_backup ? 'ENABLED' : 'DISABLED';
  document.getElementById('autoToggleLabel').style.color = settings.auto_backup ? 'var(--green)' : 'var(--dim)';
}

function setOption(key, val, btn) {
  if (key === 'interval')  settings.interval_min   = val;
  if (key === 'storage')   settings.storage_type   = val;
  if (key === 'retention') settings.retention_days = val;
  btn.parentElement.querySelectorAll('.option-btn').forEach(b => b.classList.remove('active', 'active-amber'));
  btn.classList.add(key === 'interval' ? 'active-amber' : 'active');
}

async function testWebhook() {
  const url = (document.getElementById('webhookInput') || {}).value?.trim();
  if (!url) { toast('Enter a webhook URL first', 'warn'); return; }
  toast('Sending test payload…', 'info');
  const r = await fetch('/api/settings/test-webhook', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ url }),
  });
  const d = await r.json();
  if (d.error) toast('Test failed: ' + d.error, 'error');
  else         toast(`Webhook OK — server replied ${d.http_status}`, 'success');
}

async function testEmail() {
  const ec = {
    enabled:   document.getElementById('emailToggle').classList.contains('on'),
    smtp_host: document.getElementById('emailSmtpHost').value.trim(),
    smtp_port: parseInt(document.getElementById('emailSmtpPort').value) || 587,
    username:  document.getElementById('emailUsername').value.trim(),
    password:  document.getElementById('emailPassword').value,
    from_addr: document.getElementById('emailFrom').value.trim(),
    to_addr:   document.getElementById('emailTo').value.trim(),
  };
  if (!ec.smtp_host || !ec.to_addr) { toast('Fill in SMTP host and To address first', 'warn'); return; }
  toast('Sending test email…', 'info');
  const r = await fetch('/api/settings/test-email', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email_config: ec }),
  });
  const d = await r.json();
  if (d.error) toast('Test failed: ' + d.error, 'error');
  else         toast('Test email sent successfully ✓', 'success');
}

async function loadSystemResources() {
  const el  = document.getElementById('systemResourcesContent');
  const btn = document.getElementById('resourcesBtn');
  if (btn) { btn.disabled = true; btn.innerHTML = '<span class="spinner"></span>'; }
  try {
    const r = await fetch('/api/system/resources');
    const d = await r.json();
    if (d.error) { el.innerHTML = `<div style="font-family:var(--mono);font-size:10px;color:var(--dim)">${d.error}</div>`; return; }
    el.innerHTML = `
      <div class="meta-row"><span class="meta-key">CPU</span><span class="meta-value" style="color:${d.cpu_percent>80?'var(--red)':d.cpu_percent>50?'var(--amber)':'var(--green)'}">${d.cpu_percent}%</span></div>
      <div class="meta-row"><span class="meta-key">MEMORY</span><span class="meta-value" style="color:${d.memory_percent>85?'var(--red)':'var(--text)'}">${d.memory_percent}% (${d.memory_gb} GB used)</span></div>
      <div class="meta-row"><span class="meta-key">DEST DISK FREE</span><span class="meta-value" style="color:${d.disk_percent>90?'var(--red)':d.disk_percent>75?'var(--amber)':'var(--green)'}">${d.disk_free_gb} GB (${Math.round(d.disk_percent)}% used)</span></div>`;
  } catch (e) {
    el.innerHTML = `<div style="font-family:var(--mono);font-size:10px;color:var(--red)">Failed: ${e.message}</div>`;
  } finally {
    if (btn) { btn.disabled = false; btn.innerHTML = '📊 RESOURCES'; }
  }
}

async function saveSettings() {
  const destVal = document.getElementById('destInput').value.trim() || './backups';
  settings.destination = destVal;
  const webhookEl = document.getElementById('webhookInput');
  if (webhookEl) settings.webhook_url = webhookEl.value.trim();
  const webhookSuccessEl = document.getElementById('webhookOnSuccess');
  if (webhookSuccessEl) settings.webhook_on_success = webhookSuccessEl.checked;

  // ── Bandwidth throttle ────────────────────────────────────────────────────
  const throttleEl = document.getElementById('throttleInput');
  if (throttleEl) settings.max_backup_mbps = Math.max(0, parseFloat(throttleEl.value) || 0);

  const emailToggleEl = document.getElementById('emailToggle');
  if (emailToggleEl) {
    settings.email_config = {
      enabled:   emailToggleEl.classList.contains('on'),
      smtp_host: document.getElementById('emailSmtpHost').value.trim(),
      smtp_port: parseInt(document.getElementById('emailSmtpPort').value) || 587,
      username:  document.getElementById('emailUsername').value.trim(),
      password:  document.getElementById('emailPassword').value,
      from_addr: document.getElementById('emailFrom').value.trim(),
      to_addr:   document.getElementById('emailTo').value.trim(),
    };
  }

  const saveBtn = document.querySelector('[onclick="saveSettings()"]');
  if (saveBtn) { saveBtn.disabled = true; saveBtn.innerHTML = '<span class="spinner"></span> SAVING...'; }
  const vr = await fetch('/api/settings/validate-dest', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path: settings.destination }),
  });
  const vd = await vr.json();
  if (!vd.ok) {
    if (saveBtn) { saveBtn.disabled = false; saveBtn.innerHTML = '💾 SAVE'; }
    toast(`Destination error: ${vd.error}`, 'error');
    return;
  }
  await fetch('/api/settings', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(settings),
  });
  if (saveBtn) { saveBtn.disabled = false; saveBtn.innerHTML = '💾 SAVE'; }
  toast('Settings saved!');
  loadDashboard();
}

async function toggleAuto() {
  const r = await fetch('/api/settings');
  settings = await r.json();
  settings.auto_backup = !settings.auto_backup;
  await fetch('/api/settings', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(settings),
  });
  loadDashboard();
}

function toggleAutoRetry() {
  settings.auto_retry = !settings.auto_retry;
  const t = document.getElementById('autoRetryToggle');
  t.className = 'toggle' + (settings.auto_retry ? ' on' : '');
  document.getElementById('autoRetryLabel').textContent = settings.auto_retry ? 'ENABLED' : 'DISABLED';
  document.getElementById('autoRetryLabel').style.color = settings.auto_retry ? 'var(--green)' : 'var(--dim)';
}

function setRetryDelay(val, btn) {
  settings.retry_delay_min = val;
  btn.parentElement.querySelectorAll('.option-btn').forEach(b => b.className = 'option-btn');
  btn.className = 'option-btn active-amber';
}

async function clearHistory() {
  if (!confirm('Delete ALL backup records and reset watch snapshots?\n\nThis cannot be undone.')) return;
  const btn = document.querySelector('[onclick="clearHistory()"]');
  if (btn) { btn.disabled = true; btn.innerHTML = '<span class="spinner"></span> CLEARING...'; }
  closeDetail();
  const r = await fetch('/api/history/clear', { method: 'POST' });
  const d = await r.json();
  if (btn) { btn.disabled = false; btn.innerHTML = '🗑 CLEAR ALL BACKUP HISTORY'; }
  if (d.ok) {
    toast(`Cleared ${d.deleted} backup${d.deleted !== 1 ? 's' : ''}${d.errors.length ? ' (with ' + d.errors.length + ' errors)' : ''}`,
      d.errors.length ? 'error' : 'info');
    _historyFilterWatch = _historyFilterStatus = _historyFilterQ = _historyFilterFrom = _historyFilterTo = '';
    _allBackups = []; selectedBackup = null; _historySelected.clear();
    Object.keys(_backupCache).forEach(k => delete _backupCache[k]);
    loadDashboard();
    if (currentPage === 'watches') loadWatches();
    if (currentPage === 'history') loadHistory();
  } else {
    toast('Clear failed', 'error');
  }
}


// ── Log Viewer ────────────────────────────────────────────────────────────────

function openLogsModal() {
  document.getElementById('logsModal').classList.add('open');
  loadLogs();
}
function closeLogsModal() {
  document.getElementById('logsModal').classList.remove('open');
}

async function loadLogs() {
  const el = document.getElementById('logsContent');
  const n  = (document.getElementById('logsLineCount') || {}).value || 200;
  el.innerHTML = '<span style="color:var(--dim)">Loading…</span>';
  try {
    const r = await fetch(`/api/logs?lines=${n}`);
    const d = await r.json();
    if (d.error) { el.textContent = 'Error: ' + d.error; return; }
    if (!d.lines.length) { el.innerHTML = '<span style="color:var(--dim)">No log entries yet.</span>'; return; }
    el.innerHTML = d.lines.map(line => {
      const s = escapeHtml(line);
      if (/\[ERROR\]|❌|FAILED|failed|Error/.test(line))
        return `<div style="color:var(--red)">${s}</div>`;
      if (/\[WARNING\]|⚠|Warning/.test(line))
        return `<div style="color:var(--amber)">${s}</div>`;
      if (/✅|✓|complete|successful|Backup started/.test(line))
        return `<div style="color:var(--green)">${s}</div>`;
      return `<div style="color:var(--dim)">${s}</div>`;
    }).join('');
    el.scrollTop = el.scrollHeight;
  } catch (e) {
    el.textContent = 'Failed to load logs: ' + e.message;
  }
}

// FIX #6: Download logs as a .log file
async function downloadLogs() {
  const n = (document.getElementById('logsLineCount') || {}).value || 200;
  const r = await fetch(`/api/logs?lines=${n}`);
  const d = await r.json();
  if (d.error) { toast(d.error, 'error'); return; }
  const blob = new Blob([d.lines.join('\n')], { type: 'text/plain' });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  a.href     = url;
  a.download = `backupsys_${new Date().toISOString().slice(0,10)}.log`;
  document.body.appendChild(a); a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
  toast('Log downloaded', 'success');
}


// ── Keyboard Shortcut Help Modal ──────────────────────────────────────────────

function openShortcutsModal()  { document.getElementById('shortcutsModal').classList.add('open'); }
function closeShortcutsModal() { document.getElementById('shortcutsModal').classList.remove('open'); }


// ── Init ──────────────────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
  loadDashboard();
  window.addEventListener('resize', () => { if (currentPage === 'dashboard') _renderActivityChart(); });

  _dashPollTimer = setInterval(() => {
    const anyModalOpen = document.querySelector('.modal-overlay.open');
    if (currentPage === 'dashboard' && _dashErrorCount < _DASH_MAX_ERRORS && !document.hidden && !anyModalOpen) {
      loadDashboard();
    }
  }, 5000);
});

document.getElementById('addModal').addEventListener('click',       function(e) { if (e.target === this) closeAddModal(); });
document.getElementById('editWatchModal').addEventListener('click',  function(e) { if (e.target === this) closeEditWatchModal(); });
document.getElementById('duplicateWatchModal').addEventListener('click', function(e) { if (e.target === this) closeDuplicateWatchModal(); });
document.getElementById('watchStatsModal').addEventListener('click', function(e) { if (e.target === this) closeWatchStats(); });
document.getElementById('shortcutsModal').addEventListener('click',  function(e) { if (e.target === this) closeShortcutsModal(); });
document.getElementById('logsModal').addEventListener('click',       function(e) { if (e.target === this) closeLogsModal(); });

document.addEventListener('keydown', e => {
  if (e.key === '?' && !e.ctrlKey && !e.metaKey && document.activeElement.tagName !== 'INPUT' && document.activeElement.tagName !== 'TEXTAREA') {
    e.preventDefault();
    openShortcutsModal();
    return;
  }
  if ((e.ctrlKey || e.metaKey) && e.shiftKey && e.key === 'S' && currentPage === 'editor') {
    e.preventDefault();
    saveFile().then(() => { if (editorCurrentWatchId) runBackup(editorCurrentWatchId); });
    return;
  }
  if ((e.ctrlKey || e.metaKey) && e.shiftKey && e.key === 'B') {
    e.preventDefault();
    backupAll();
    return;
  }
  // FIX #3: Ctrl+B to backup current watch from editor without saving
  if ((e.ctrlKey || e.metaKey) && e.key === 'b' && currentPage === 'editor') {
    e.preventDefault();
    if (editorCurrentWatchId) runBackup(editorCurrentWatchId);
    return;
  }
  if ((e.ctrlKey || e.metaKey) && e.key === 's' && currentPage === 'editor') { e.preventDefault(); saveFile(); }
  if ((e.ctrlKey || e.metaKey) && e.key === 'f' && currentPage === 'editor') { e.preventDefault(); openFindBar(); }
  if (e.key === 'Escape') {
    closeAddModal(); closeEditWatchModal(); closeWatchStats(); closeDuplicateWatchModal();
    closeShortcutsModal(); closeLogsModal(); hideAllContextMenus();
    const dryRun = document.getElementById('dryRunModal');
    if (dryRun) dryRun.classList.remove('open');
    if (currentPage === 'editor') closeFindBar();
    if (currentPage === 'history' && selectedBackup) closeDetail();
  }
  if (e.key === 'Enter' && document.getElementById('findBar').classList.contains('open')) {
    e.shiftKey ? findPrev() : findNext();
  }
  // ── R to refresh dashboard ────────────────────────────────────────────────
  if (e.key === 'r' && !e.ctrlKey && !e.metaKey
      && document.activeElement.tagName !== 'INPUT'
      && document.activeElement.tagName !== 'TEXTAREA') {
    e.preventDefault();
    if (currentPage === 'dashboard') {
      _dashErrorCount = 0;
      loadDashboard();
      toast('Dashboard refreshed', 'info');
    } else if (currentPage === 'history') {
      loadHistory(1);
      toast('History refreshed', 'info');
    } else if (currentPage === 'watches') {
      loadWatches();
      toast('Watches refreshed', 'info');
    }
  }
});

document.addEventListener('DOMContentLoaded', () => {
  const ta = document.getElementById('editorTextarea');
  if (ta) {
    ta.addEventListener('scroll', () => {
      const nums = document.getElementById('lineNumbers');
      if (nums) nums.scrollTop = ta.scrollTop;
    });
  }

  // Drag-and-drop upload onto file browser
  const fl = document.getElementById('fileList');
  if (fl) {
    fl.addEventListener('dragover',  e => { e.preventDefault(); fl.style.outline = '2px dashed var(--blue)'; });
    fl.addEventListener('dragleave', () => { fl.style.outline = ''; });
    fl.addEventListener('drop', e => {
      e.preventDefault();
      fl.style.outline = '';
      if (e.dataTransfer.files.length) uploadFiles(e.dataTransfer.files);
    });
  }
});