# Screenshot shot list — USER_MANUAL_JA.md

20 screenshots are needed. Save each one into this folder (`docs/images/`) using the **exact filename** in the table — the manual already links to these names, so once the files exist the images appear automatically. No editing of the manual is required.

## Before you start

1. **Set the app language to Japanese** — the manual is Japanese, so the screenshots must show the Japanese UI. Check **🔧 Admin → 全般 → 言語 → 日本語**, then restart the app.
2. **Use demo data, not real data.** Create a throwaway watch (e.g. source `C:\Demo\営業部` → destination `C:\Demo\Backup`) so no real folder names, server names, customer names, or usernames appear in the images.
3. **Format:** PNG. Capture the window only (`Alt` + `PrtScn`), not the whole desktop, except where noted.
4. **Redact** anything sensitive that still slips in: IP addresses, hostnames, real usernames, passwords.

## The shots

| # | Filename | What to capture | How to get there |
|---|---|---|---|
| 1 | `01-main-window.png` | Main window with at least one watch card visible. Top bar must be included. | Open the app. |
| 2 | `02-set-admin-password.png` | The "管理者パスワードを設定" dialog. | Appears on first launch. If you already set one, temporarily rename `config.json` to see it again — **back it up first**. |
| 3 | `03-language-setting.png` | The 言語 (Language) group box. | 🔧 Admin → 全般 tab → scroll to 言語. |
| 4 | `04-admin-watches-tab.png` | The Watches tab with the watch table and the **➕ ウォッチを追加** button visible. | 🔧 Admin → ウォッチ tab. |
| 5 | `05-add-watch-dialog.png` | The "ウォッチを追加" dialog, **collapsed** (basic fields only: name, source, destination). | 🔧 Admin → ウォッチ → ➕ ウォッチを追加. |
| 6 | `06-add-watch-options.png` | The same dialog with **▶ スケジュールとオプション** expanded, showing the advanced fields. | Same dialog, click the ▶ スケジュールとオプション toggle. |
| 7 | `07-edit-watch-dialog.png` | The "ウォッチの編集" dialog. Expand **▶ 詳細設定** so the advanced options are visible. | Main window card → More ▾ → ⚙ ウォッチ設定… |
| 8 | `08-remove-watch-button.png` | The watch table row, with the **🗑 Remove** button clearly visible. Crop tight on the row if easier. | 🔧 Admin → ウォッチ tab. |
| 9 | `09-delete-watch-confirm.png` | The "Delete Watch" confirmation message box. | 🔧 Admin → ウォッチ → 🗑 Remove on a **demo** watch. Click **Cancel** afterwards. |
| 10 | `10-backup-now-progress.png` | A watch card **mid-backup** — progress bar and the details row (%, GB left, files left, speed) visible. | Click **Backup Now** on a demo watch with enough files that the bar is visible for a few seconds. |
| 11 | `11-more-menu.png` | The **More ▾** dropdown, open, with all menu items visible. | Main window card → click More ▾. |
| 12 | `12-admin-schedule.png` | The **スケジュールと制限** group box. | 🔧 Admin → 全般 tab. |
| 13 | `13-auto-shutdown.png` | The **自動シャットダウン** group box with its checkbox and note text. | 🔧 Admin → 全般 tab → scroll down. |
| 14 | `14-change-history.png` | The **変更履歴** tab with several rows showing added / modified / deleted entries **and the who-did-it column populated**. This is the headline feature — make it a good one. | 📋 History → 変更履歴 tab. Generate rows first by creating/editing/deleting a few files in a demo watched folder. |
| 15 | `15-backup-history.png` | The **バックアップ履歴** tab with a few completed backup rows. | 📋 History → バックアップ履歴 tab. |
| 16 | `16-file-search.png` | The **🔍 ファイル検索** tab **with search results showing** (not an empty form). | 📋 History → 🔍 File Search tab → search for a filename you know exists. |
| 17 | `17-logs-window.png` | The log viewer window with log lines visible. | Main window → 📜 Logs. |
| 18 | `18-admin-logs-tab.png` | The Logs tab, showing the 🔄 / 🗑 / 💾 buttons. | 🔧 Admin → ログ tab. |
| 19 | `19-restore-dialog.png` | The restore dialog with the list of restorable backups (dates) visible. | Main window card → More ▾ → ↩ 復元. **Cancel** without restoring. |
| 20 | `20-tray-menu.png` | The tray icon right-click menu, open. Include a bit of the taskbar for context — this is the one shot where a partial-desktop capture is correct. | Right-click the tray icon near the clock. |

## Notes on specific shots

- **#2** — if recreating the first-run state is a hassle, skip it and tell me; I'll reword that section so it doesn't need the image.
- **#10** — if backups finish too fast to capture, point the demo watch at a folder with a few thousand files, or a couple of large files, so the progress bar stays on screen.
- **#14** — the most important screenshot in the manual. If the who-did-it column shows *Unknown* for every row, the audit setup isn't configured on that folder; use a share where it works, otherwise the screenshot demonstrates the opposite of the feature.
- **#9** and **#19** are destructive-looking dialogs — capture them, then **Cancel**. Do not confirm.

Once the files are in this folder, the manual renders complete. Tell me if any shot is impossible to get and I'll rewrite that section around it.
