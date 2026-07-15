# Backup System — Setup Guide

**Version:** 1.1.10 · For Windows 10 and Windows 11

This guide takes you from nothing to a working Backup System, step by step.

---

## Do we need to install Python?

**No.** This is the most common question, so here is the short answer.

The program is delivered as a ready-to-run application. You copy one folder to the PC and double-click an icon. Python is **not** required, and nothing else needs to be installed first.

Python is only needed by the person who *builds* the application from the original code. That is a one-time job for the developer, and it is covered at the end of this guide under [For the developer](#for-the-developer). Everyone else can ignore it completely.

---

## What you need

| | |
|---|---|
| **A Windows PC** | Windows 10 or Windows 11. |
| **The BackupSystem folder** | Provided to you as a folder (or a ZIP file you unzip). It contains `BackupSystem.exe` and the files it needs. |
| **Somewhere to save backups** | A folder on the PC, a USB or external drive, a shared network folder, or a Google Drive account. |

That's all. No other software is required.

---

## Step 1 — Copy the program to the PC

1. Copy the whole **`BackupSystem`** folder onto the PC — for example to `C:\BackupSystem` or onto the Desktop.
2. If you received a ZIP file, right-click it and choose **Extract All** first.

> **Important:** copy the **entire folder**, not just the `BackupSystem.exe` file on its own. The program needs the other files that sit beside it. If you copy only the icon, it will not start.

## Step 2 — Start the program

Open the folder and double-click **`BackupSystem.exe`**.

The program starts and places a small icon in the **system tray** — the area at the bottom-right of the screen, next to the clock. The program runs quietly in the background from there.

> If you can't see the icon, click the small arrow (**^**) next to the clock to show hidden icons.

**Right-click the tray icon** at any time to open the program.

## Step 3 — Choose your language

The program starts in **Japanese**. To switch to English:

1. Click **🔧 Admin** at the top of the window.
2. Go to the **General** tab.
3. Under **Language**, choose **English**.
4. **Close and restart the program.** The language changes after a restart.

## Step 4 — Set the admin password

The first time you open the settings, the program asks you to create an **admin password**.

This password protects the settings, so that ordinary staff cannot change or switch off the backups by accident. You will need it every time you open the settings screen.

> Write this password down and keep it somewhere safe.

## Step 5 — Choose a folder to back up

The program calls each folder it looks after a **watch**.

1. Click **🔧 Admin**, then open the **Watches** tab.
2. Click **➕ Add Watch**.
3. Fill in three things:
   - **Name** — anything you like, so you can recognise it later (for example "Sales Documents").
   - **Folder to watch** — the folder you want protected. Click **Browse** to pick it.
   - **Where to save the backup** — the destination. A folder on another drive is the simplest choice.
4. Click **Add Watch**.

The folder now appears on the main screen as a card.

> **Tip:** save the backup on a *different* drive from the original folder. If the backup sits on the same disk and that disk fails, you lose both copies at once.

## Step 6 — Run your first backup

On the main screen, find the card for your folder and click **Backup Now**.

A progress bar shows how far along it is. When it finishes, the button returns to normal.

> The first backup copies everything, so it can take a while. After that the program only copies what has actually **changed**, which is much faster.

## Step 7 — Turn on automatic backups

So that nobody has to remember to do it:

1. Click **🔧 Admin**, then the **General** tab.
2. Find **Schedule & Limits**.
3. Switch on automatic backups, and choose either:
   - **every so many minutes**, or
   - **at set times** (for example every day at 02:00).

The program will now back up on its own, in the background.

**Done.** The program is set up and protecting your files.

---

## Optional settings worth knowing

You do not need any of these to get started — turn them on later if they're useful.

| Setting | What it does | Where |
|---|---|---|
| **Start with Windows** | The program starts automatically whenever the PC is switched on, so it's never left off by accident. | Admin → General → Startup |
| **Auto-shutdown** | Switches the PC off by itself once all backups have finished. Useful for overnight backups. A 60-second countdown appears first, so you can stop it if you're still working. | Admin → General → Auto-Shutdown |
| **Email / phone alerts** | Tells you if a backup fails, so a problem doesn't go unnoticed for weeks. | Admin → Notifications |
| **Backup window** | Only allow automatic backups during set hours (for example 22:00–06:00), so they never slow anyone down during office hours. | Admin → General |
| **Delete old backups** | Automatically removes backups older than a set number of days, so the disk doesn't fill up. | Admin → General |

---

## If something goes wrong

| Problem | What to do |
|---|---|
| Nothing happens when I double-click the icon | You probably copied only `BackupSystem.exe` instead of the whole folder. Copy the entire folder again. |
| I can't find the program on screen | It runs in the background. Look for its icon by the clock, bottom-right. Click the arrow (**^**) to show hidden icons, then right-click it. |
| I changed the language but it's still Japanese | The language only changes after you close and restart the program. |
| Files that are open don't get backed up | Right-click `BackupSystem.exe` and choose **Run as administrator**. This lets the program copy files that are currently in use. |
| The change history says "Unknown" instead of a person's name | The PC login details for that shared folder haven't been entered yet. Open **Admin → Watches → Edit → PC Credentials**. This needs administrator rights. |
| I forgot the admin password | Click **Forgot password?** on the login screen. |
| A backup failed and I don't know why | Click **📜 Logs** on the main screen to see what happened. To send the details to whoever supports the program, use **More ▾ → 🛟 Export Diagnostics** on the folder's card — it packages everything into one file, with passwords removed. |

---

## For the developer

*This section is only for the person who builds the application. If you are just using the program, you can stop reading here.*

Building `BackupSystem.exe` from the source code requires **Python 3.11 or newer** (tick **"Add python.exe to PATH"** during the Python install).

Install the dependencies and build:

```bash
python setup_wizard.py          # installs everything needed
python build_exe.py             # produces the distributable app
```

The result appears in `dist/BackupSystem/`. That folder is what you hand to users in Step 1 — it runs on any Windows PC with no Python installed.

To run directly from source instead of building:

```bash
python desktop_app.py
```

The libraries the program depends on are listed in `requirements_desktop.txt`; `setup_wizard.py` installs them all automatically. Administrator rights are needed for two optional features: backing up files that are currently open, and recording who changed a file on a shared folder.

Full technical documentation is in the project `README.md`.
