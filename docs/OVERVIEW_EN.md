# Backup System — What the Program Does

**Version:** 1.1.10 · Windows desktop application

The program watches the folders you care about and copies them somewhere safe — automatically, in the background. If the folder is shared, it also keeps a record of **who** added, changed, or deleted each file.

It sits quietly in the system tray and looks after itself. Once set up, nobody has to remember to do anything.

---

## 1. Backing up

| Feature | What it means for you |
|---|---|
| **Watch any number of folders** | Each folder you protect is called a "watch". You can add as many as you like, and each one has its own destination and its own schedule. |
| **Notices changes immediately** | The program sees a file change the moment it happens — it isn't waiting for a scheduled scan. |
| **Only copies what changed** | The first backup copies everything. After that, only files that were actually modified get copied. This makes later backups much faster and saves a lot of disk space. |
| **Preview before backing up** | Shows you exactly which files *would* be copied, without copying anything. Useful when you want to check before committing. |
| **Backs up files that are open** | Files being used right now — Outlook mail files, databases, files someone left open — are normally impossible to copy. The program can back them up anyway. |
| **Password-protect the backups (encryption)** | Backups can be scrambled so that they're unreadable to anyone without the key. If a backup drive is lost or stolen, the files on it are useless to whoever finds it. |
| **Squeeze backups smaller (compression)** | Optionally makes backups take up less space, at the cost of taking a bit longer to run. |

## 2. Running automatically

| Feature | What it means for you |
|---|---|
| **Back up on a timer** | Every so many minutes, automatically. |
| **Back up at set times** | For example, every weekday at 02:00. You can choose which days of the week. |
| **Only run outside office hours** | Restrict automatic backups to a time range (say 22:00–06:00), so they never slow anyone's work down. |
| **Auto-shutdown when finished** | Switches the PC off by itself once **all** backups are done — ideal for overnight runs. A 60-second countdown appears first, so anyone still at the desk can cancel it. |
| **Back up when a USB drive is plugged in** | Plug in your backup drive and the backup starts on its own. Nobody has to click anything or remember which drive it was. |
| **Stay out of the way** | The program can hold off while the PC is busy, so backups never make someone's work feel slow. It can also pause when the PC is on a mobile/metered internet connection, to avoid burning through data. |
| **Start with Windows** | Starts automatically when the PC is switched on, so it can't be left off by accident. |

## 3. Where backups can be saved

One folder can be backed up to **several places at once** — for example onto a local drive *and* to Google Drive, so you're covered even if the office burns down.

| Destination | Notes |
|---|---|
| **A folder on this PC, or a USB / external drive** | The simplest option. |
| **A shared network folder or NAS** | The usual choice in an office. |
| **Google Drive** | You sign in through your browser, the normal way. |
| **A company file server (SFTP / FTP)** | For offices where IT provides a server. |

Every remote destination has a **Test Connection** button, so you can confirm it actually works before trusting your files to it.

## 4. Knowing who changed what

This is the feature that sets the program apart, and it matters most on shared folders.

| Feature | What it means for you |
|---|---|
| **A record of every change** | Every file that was **added, edited, deleted, or renamed** in a watched folder is logged, as it happens. |
| **The name of the person who did it** | For shared folders, the program identifies which person or PC made each change. When a file goes missing or gets overwritten, you can see who did it and when. |
| **Works with colleagues' shared folders** | Including folders hosted on other people's PCs. |
| **Says "Unknown" rather than guessing** | If it cannot be certain who made a change, it says so honestly instead of naming the wrong person. You can trust what the record tells you. |

## 5. Getting files back

| Feature | What it means for you |
|---|---|
| **Restore from any past backup** | Pick a date, pick where to put the files, and get them back. |
| **Put files back where they came from** | Restores straight to the original folder in one step. |
| **Rewind a folder to how it was on a given day** | Reconstructs the folder exactly as it looked at a chosen point in time. |
| **Find a file across all backups** | Search for a filename across every folder's backups at once, and see which backup contains it. Useful when you know the name but not where or when. |

## 6. Keeping an eye on things

| Feature | What it means for you |
|---|---|
| **Backup history** | Whether each backup worked or failed, when it ran, and how much it copied. |
| **One dashboard for everything** | The status of every protected folder on a single screen. |
| **Health checks** | The program regularly verifies that the backup files are still readable and undamaged — so you don't discover a corrupted backup on the day you actually need it. |
| **Logs** | A detailed record of what the program did, for when something needs investigating. |
| **One-click support file** | Bundles a folder's settings, history, and logs into a single file to send for support. Passwords are automatically removed first. |
| **Don't let backups fill the disk** | Automatically delete backups older than a set age, keep only the most recent few, or set a size limit. |
| **Don't hog the network** | Limit how much bandwidth backups use — for example, run slowly during the day and at full speed at night. |

## 7. Alerts

The program can tell you when a backup fails, so a problem doesn't go unnoticed for weeks. You can be notified by:

- **Email**
- **A message to your phone**
- **Telegram**
- **Chat apps such as Slack or Discord**

By default it only contacts you when something **fails**. You can also ask to be told on success.

## 8. Everyday use and safety

| Feature | What it means for you |
|---|---|
| **Runs in the background** | Lives in the system tray by the clock. Right-click it to open. |
| **Admin password** | Protects the settings, so staff can't change or switch off the backups by accident. |
| **Passwords stored safely** | Any passwords you enter are kept in the Windows password vault, not in a plain file that anyone could read. |
| **Japanese and English** | Switch language in the settings. |
| **Light and dark appearance** | Whichever is easier on the eye. |
| **Copy settings to another PC** | Export the setup and import it elsewhere, instead of typing it all again. |
| **Run from a USB stick** | The whole program, including its settings, can live on a USB drive. |
