"""
clear_admin.py — Clears the saved admin password and recovery email.
Run this ONCE to reset, then restart desktop_app.py to set a new password + email.

    python clear_admin.py
"""
from PyQt5.QtCore import QSettings

SETTINGS_ORG = "BackupSystem"
SETTINGS_APP = "BackupSystem"

s = QSettings(SETTINGS_ORG, SETTINGS_APP)
s.remove("admin_password_hash")
s.remove("admin_email")
s.remove("launched_before")
s.sync()
print("✅ Admin password and email cleared.")
print("   Now restart desktop_app.py — it will ask you to set a new password + recovery email.")
