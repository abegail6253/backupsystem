import sys
sys.path.insert(0, ".")

from transport_utils import upload_to_smb

result = upload_to_smb(
    r"D:\backups",
    {
        "server":      "DESKTOP-0EDUBAP",
        "share":       "smb_test",
        "username":    "user",
        "password":    "test1234",
        "remote_path": "",
    }
)
print(result)