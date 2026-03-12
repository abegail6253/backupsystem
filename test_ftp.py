import sys
sys.path.insert(0, ".")

from transport_utils import upload_to_ftp

result = upload_to_ftp(
    r"D:\backups",
    {
        "host":        "127.0.0.1",
        "port":        21,
        "username":    "ftpuser",
        "password":    "ftppass",
        "remote_path": "/backups",
        "use_tls":     False,
    }
)
print(result)