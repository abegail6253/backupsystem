import sys
sys.path.insert(0, ".")

from transport_utils import upload_to_https

result = upload_to_https(
    r"D:\backups",
    {
        "url":        "http://127.0.0.1:8787/upload",
        "token":      "test-token-123",
        "verify_ssl": False,
    }
)
print(result)