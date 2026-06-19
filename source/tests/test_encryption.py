"""
tests/test_encryption.py — Unit tests for backup_engine encryption functions.
Tests AES-GCM streaming encryption and key generation.
"""
import sys
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import backup_engine as be


class TestKeyGeneration:
    """Test encryption key generation."""

    def test_generate_encryption_key_returns_44_chars(self):
        """Key should be 44-character URL-safe base64."""
        key = be.generate_encryption_key()
        assert isinstance(key, str)
        assert len(key) == 44
        # Verify it's valid base64 (should decode without error)
        import base64
        decoded = base64.urlsafe_b64decode(key + "==")
        assert len(decoded) == 32

    def test_generate_unique_keys(self):
        """Multiple calls should generate different keys."""
        key1 = be.generate_encryption_key()
        key2 = be.generate_encryption_key()
        assert key1 != key2


class TestKeyValidation:
    """Test encryption key validation."""

    def test_valid_key_accepted(self):
        """A generated key should pass validation."""
        key = be.generate_encryption_key()
        # Should not raise
        raw = be._validate_key(key)
        assert len(raw) == 32

    def test_invalid_key_too_short(self):
        """Short key should raise ValueError."""
        with pytest.raises(ValueError):
            be._validate_key("tooShort")

    def test_invalid_key_bad_base64(self):
        """Invalid base64 should raise ValueError."""
        with pytest.raises(ValueError):
            be._validate_key("!!!invalid!!!invalid!!!invalid!!!")

    def test_invalid_key_wrong_length_decoded(self):
        """Key that decodes to wrong length should raise ValueError."""
        # Create a 44-char base64 string that decodes to wrong length
        # 44 chars of 'A' decodes to 33 bytes, not 32
        invalid_key = "A" * 44
        with pytest.raises(ValueError):
            be._validate_key(invalid_key)


class TestEncryption:
    """Test AES-GCM streaming encryption."""

    def test_roundtrip_encrypt_decrypt(self, tmp_path):
        """Test encrypt then decrypt produces original content."""
        # Create source file with known content
        content = b"Hello, World! This is a test file for encryption."
        src_file = tmp_path / "original.txt"
        src_file.write_bytes(content)

        # Encrypt
        enc_file = tmp_path / "encrypted.bin"
        key = be.generate_encryption_key()
        be._encrypt_file(str(src_file), str(enc_file), key)

        # Verify encrypted file exists
        assert enc_file.exists()
        assert enc_file.stat().st_size > 0

        # Decrypt
        dec_file = tmp_path / "decrypted.txt"
        be._decrypt_file(str(enc_file), str(dec_file), key)

        # Verify content matches
        decrypted = dec_file.read_bytes()
        assert decrypted == content

    def test_roundtrip_large_file(self, tmp_path):
        """Test encryption/decryption with large file (tests chunking)."""
        # Create a 32 MB file to ensure chunking works (chunk size is 16 MB)
        chunk_size = 16 * 1024 * 1024
        large_content = b"x" * (2 * chunk_size + 1000)

        src_file = tmp_path / "large.bin"
        src_file.write_bytes(large_content)

        enc_file = tmp_path / "large_enc.bin"
        key = be.generate_encryption_key()
        be._encrypt_file(str(src_file), str(enc_file), key)

        dec_file = tmp_path / "large_dec.bin"
        be._decrypt_file(str(enc_file), str(dec_file), key)

        decrypted = dec_file.read_bytes()
        assert decrypted == large_content

    def test_roundtrip_empty_file(self, tmp_path):
        """Test encryption/decryption with empty file."""
        src_file = tmp_path / "empty.txt"
        src_file.write_bytes(b"")

        enc_file = tmp_path / "empty_enc.bin"
        key = be.generate_encryption_key()
        be._encrypt_file(str(src_file), str(enc_file), key)

        dec_file = tmp_path / "empty_dec.txt"
        be._decrypt_file(str(enc_file), str(dec_file), key)

        decrypted = dec_file.read_bytes()
        assert decrypted == b""

    def test_encrypted_file_not_readable_as_text(self, tmp_path):
        """Encrypted file should not be readable as plain text."""
        content = b"Secret data"
        src_file = tmp_path / "secret.txt"
        src_file.write_bytes(content)

        enc_file = tmp_path / "secret.enc"
        key = be.generate_encryption_key()
        be._encrypt_file(str(src_file), str(enc_file), key)

        # Encrypted data should not equal original
        encrypted = enc_file.read_bytes()
        assert encrypted != content

    def test_wrong_key_fails_to_decrypt(self, tmp_path):
        """Decryption with wrong key should fail."""
        content = b"Test content"
        src_file = tmp_path / "test.txt"
        src_file.write_bytes(content)

        enc_file = tmp_path / "test.enc"
        key1 = be.generate_encryption_key()
        be._encrypt_file(str(src_file), str(enc_file), key1)

        # Try to decrypt with different key
        key2 = be.generate_encryption_key()
        dec_file = tmp_path / "decrypted.txt"
        with pytest.raises(Exception):  # Should raise during decryption
            be._decrypt_file(str(enc_file), str(dec_file), key2)


class TestFormatDetection:
    """Test format detection in _decrypt_file."""

    def test_aes_gcm_magic_detection(self, tmp_path):
        """Verify AES-GCM format has correct magic bytes."""
        content = b"Magic bytes test"
        src_file = tmp_path / "magic_test.txt"
        src_file.write_bytes(content)

        enc_file = tmp_path / "magic_test.enc"
        key = be.generate_encryption_key()
        be._encrypt_file(str(src_file), str(enc_file), key)

        # Read first 8 bytes and verify magic
        with open(enc_file, "rb") as f:
            magic = f.read(8)
        assert magic == b"BACKENC1", f"Expected b'BACKENC1', got {magic!r}"

    def test_decrypt_auto_detects_format(self, tmp_path):
        """_decrypt_file should auto-detect AES-GCM format and decrypt correctly."""
        content = b"Format detection test"
        src_file = tmp_path / "format_test.txt"
        src_file.write_bytes(content)

        enc_file = tmp_path / "format_test.enc"
        key = be.generate_encryption_key()

        # Encrypt with one key
        be._encrypt_file(str(src_file), str(enc_file), key)

        # Decrypt with same key - should auto-detect format
        dec_file = tmp_path / "format_test_dec.txt"
        be._decrypt_file(str(enc_file), str(dec_file), key)

        # Verify decryption works
        decrypted = dec_file.read_bytes()
        assert decrypted == content


class TestEncryptionEdgeCases:
    """Test edge cases in encryption."""

    def test_binary_content_preserved(self, tmp_path):
        """Binary content with all byte values should be preserved."""
        # Create binary content with all possible byte values
        content = bytes(range(256)) * 4  # All byte values repeated

        src_file = tmp_path / "binary.bin"
        src_file.write_bytes(content)

        enc_file = tmp_path / "binary.enc"
        key = be.generate_encryption_key()
        be._encrypt_file(str(src_file), str(enc_file), key)

        dec_file = tmp_path / "binary_dec.bin"
        be._decrypt_file(str(enc_file), str(dec_file), key)

        decrypted = dec_file.read_bytes()
        assert decrypted == content

    def test_unicode_text_preserved(self, tmp_path):
        """Unicode text should be preserved correctly."""
        content = "Unicode test: 你好世界 🌍 Привет مرحبا".encode('utf-8')

        src_file = tmp_path / "unicode.txt"
        src_file.write_bytes(content)

        enc_file = tmp_path / "unicode.enc"
        key = be.generate_encryption_key()
        be._encrypt_file(str(src_file), str(enc_file), key)

        dec_file = tmp_path / "unicode_dec.txt"
        be._decrypt_file(str(enc_file), str(dec_file), key)

        decrypted = dec_file.read_bytes()
        assert decrypted == content
        # Verify it's valid UTF-8
        assert decrypted.decode('utf-8')


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
