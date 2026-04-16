import logging
import os
import ssl
import sys
import json
import shutil
import socket
import tempfile
import threading
import time
from pathlib import Path

import backup_engine
from transport_utils import upload_to_sftp, upload_to_ftp, upload_to_smb, upload_to_https

try:
    import paramiko
except ImportError:
    raise SystemExit('paramiko is required for SFTP tests')
try:
    from pyftpdlib.handlers import FTPHandler
    from pyftpdlib.servers import FTPServer
    from pyftpdlib.authorizers import DummyAuthorizer
except ImportError:
    raise SystemExit('pyftpdlib is required for FTP tests')
try:
    from http.server import BaseHTTPRequestHandler, HTTPServer
    import cgi
except ImportError:
    raise SystemExit('http.server is required for HTTPS tests')

ROOT = Path(tempfile.mkdtemp(prefix='backupsys_live_test_'))
SRC_DIR = ROOT / 'src'
SFTP_ROOT = ROOT / 'sftp_root'
FTP_ROOT = ROOT / 'ftp_root'
HTTPS_ROOT = ROOT / 'https_root'
SMB_REMOTE = Path(r'\\localhost\smb_test')

SRC_DIR.mkdir(parents=True, exist_ok=True)
SFTP_ROOT.mkdir(parents=True, exist_ok=True)
FTP_ROOT.mkdir(parents=True, exist_ok=True)
HTTPS_ROOT.mkdir(parents=True, exist_ok=True)

(SRC_DIR / 'file1.txt').write_text('SFTP/FTP/SMB/HTTPS live test - file1', encoding='utf-8')
(SRC_DIR / 'nested').mkdir(exist_ok=True)
(SRC_DIR / 'nested' / 'file2.txt').write_text('Nested file content', encoding='utf-8')

REMOTE_SFTP_PORT = 2222
REMOTE_FTP_PORT = 2121
HTTPS_PORT = 9000

class StubServer(paramiko.ServerInterface):
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.event = threading.Event()

    def check_auth_password(self, username, password):
        if username == self.username and password == self.password:
            return paramiko.AUTH_SUCCESSFUL
        return paramiko.AUTH_FAILED

    def get_allowed_auths(self, username):
        return 'password'

    def check_channel_request(self, kind, chanid):
        print(f'SFTP server: channel request kind={kind} chanid={chanid}')
        if kind == 'session':
            return paramiko.OPEN_SUCCEEDED
        return paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED

    def check_channel_subsystem_request(self, channel, name):
        print(f'SFTP server: subsystem request name={name} channel={channel}')
        if name == 'sftp':
            return True
        return False

class SimpleSFTPServer(paramiko.SFTPServerInterface):
    def __init__(self, server, *args, **kwargs):
        super().__init__(server, *args, **kwargs)
        self.root = str(SFTP_ROOT)

    def _abs_path(self, path):
        if path.startswith('/'):
            path = path[1:]
        path = path.replace('/', os.sep)
        abs_path = os.path.normpath(os.path.join(self.root, path))
        if not abs_path.startswith(os.path.normpath(self.root)):
            raise IOError('Invalid path')
        return abs_path

    def stat(self, path):
        p = self._abs_path(path)
        st = os.stat(p)
        return paramiko.SFTPAttributes.from_stat(st)

    lstat = stat

    def list_folder(self, path):
        p = self._abs_path(path)
        entries = []
        for name in os.listdir(p):
            entry_path = os.path.join(p, name)
            st = os.stat(entry_path)
            attr = paramiko.SFTPAttributes.from_stat(st)
            attr.filename = name
            entries.append(attr)
        return entries

    def mkdir(self, path, attr):
        os.makedirs(self._abs_path(path), exist_ok=True)

    def remove(self, path):
        os.remove(self._abs_path(path))

    def rmdir(self, path):
        os.rmdir(self._abs_path(path))

    def open(self, path, flags, attr):
        abspath = self._abs_path(path)
        os.makedirs(os.path.dirname(abspath), exist_ok=True)
        mode = ''
        if flags & os.O_WRONLY:
            mode = 'wb'
        elif flags & os.O_RDWR:
            mode = 'r+b'
        else:
            mode = 'rb'
        f = open(abspath, mode)
        handle = paramiko.SFTPHandle(flags)
        handle.filename = abspath
        if 'r' in mode:
            handle.readfile = f
        if 'w' in mode or 'b' in mode:
            handle.writefile = f
        return handle


def start_sftp_server():
    logging.basicConfig(level=logging.DEBUG)
    host_key_path = ROOT / 'sftp_host_key.pem'
    if not host_key_path.exists():
        host_key = paramiko.RSAKey.generate(2048)
        host_key.write_private_key_file(str(host_key_path))
    else:
        host_key = paramiko.RSAKey(filename=str(host_key_path))
    paramiko.util.log_to_file(str(ROOT / 'paramiko_sftp.log'))

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('127.0.0.1', REMOTE_SFTP_PORT))
    sock.listen(100)

    def run_server():
        while True:
            client, addr = sock.accept()
            print('SFTP connection from', addr)
            try:
                transport = paramiko.Transport(client)
                transport.add_server_key(host_key)
                transport.set_subsystem_handler('sftp', paramiko.SFTPServer, SimpleSFTPServer)
                server = StubServer('sftpuser', 'sftppass')
                transport.start_server(server=server)
                chan = transport.accept(20)
                if chan is None:
                    print('SFTP no channel accepted')
                    transport.close()
                    continue
                print('SFTP channel accepted')
                while transport.is_active():
                    time.sleep(0.1)
                print('SFTP transport inactive, closing')
            except Exception as exc:
                print('SFTP server exception:', exc)
            finally:
                try:
                    transport.close()
                except Exception:
                    pass

    thread = threading.Thread(target=run_server, daemon=True)
    thread.start()
    time.sleep(0.5)
    return thread


def create_ftps_cert(cert_path, key_path):
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, 'localhost'),
    ])
    cert = x509.CertificateBuilder().subject_name(subject).issuer_name(issuer).public_key(
        key.public_key()
    ).serial_number(x509.random_serial_number()).not_valid_before(
        x509.datetime.datetime.utcnow()
    ).not_valid_after(
        x509.datetime.datetime.utcnow() + x509.timedelta(days=365)
    ).add_extension(x509.SubjectAlternativeName([x509.DNSName('localhost')]), critical=False).sign(key, hashes.SHA256())
    with open(cert_path, 'wb') as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
    with open(key_path, 'wb') as f:
        f.write(key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        ))


def start_ftp_server():
    authorizer = DummyAuthorizer()
    authorizer.add_user('ftpuser', 'ftppass', str(FTP_ROOT), perm='elradfmwM')
    handler = FTPHandler
    handler.authorizer = authorizer
    server = FTPServer(('127.0.0.1', REMOTE_FTP_PORT), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    time.sleep(0.5)
    return server, thread

class HTTPSUploadHandler(BaseHTTPRequestHandler):
    received = []

    def do_POST(self):
        content_type = self.headers.get('Content-Type', '')
        if 'multipart/form-data' not in content_type:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'Bad content type')
            return
        fs = cgi.FieldStorage(fp=self.rfile, headers=self.headers, environ={
            'REQUEST_METHOD': 'POST',
            'CONTENT_TYPE': content_type,
        })
        if 'file' not in fs:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'Missing file field')
            return
        field = fs['file']
        filename = fs.getvalue('filename') or field.filename or 'uploaded.bin'
        file_path = HTTPS_ROOT / filename
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'wb') as out:
            out.write(field.file.read())
        self.received.append(str(file_path))
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'OK')


def start_https_server():
    server = HTTPServer(('127.0.0.1', HTTPS_PORT), HTTPSUploadHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    time.sleep(0.5)
    return server, thread


def run_upload_tests():
    print('SOURCE DIR:', SRC_DIR)

    sftp_thread = start_sftp_server()
    ftp_server, ftp_thread = start_ftp_server()
    https_server, https_thread = start_https_server()

    results = {}

    print('Testing SFTP upload...')
    sftp_cfg = {
        'host': '127.0.0.1',
        'port': REMOTE_SFTP_PORT,
        'username': 'sftpuser',
        'password': 'sftppass',
        'remote_path': '/backups_test',
    }
    results['sftp'] = upload_to_sftp(str(SRC_DIR), sftp_cfg)
    print('SFTP result:', results['sftp'])

    print('Testing FTP upload...')
    ftp_cfg = {
        'host': '127.0.0.1',
        'port': REMOTE_FTP_PORT,
        'username': 'ftpuser',
        'password': 'ftppass',
        'remote_path': '/backups_test',
        'use_tls': False,
    }
    results['ftp'] = upload_to_ftp(str(SRC_DIR), ftp_cfg)
    print('FTP result:', results['ftp'])

    print('Testing HTTPS upload...')
    https_cfg = {
        'url': f'http://127.0.0.1:{HTTPS_PORT}/upload',
        'token': '',
        'headers': {},
        'verify_ssl': False,
    }
    results['https'] = upload_to_https(str(SRC_DIR), https_cfg)
    print('HTTPS result:', results['https'])

    print('Testing SMB upload...')
    if not SMB_REMOTE.exists():
        raise SystemExit(f'SMB remote share path not found: {SMB_REMOTE}')
    smb_cfg = {
        'server': 'localhost',
        'share': 'smb_test',
        'username': '',
        'password': '',
        'remote_path': 'backups_test',
    }
    results['smb'] = upload_to_smb(str(SRC_DIR), smb_cfg)
    print('SMB result:', results['smb'])

    print('\nRESULTS:')
    print(json.dumps(results, indent=2))

    print('\nCleanup servers...')
    ftp_server.close_all()
    https_server.shutdown()
    print('done')

if __name__ == '__main__':
    run_upload_tests()
