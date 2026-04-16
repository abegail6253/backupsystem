import os
import socket
import threading
import tempfile
import time

import paramiko

ROOT = tempfile.mkdtemp(prefix='backupsys_sftp_repro_')
SFTP_ROOT = os.path.join(ROOT, 'root')
os.makedirs(SFTP_ROOT, exist_ok=True)

class StubServer(paramiko.ServerInterface):
    def __init__(self, username, password):
        self.username = username
        self.password = password

    def check_auth_password(self, username, password):
        print('server: auth request', username)
        if username == self.username and password == self.password:
            return paramiko.AUTH_SUCCESSFUL
        return paramiko.AUTH_FAILED

    def get_allowed_auths(self, username):
        return 'password'

    def check_channel_request(self, kind, chanid):
        print('server: channel request', kind)
        if kind == 'session':
            return paramiko.OPEN_SUCCEEDED
        return paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED

    def check_channel_subsystem_request(self, channel, name):
        print('server: subsystem request', name)
        if name == 'sftp':
            return True
        return False

class SimpleSFTPServer(paramiko.SFTPServerInterface):
    def __init__(self, server, *args, **kwargs):
        print('SimpleSFTPServer.__init__ called')
        super().__init__(server, *args, **kwargs)
        self.root = SFTP_ROOT

    def _abs_path(self, path):
        if path.startswith('/'):
            path = path[1:]
        abs_path = os.path.normpath(os.path.join(self.root, path))
        if not abs_path.startswith(os.path.normpath(self.root)):
            raise IOError('Invalid path')
        return abs_path

    def list_folder(self, path):
        print(f'SimpleSFTPServer.list_folder({path})')
        p = self._abs_path(path)
        return []

    def stat(self, path):
        print(f'SimpleSFTPServer.stat({path})')
        return paramiko.SFTPAttributes.from_stat(os.stat(self._abs_path(path)))

    def open(self, path, flags, attr):
        print(f'SimpleSFTPServer.open({path}, {flags})')
        abspath = self._abs_path(path)
        os.makedirs(os.path.dirname(abspath), exist_ok=True)
        mode = 'wb' if flags & os.O_WRONLY else 'rb'
        f = open(abspath, mode)
        handle = paramiko.SFTPHandle(flags)
        handle.filename = abspath
        if 'w' in mode:
            handle.writefile = f
        else:
            handle.readfile = f
        return handle


def start_server():
    host_key = paramiko.RSAKey.generate(2048)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('127.0.0.1', 2222))
    sock.listen(5)

    def run():
        client, addr = sock.accept()
        print('server: accepted', addr)
        t = paramiko.Transport(client)
        t.add_server_key(host_key)
        t.set_subsystem_handler('sftp', paramiko.SFTPServer, SimpleSFTPServer)
        print('server: subsystem_table=', t.subsystem_table)
        server = StubServer('sftpuser', 'sftppass')
        t.start_server(server=server)
        chan = t.accept(20)
        print('server: channel', chan)
        while t.is_active():
            time.sleep(0.1)
        print('server: transport ended')
        t.close()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    time.sleep(0.5)
    return thread

thread = start_server()

print('client: start')
try:
    transport = paramiko.Transport(('127.0.0.1', 2222))
    transport.connect(username='sftpuser', password='sftppass')
    print('client: connected auth', transport.is_authenticated())
    sftp = paramiko.SFTPClient.from_transport(transport)
    print('client: got sftp', sftp)
    sftp.mkdir('/remote_test_dir')
    print('client: created directory')
    transport.close()
except Exception as e:
    print('client error', e)

print('done')
