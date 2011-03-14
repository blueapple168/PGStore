#!/usr/bin/env python
#-*- coding:utf-8 -*-


__USAGE__ = """%prog [action]
    archive-wal [name] [path]
    restore-wal [name] [path]
    archive-base
    restore-base [ref]"""


import os
import sys
import pwd
import commands
import tarfile
import gzip
import subprocess
import time
from os import path
from tempfile import NamedTemporaryFile, mkstemp
from ConfigParser import SafeConfigParser, NoOptionError
from optparse import OptionParser
from urlparse import urlparse
from shutil import copyfile, rmtree


class SaveError(Exception):
    pass

class URLError(Exception):
    pass

class PGError(Exception):
    pass

class LocalBackend(object):
    """
    An local filesystem backend
    """

    def __init__(self, base_dir):
        self._base_dir = base_dir

    def put(self, file_path, name, overwrite=False):
        """
        Archive file
        """
        
        raise NotImplementedError

    def fetch(self, file_path, name):
        """
        Restore file
        """

        copyfile(self._get_path(name), file_path)

    def _get_path(self, name):
        """
        Return full path from name
        """

        return path.join(self._base_dir, name)


class SSHBackend(object):
    """
    An ssh backend to copy files over ssh
    """

    def __init__(self, host, user, path, port=22):
        self.host = host
        self.user = user
        self.path = path
        self.port = port

    def put(self, file_path, dest_name, overwrite=False, exclude=None):
        """
        Archive file to ssh server
        """

        dest_path = self._get_dest_path(dest_name)
        file_path, file_name = path.split(file_path)

        if overwrite == False and self.exist(dest_name):
            raise SaveError('File already exist')
        
        cmd = ['tar cz', '-C %s' % file_path]
        if exclude:
             cmd.append('--exclude %s' % exclude)
        #if file_name != path.basename(dest_name):
            #cmd.append('--xform=s/%s/%s/' % (file_name, path.basename(dest_name)))
        cmd.append(file_name)
        cmd.append('|')
        cmd.append('ssh -qp %i %s@%s' % (int(self.port), self.user, self.host))
        #cmd.append('"tar xzf - -C %s"' % path.dirname(dest_path))
        cmd.append('"mkdir -p %s && dd of=%s"' % (path.dirname(dest_path), dest_path))
        run(' '.join(cmd))

    def exist(self, name):
        """
        Check if file exists
        """

        full_path = self._get_dest_path(name)
        p = self._ssh_cmd('ls %s 1> /dev/null 2> /dev/null' % full_path)
        return True if p.returncode == 0 else False

    def fetch(self, dest_path, name):
        """
        Restore file from ssh server
        """

        run(' '.join([
            'ssh -qp %i %s@%s' % (int(self.port), self.user, self.host),
            '"cat %s"' % self._get_dest_path(name),
            '|',
            'tar zxf - -C %s' % path.dirname(dest_path)
        ]))

    def _ssh_cmd(self, cmd):
        """
        Run a command over ssh
        """

        cmd = 'ssh -qp %i %s@%s "%s"' % (int(self.port), self.user, self.host, cmd)
        return run(cmd)

    def _get_dest_path(self, dest_name):
        """
        Return destination path from destination name
        """

        return '%s.tar.gz' % path.join(self.path, dest_name)


def parse_url(url):
    """
    Parse url and return an instance of backend
    """

    parsed_url = urlparse(url)
    
    if parsed_url.scheme in ('', 'file'):
        return LocalBackend(parsed_url.path)

    if parsed_url.scheme == 'ssh':
        (args, kwargs) = ([], {})       
        netloc = parsed_url.netloc        

        try:
            (netloc, kwargs['port']) = netloc.split(':')
        except ValueError:
            pass

        try:
            (user, host) = netloc.split('@')
            args.append(host)
            args.append(user)
        except ValueError, e:
            raise URLError('No ssh username supplied')

        args.append(parsed_url.path)
        return SSHBackend(*args, **kwargs)

    raise URLError('Scheme not supported: %s' % url)


def main():
    """
    Main application
    """

    parser = OptionParser(usage=__USAGE__)
    parser.add_option('-c', dest='conf', default='/etc/pgstore.conf',
        help='A config file to load. [/etc/pgstore.conf]')
    (options, args) = parser.parse_args()

    config = SafeConfigParser()
    config.read(options.conf)

    try:
        data_directory = config.get('default', 'data_directory')
    except NoOptionError, e:
        data_directory = '/var/lib/postgresql/8.4/main'

    try:
        hba_file = config.get('default', 'hba_file')
    except NoOptionError, e:
        hba_file = '/etc/postgresql/8.4/main/pg_hba.conf'

    try:
        db_user = config.get('default', 'db_user')
    except NoOptionError, e:
        db_user = 'postgres'

    try:
        restore_location = config.get('default', 'restore_location')
        archive_location = config.get('default', 'archive_location')
    except NoOptionError, e:
        parser.error(e)

    actions = {
        'archive-wal': archive_wal,   'restore-wal': restore_wal,
        'archive-base': archive_base, 'restore-base': restore_base
    }

    try:
        action = args[0]
        action_cmd = actions[action]
    except IndexError:
        parser.error('Need an action')
    except KeyError:
        parser.error('Action "%s" not recognised!' % action)

    try:    
        if action in ('archive-wal', 'archive-base'):
            bkend = parse_url(archive_location)
        else:
            bkend = parse_url(restore_location)
    except URLError, e:
        parser.error(e)

    if action in ('archive-wal', 'restore-wal'):
        try:
            (file_name, file_path) = args[1:3]
        except ValueError:
            parser.error('Need [name] [path]!')

        file_path = path.join(data_directory, file_path)
        run_cmd(action_cmd, [file_path, file_name, bkend])

    if action in ('archive-base', 'restore-base'):
        try:
            ref = args[1]
        except IndexError:
            ref = None

        run_cmd(action_cmd, [data_directory, bkend, db_user], {'ref': ref})


def run_cmd(cmd, args=[], kwargs={}):
    """
    Run a command functon
    """

    try:
        cmd(*args, **kwargs)
        exit(code=0)
    except (SaveError, PGError), e:
        exit(e, code=1)


def archive_wal(file_path, file_name, bkend):
    """
    Archive with backend
    """

    bkend.put(file_path, path.join('wal', file_name))


def restore_wal(file_path, file_name, bkend):
    """
    Restore wal archive
    """

    wal_name = path.join('wal', file_name)
    ext = path.splitext(wal_name)[1]

    if ext in ('.backup', '.history'):
        if bkend.exist(wal_name):
            bkend.fetch(file_path, wal_name)
        return

    while True:
        if bkend.exist(wal_name):
            bkend.fetch(file_path, wal_name)
            break

        if path.exists('/tmp/halt-postgres-recovery.tmp'):
            raise PGError('Recovery halted')

        time.sleep(0.1)


def archive_base(data_dir, bkend, db_user, ref=None):
    """
    Tar/gzip base data dir then archive with backend 
    """

    if not ref:
        ref = str(time.strftime('%Y-%m-%d-%H:%M:%S', time.gmtime()))

    try:
        pg_cmd('SELECT * FROM pg_start_backup(\'%s\')' % ref, db_user)
        name = path.join('base', ref)
        bkend.put(data_dir, name, exclude='pg_xlog', overwrite=False)
        print('Created base archive: %s' % ref)
    finally:
        pg_cmd('SELECT * FROM pg_stop_backup()', db_user)


def restore_base(data_dir, bkend, db_user, ref=None):
    """
    Restore base from archive
    """

    if not ref:
        raise SaveError('You must provide ref to restore from.')

    base_name = path.join('base', ref)
    if not bkend.exist(base_name):
        raise SaveError('Base archive does not exist: %s.' % ref)

    run('service postgresql stop')

    for x in os.listdir(data_dir):
        x = path.join(data_dir, x)
        try:
            os.remove(x)
        except OSError:
            rmtree(x)

    bkend.fetch(data_dir, base_name)

    xlog_path = path.join(data_dir, 'pg_xlog')
    pdb = pwd.getpwnam(db_user)
    os.mkdir(xlog_path, 0700)
    os.chown(xlog_path, pdb[2], pdb[3])

    recovery_path = path.join(data_dir, 'recovery.conf')
    f = open(recovery_path, 'w')
    f.write("restore_command = '/usr/local/bin/pgstore restore-wal %f %p'")
    f.close()
    os.chown(recovery_path, pdb[2], pdb[3])

    try:
        os.remove('/tmp/halt-postgres-recovery.tmp')
    except OSError:
        pass

    run('service postgresql start')


def pg_cmd(cmd, db_user):
    """
    Run a Postgresql command
    """

    if pwd.getpwuid(os.getuid())[0] == db_user:
        cmd = 'psql -tc "%s"' % cmd
    else:
        cmd = 'su %s -c "psql -tc \\"%s\\""' % (db_user, cmd)

    (status, stdout) = commands.getstatusoutput(cmd)
    if status != 0:
        raise PGError(stdout)
    return stdout.strip()


def run(cmd):
    """
    Run a local shell command
    """
    
    p = subprocess.Popen([cmd], shell=True, stdout=None, stderr=None)
    (stdout, stderr) = p.communicate()
    return p


def exit(message=None, code=0):
    """
    Exit program with a status code
    """

    if message:
        print(message)
    sys.exit(code)


if __name__ == '__main__':
    main()
