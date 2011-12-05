#!/usr/bin/env python
#-*- coding:utf-8 -*-


__USAGE__ = """%prog [action]
    archive-wal [name] [path]
    restore-wal [name] [path]
    archive-base [ref]
    restore-base [ref]
    batch-remove [keep_after]"""


import os
import sys
import pwd
import commands
import subprocess
import time
import tarfile
from os import path
from tempfile import mkstemp
from ConfigParser import SafeConfigParser, NoOptionError, NoSectionError
from optparse import OptionParser
from shutil import rmtree
from bz2 import compress, decompress


try:
    from fs.base import FS
    from fs.opener import fsopendir
    from fs.errors import ResourceNotFoundError
except ImportError:
    print('pyFilesystem is required')
    sys.exit(1)


class TransferError(Exception):
    pass

class PGError(Exception):
    pass


def main():
    """
    Main application
    """

    parser = OptionParser(usage=__USAGE__)
    parser.add_option('-c', '--conf', dest='conf', default='/etc/pgstore.conf',
        help='A config file to load. [%default]')
    parser.add_option('-s', '--standby', dest='standby', action='store_true', default=False,
        help='Put wal restore in recovery mode. i.e. wait for next wal file rather then quiting. [%default]')
    (options, args) = parser.parse_args()

    config = SafeConfigParser()
    config.read(options.conf)

    try:
        data_directory = config.get('default', 'data_directory')
    except NoOptionError, e:
        data_directory = '/var/lib/postgresql/8.4/main'

    try:
        db_user = config.get('default', 'db_user')
    except NoOptionError, e:
        db_user = 'postgres'

    try:
        restore_location = config.get('default', 'restore_location')
        archive_location = config.get('default', 'archive_location')
    except NoOptionError, e:
        parser.error(e)

    try:
        for env in config.options('env'):
            os.environ[env.upper()] = config.get('env', env)
    except NoSectionError:
        pass

    actions = {
        'archive-wal': archive_wal,   'restore-wal': restore_wal,
        'archive-base': archive_base, 'restore-base': restore_base,
        'batch-remove': batch_remove,
    }

    try:
        action = args[0]
        actions[action]
    except IndexError:
        parser.error('Need an action')
    except KeyError:
        parser.error('Action "%s" not recognised!' % action)

    if action in ['archive-wal', 'restore-wal']:
        try:
            (file_name, file_path) = args[1:3]
            file_path = path.join(data_directory, file_path)
        except ValueError:
            parser.error('Need [name] [path]!')

    if action in ['archive-base', 'restore-base', 'batch-remove']:
        try:
            ref = args[1]
        except IndexError:
            ref = None

    if action == 'archive-wal':
        store_fs = getdir(fsopendir(archive_location), 'wal')
        run_cmd(archive_wal, [file_path, file_name, store_fs])

    if action == 'restore-wal':
        store_fs = getdir(fsopendir(restore_location), 'wal')
        args = [file_path, file_name, store_fs]
        if options.standby:
            run_cmd(restore_wal_standby, args)
        else:
            run_cmd(restore_wal, args)

    if action == 'archive-base':
        store_fs = getdir(fsopendir(archive_location), 'base')
        run_cmd(archive_base, [data_directory, store_fs, db_user], {'ref': ref})

    if action == 'restore-base':
        store_fs = getdir(fsopendir(restore_location), 'base')
        run_cmd(restore_base, [data_directory, store_fs, db_user], {'ref': ref})

    if action == 'batch-remove':
        store_fs = getdir(fsopendir(archive_location), 'wal')
        run_cmd(batch_remove, [store_fs], {'ref': ref})

def getdir(store_fs, dir_name):
    """
    Try to open a dir on a fs. If it doesn't exist create it and return it
    """

    try:
        return store_fs.opendir(dir_name)
    except ResourceNotFoundError:
        store_fs.makedir(dir_name)
        return store_fs.opendir(dir_name)


def run_cmd(cmd, args=[], kwargs={}):
    """
    Run a command
    """

    try:
        cmd(*args, **kwargs)
        exit(code=0)
    except Exception, e:
        exit(e, code=1)


class Store(object):
    def __init__(self, store_fs):
        if not isinstance(store_fs, FS):
            store_fs = fsopendir(store_fs)
        self.store_fs = store_fs

    def _get_path(self, ref):
        return u'%s.bz2' % ref

    def items(self):
        return [path.splitext(i)[0] for i in self.store_fs.listdir()]

    def exists(self, ref):
        return self.store_fs.exists(self._get_path(ref))

    def setcontents(self, ref, data):
        tmp_name = u'%s.tmp' % ref
        self.store_fs.setcontents(tmp_name, data)
        self.store_fs.rename(tmp_name, self._get_path(ref))

    def getcontents(self, ref):
        return self.store_fs.getcontents(self._get_path(ref))

    def remove(self, ref):
        self.store_fs.remove(self._get_path(ref))

    def batch_remove(self, keep_after=None):
        for ref in self.items():
            if keep_after and ref >= keep_after:
                continue
            self.remove(ref)


def batch_remove(store, ref=None):
    """
    Batch remove wall archives from store
    """

    if not isinstance(store, Store):
        store = Store(store)

    if not store.exists(ref):
        raise TransferError('The file "%s" is not in archive!' % ref)

    store.batch_remove(ref)


def archive_wal(file_path, file_name, store):
    """
    Archive wal file
    """

    if not isinstance(store, Store):
        store = Store(store)

    if store.exists(file_name):
        raise TransferError('Wal file "%s" already archived!' % file_name)

    fh = open(file_path, 'rb')
    try:
        data = compress(fh.read(), 9)
    finally:
        fh.close()

    store.setcontents(file_name, data)


def restore_wal(file_path, file_name, store):
    """
    Restore wal archive
    """

    if not isinstance(store, Store):
        store = Store(store)

    try:
        data = store.getcontents(file_name)
    except ResourceNotFoundError:
        raise TransferError('Wal file "%s" not in archive!' % file_name)

    fh = open(file_path, 'wb')
    try:
        fh.write(decompress(data))
    finally:
        fh.close()


def restore_wal_standby(file_path, file_name, store, wait=0.5, max_wait=60):
    """
    Restore wal archive waiting if file not found
    """

    if not isinstance(store, Store):
        store = Store(store)

    ext = path.splitext(file_name)[1]
    if ext in ('.backup', '.history'):
        return restore_wal(file_path, file_name, store)

    while True:
        try:
            data = store.getcontents(file_name)

            fh = open(file_path, 'wb')
            try:
                fh.write(decompress(data))
            finally:
                fh.close()

            break

        except ResourceNotFoundError:
            pass

        if path.exists('/tmp/halt-postgres-recovery.tmp'):
            raise PGError('Recovery halted!')

        time.sleep(wait)
        wait *= 2
        if wait > max_wait:
            wait = max_wait


def archive_base(data_dir, store, db_user, ref=None):
    """
    Tar/gzip base data dir then archive 
    """

    if not isinstance(store, Store):
        store = Store(store)

    if not ref:
        ref = str(time.strftime('%Y-%m-%d-%H:%M:%S', time.gmtime()))

    try:
        pg_cmd('SELECT * FROM pg_start_backup(\'%s\')' % ref, db_user)

        tmpfile_name = mkstemp()[1]
        tmpfile_obj = open(tmpfile_name, 'wb')
        tar = tarfile.open(fileobj=tmpfile_obj, mode='w:bz2')
        for filename in os.listdir(data_dir):
            if filename != 'pg_xlog':
                tar.add(path.join(data_dir, filename), arcname=filename)
        tar.close()
        tmpfile_obj.close()

        fh = open(tmpfile_name, 'rb')
        try:
            data = fh.read()
        finally:
            fh.close()
            os.remove(tmpfile_name)
    
        store.setcontents(ref, data)
        print('Created base archive: %s' % ref)
        print(open('%s/backup_label' % data_dir, 'r').read())
    finally:
        pg_cmd('SELECT * FROM pg_stop_backup()', db_user)


def restore_base(data_dir, store, db_user, ref=None):
    """
    Restore base from archive
    """

    if not isinstance(store, Store):
        store = Store(store)

    if not ref:
        print(store.items())
        raise TransferError('You must provide ref to restore from.')

    if not store.exists(ref):
        raise TransferError('Base archive does not exist: %s.' % ref)

    run('service postgresql stop')

    for x in os.listdir(data_dir):
        x = path.join(data_dir, x)
        try:
            os.remove(x)
        except OSError:
            rmtree(x)

    data = store.getcontents(ref)

    tmpfile_name = mkstemp()[1]
    fh = open(tmpfile_name, 'wb')
    try:
        fh.write(data)
    finally:
        fh.close()

    tar = tarfile.open(fileobj=open(tmpfile_name, 'rb'), mode='r:bz2')
    tar.extractall(data_dir)
    tar.close()
    os.remove(tmpfile_name)

    xlog_path = path.join(data_dir, 'pg_xlog')
    pdb = pwd.getpwnam(db_user)
    os.mkdir(xlog_path, 0700)
    os.chown(xlog_path, pdb[2], pdb[3])

    recovery_path = path.join(data_dir, 'recovery.conf')
    fh = open(recovery_path, 'w')
    try:
        fh.write("restore_command = '/usr/local/bin/pgstore restore-wal --standby %f %p'\n")
    finally:
        fh.close()
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
