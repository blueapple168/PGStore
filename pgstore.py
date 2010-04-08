#!/usr/bin/env python

import os
import commands
import ftplib
import tarfile
import gzip
from datetime import date
from tempfile import TemporaryFile, mkstemp
from ConfigParser import SafeConfigParser
from optparse import OptionParser


class FTPBackend(object):
    def __init__(self, host, user, passwd, path):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.path = path

    def _connect(self):
        return ftplib.FTP(self.host, self.user, self.passwd)

    def archive(self, fileobj, name, bktype, overwrite=False):
        ftp = self._connect()
        try:
            ftp.cwd('%s/%s' % (self.path, bktype))
        except ftplib.error_perm:
            ftp.cwd(self.path)
            ftp.mkd(bktype)
            ftp.cwd('%s/%s' % (self.path, bktype))
        if not overwrite and name in ftp.nlst():
            ftp.quit()
            raise Exception('File aready backed up!')
        ftp.storbinary('STOR %s' % name, fileobj)
        ftp.quit()

    def restore(self, fileobj, name, bktype):
        ftp = self._connect()
        ftp.cwd('%s/%s' % (self.path, bktype))
        try:
            ftp.retrbinary('RETR %s' % name, fileobj.write)
        finally:
            ftp.quit()


def get_store_config(config, section, defaults={}):
    conf = {}
    for (key, val) in defaults.items():
        conf[key] = val
    for (key, val) in config.items(section):
        if key != 'type':
            conf[key] = val
    return conf


def pg_cmd(cmd):
    (status, stdout) = commands.getstatusoutput('psql -tc "%s"' % cmd)
    if status != 0:
        raise Exception(stdout)
    return stdout.strip()


def gz(fileobj):
    tmpfile = TemporaryFile()
    fout = gzip.GzipFile(fileobj=tmpfile)
    fout.writelines(fileobj)
    fout.close()
    tmpfile.seek(0)
    return tmpfile


def main():
    parser = OptionParser(usage='%prog [action]')
    parser.add_option('-s', dest='store', default='default',
        help='The data store config to use. [default]')
    parser.add_option('-c', dest='conf', default='/etc/pgstore.conf',
        help='A data store config file. [pgstore.conf]')
    (options, args) = parser.parse_args()

    config = SafeConfigParser()
    config.read(options.conf)

    sttype = config.get(options.store, 'type')
    if sttype == 'ftp':
        store_config = get_store_config(config, options.store, {'path': '/'})
        bkend = FTPBackend(**store_config)
    else:
        raise Exception('Storage type "%s" not recognised!' % sttype)

    try:
        action = args[0]
    except IndexError:
        parser.error('Need an action')

    if action in ('archive-wal', 'restore-wal'):
        parser.set_usage('%%prog %s [path] [name]' % action)
        try:
            (path, name) = args[1:3]
        except ValueError:
            parser.error('Need both path and name!')
        if action == 'archive-wal':
            bkend.archive(gz(open(path, 'rb')), '%s.gz' % name, 'wal')
        else:
            tmpfile_name = mkstemp()[1]
            tmpfile = open(tmpfile_name, 'wb')
            bkend.restore(tmpfile, '%s.gz' % name, 'wal')
            tmpfile.close()
            open(path, 'wb').write(gzip.open(tmpfile_name, 'rb').read())
            os.remove(tmpfile_name)

    elif action in ('archive-current', 'archive-base'):
        parser.set_usage('%%prog %s [datadir]' % action)
        try:
            datadir = args[1]
        except IndexError:
            parser.error('Need a datadir')

        if action == 'archive-current':
            name = pg_cmd(
                'SELECT * FROM pg_xlogfile_name(pg_current_xlog_location())'
            )
            path = os.path.join(datadir, 'pg_xlog', name)
            bkend.archive(gz(open(path, 'rb')), 'current.gz', 'wal', overwrite=True)

        elif action == 'archive-base':
            ref = 'base-%s' % date.today()
            pg_cmd('SELECT * FROM pg_start_backup(\'%s\')' % ref)

            def exclude(name):
                if name == 'pg_xlog': return True
            tmpfile_name = mkstemp()[1]
            tar = tarfile.open(fileobj=open(tmpfile_name, 'wb'), mode='w:gz')
            tar.add(datadir, arcname='/data', exclude=exclude)
            tar.close()

            pg_cmd('SELECT * FROM pg_stop_backup()')
            
            bkend.archive(open(tmpfile_name, 'rb'), '%s.tar.gz' % ref, 'base')
            os.remove(tmpfile_name)

    else:
        parser.error('Action "%s" not recognised!' % action)


if __name__ == '__main__':
    main()
