#!/usr/bin/env python2
# pylint: skip-file

import unittest
import os
import stat
from MockDBI import MockConnection
import sys
import copy

from contextlib import contextmanager
from StringIO import StringIO

import databaseapps.Ingest as Ingest
import databaseapps.ingestutils as ingutil
from despydb import desdbi

import catalog_ingest as cati
import datafile_ingest as dfi

@contextmanager
def capture_output():
    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err

class TestCatalogIngest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print 'SETUP'
        cls.sfile = 'services.ini'
        open(cls.sfile, 'w').write("""

[db-maximal]
PASSWD  =   maximal_passwd
name    =   maximal_name_1    ; if repeated last name wins
user    =   maximal_name      ; if repeated key, last one wins
Sid     =   maximal_sid       ;comment glued onto value not allowed
type    =   POSTgres
server  =   maximal_server

[db-minimal]
USER    =   Minimal_user
PASSWD  =   Minimal_passwd
name    =   Minimal_name
sid     =   Minimal_sid
server  =   Minimal_server
type    =   oracle

[db-test]
USER    =   Minimal_user
PASSWD  =   Minimal_passwd
name    =   Minimal_name
sid     =   Minimal_sid
server  =   Minimal_server
type    =   test
port    =   0
""")
        os.chmod(cls.sfile, (0xffff & ~(stat.S_IROTH | stat.S_IWOTH | stat.S_IRGRP | stat.S_IWGRP )))

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.sfile)
        MockConnection.destroy()

    def test_ingest(self):
        os.environ['DES_SERVICES'] = self.sfile
        os.environ['DES_DB_SECTION'] = 'db-test'
        temp = copy.deepcopy(sys.argv)
        sys.argv = ['catalog_ingest.py',
                    '-request',
                    '3463',
                    '-filename',
                    '/var/lib/jenkins/test_data/D00526157_r_c01_r3463p01_red-fullcat.fits',
                    '-filetype',
                    'cat_firstcut',
                    '-targettable',
                    'MAIN.SE_OBJECT']
        output = ''
        with capture_output() as (out, err):
            self.assertEqual(cati.main(), 0)
            output = out.getvalue().strip()
        count = 0
        table = None
        output = output.split('\n')
        for line in output:
            if 'LOAD' in line and 'finished' in line:
                line = line[line.find('LOAD'):]
                temp = line.split()
                count = int(temp[1])
            elif 'Creating tablespace' in line:
                line = line[line.find('MAIN.'):]
                temp = line.split()[0]
                table = temp.split('.')[1]
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        curs = dbh.cursor()
        curs.execute('select count(*) from ' + table)
        res = curs.fetchall()[0][0]
        self.assertEqual(res, count)

        sys.argv = ['catalog_ingest.py',
                    '-request',
                    '3463']
        self.assertEqual(cati.main(), 1)
        sys.argv = temp

    #def test_fail_states(self):

class TestDatafileIngest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print 'SETUP'
        cls.sfile = 'services.ini'
        open(cls.sfile, 'w').write("""

[db-maximal]
PASSWD  =   maximal_passwd
name    =   maximal_name_1    ; if repeated last name wins
user    =   maximal_name      ; if repeated key, last one wins
Sid     =   maximal_sid       ;comment glued onto value not allowed
type    =   POSTgres
server  =   maximal_server

[db-minimal]
USER    =   Minimal_user
PASSWD  =   Minimal_passwd
name    =   Minimal_name
sid     =   Minimal_sid
server  =   Minimal_server
type    =   oracle

[db-test]
USER    =   Minimal_user
PASSWD  =   Minimal_passwd
name    =   Minimal_name
sid     =   Minimal_sid
server  =   Minimal_server
type    =   test
port    =   0
""")
        os.chmod(cls.sfile, (0xffff & ~(stat.S_IROTH | stat.S_IWOTH | stat.S_IRGRP | stat.S_IWGRP )))

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.sfile)
        MockConnection.destroy()

    def test_ingest(self):
        os.environ['DES_SERVICES'] = self.sfile
        os.environ['DES_DB_SECTION'] = 'db-test'
        temp = copy.deepcopy(sys.argv)
        sys.argv = ['datafile_ingest.py',
                    '--filename',
                    '/var/lib/jenkins/test_data/D00526157_r_c01_r3463p01_hpix.fits',
                    '--filetype',
                    'red_hpix']
        output = ''
        with capture_output() as (out, err):
            dfi.main()
            output = out.getvalue().strip()
        count = 0
        table = 'SE_OBJECT_HPIX'
        output = output.split('\n')
        for line in output:
            if 'ingest of' in line:
                temp = line.split(',')[1]
                count = int(temp.split()[0])
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        curs = dbh.cursor()
        curs.execute('select count(*) from ' + table)
        res = curs.fetchall()[0][0]
        self.assertEqual(res, count)

        sys.argv = temp


class TestIngest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print 'SETUP'
        cls.sfile = 'services.ini'
        open(cls.sfile, 'w').write("""

[db-maximal]
PASSWD  =   maximal_passwd
name    =   maximal_name_1    ; if repeated last name wins
user    =   maximal_name      ; if repeated key, last one wins
Sid     =   maximal_sid       ;comment glued onto value not allowed
type    =   POSTgres
server  =   maximal_server

[db-minimal]
USER    =   Minimal_user
PASSWD  =   Minimal_passwd
name    =   Minimal_name
sid     =   Minimal_sid
server  =   Minimal_server
type    =   oracle

[db-test]
USER    =   Minimal_user
PASSWD  =   Minimal_passwd
name    =   Minimal_name
sid     =   Minimal_sid
server  =   Minimal_server
type    =   test
port    =   0
""")
        os.chmod(cls.sfile, (0xffff & ~(stat.S_IROTH | stat.S_IWOTH | stat.S_IRGRP | stat.S_IWGRP )))

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.sfile)
        MockConnection.destroy()

    def test_init(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        ing = Ingest.Ingest('cat_finalcut', 'test.junk', dbh=dbh)
        self.assertEqual(ing.getstatus(), 0)

    def test_debug_and_info(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        ing = Ingest.Ingest('cat_finalcut', 'test.junk', dbh=dbh)
        msg = "test message"
        ing._debug = False
        with capture_output() as (out, err):
            ing.debug(msg)
            output = out.getvalue().strip()
            self.assertEqual(output, "")

        ing._debug = True
        with capture_output() as (out, err):
            ing.debug(msg)
            output = out.getvalue().strip()
            self.assertTrue(msg in output)

        msg = "info message"
        with capture_output() as (out, err):
            ing.info(msg)
            output = out.getvalue().strip()
            self.assertTrue(msg in output)

        msg = "info message"
        with capture_output() as (out, err):
            ing.printinfo(msg)
            output = out.getvalue().strip()
            self.assertTrue(msg in output)

    def test_getObjectColumns(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        ing = Ingest.Ingest('cat_finalcut', 'test.junk', dbh=dbh)
        cols = ing.getObjectColumns()
        self.assertTrue('WCL' in cols.keys())
        self.assertTrue('FILENAME' in cols['WCL'].keys())

    def test_blanks(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        ing = Ingest.Ingest('cat_finalcut', 'test.junk', dbh=dbh)
        self.assertIsNone(ing.getNumObjects())
        self.assertIsNone(ing.generateRows())
        self.assertEqual(ing.numAlreadyIngested(), 0)
        self.assertFalse(ing.isLoaded())

class TestIngestUtils(unittest.TestCase):
    def test_getShortFilename(self):
        fname = 'test.fits'
        path = '/the/path/to/thefile/' + fname
        self.assertEqual(fname, ingutil.IngestUtils.getShortFilename(path))
        self.assertEqual(fname, ingutil.IngestUtils.getShortFilename(fname))

    def test_isInteger(self):
        self.assertTrue(ingutil.IngestUtils.isInteger(5))
        self.assertTrue(ingutil.IngestUtils.isInteger(5.8))
        self.assertFalse(ingutil.IngestUtils.isInteger('fna'))

if __name__ == '__main__':
    unittest.main()
