#!/usr/bin/env python2

import unittest
import os
import stat
from MockDBI import MockConnection

import databaseapps.Ingest as Ingest
import databaseapps.ingestutils as ingutil

'''
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
        ing = Ingest()
'''

class TestIngestUtils(unittest.TestCase):
    def test_getShortFilename(self):
        pass


if __name__ == '__main__':
    unittest.main()
