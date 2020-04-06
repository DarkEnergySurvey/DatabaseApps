#!/usr/bin/env python3

import unittest
import os
import stat
import sys
import copy
import mock
import numpy as np
from mock import patch, MagicMock
from contextlib import contextmanager
from io import StringIO
import re
from collections import OrderedDict
from astropy.io import fits

from MockDBI import MockConnection

import databaseapps.Ingest as Ingest
import databaseapps.ingestutils as ingutil
import databaseapps.datafile_ingest_utils as diu
import databaseapps.objectcatalog as ojc
import databaseapps.CoaddCatalog as ccol
from despydb import desdbi

import catalog_ingest as cati
import datafile_ingest as dfi
import mepoch_ingest as mei

@contextmanager
def capture_output():
    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err

def make_data(lastcol=True):
    data = OrderedDict()
    data['count'] = {'format': 'J',
                     'data': np.random.randint(1500, size=1000)}
    data['ra'] = {'format': 'E',
                  'data': np.random.random_sample((1000,)) * 100.}
    if lastcol:
        data['comment'] = {'format': '2A',
                           'data': np.array(['a'] * 1000)}
    return data

def make_xml(name='TESTER'):
    data = make_data()
    xmldata = f"<main><TABLE name=\"{name:s}\">\n"
    cols = []
    for col, val in data.items():
        cols.append(col)
        if 'J' in val['format']:
            dtype = "int"
        elif 'E' in val['format']:
            dtype = "float"
        else:
            dtype = "str"
        xmldata += f'<field name="{col}" datatype="{dtype:s}"/>\n'

    count = len(data[cols[0]]['data'])
    for i in range(count):
        xmldata += "<tr>"
        for col in cols:
            xmldata += f"<td>{str(data[col]['data'][i])}</td>"
        xmldata += "</tr>\n"

    xmldata += "</TABLE>\n</main>\n"
    return xmldata

def make_fits(name='TESTER', lastcol=True):
    cols = []
    data = make_data(lastcol)
    for colname, val in data.items():
        cols.append(fits.Column(name=colname, format=val['format'], array=val['data']))
    hdu = fits.BinTableHDU.from_columns(cols, name=name)

    return hdu

def write_fits(count=1):
    filename = 'test.fits'
    hdulist = fits.HDUList()
    name = 'TESTER'
    for i in range(count):
        hdulist.append(make_fits(name))
    name = None
    hdulist.writeto(filename)


class TestCatalogIngest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sfile = 'services.ini'
        open(cls.sfile, 'w').write("""

[db-test]
USER    =   Minimal_user
PASSWD  =   Minimal_passwd
name    =   Minimal_name
sid     =   Minimal_sid
server  =   Minimal_server
type    =   test
port    =   0
""")
        os.chmod(cls.sfile, (0xffff & ~(stat.S_IROTH | stat.S_IWOTH | stat.S_IRGRP | stat.S_IWGRP)))

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
        print('SETUP')
        cls.sfile = 'services.ini'
        open(cls.sfile, 'w').write("""

[db-test]
USER    =   Minimal_user
PASSWD  =   Minimal_passwd
name    =   Minimal_name
sid     =   Minimal_sid
server  =   Minimal_server
type    =   test
port    =   0
""")
        os.chmod(cls.sfile, (0xffff & ~(stat.S_IROTH | stat.S_IWOTH | stat.S_IRGRP | stat.S_IWGRP)))

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


class TestMepochIngest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sfile = 'services.ini'
        open(cls.sfile, 'w').write("""

[db-test]
USER    =   Minimal_user
PASSWD  =   Minimal_passwd
name    =   Minimal_name
sid     =   Minimal_sid
server  =   Minimal_server
type    =   test
port    =   0
""")
        os.chmod(cls.sfile, (0xffff & ~(stat.S_IROTH | stat.S_IWOTH | stat.S_IRGRP | stat.S_IWGRP)))

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.sfile)
        MockConnection.destroy()

    def test_ingest(self):
        os.environ['DES_SERVICES'] = self.sfile
        os.environ['DES_DB_SECTION'] = 'db-test'
        temp = copy.deepcopy(sys.argv)
        for i in ['cat', 'list', 'wavg', 'mangle']:
            try:
                os.symlink('/var/lib/jenkins/test_data/' + i, i)
            except:
                pass
        sys.argv = ['mepoch_ingest.py',
                    '--detcat',
                    'cat/test_det_cat.fits']
        output = ''
        with capture_output() as (out, err):
            self.assertEqual(mei.main(), 0)
            output = out.getvalue().strip()

        outlines = output.split('\n')

        sys.argv = ['mepoch_ingest.py',
                    '--detcat',
                    'cat/test_det_cat.fits',
                    '--bandcat_list',
                    'list/mepoch-ingest/coadd-cat.list',
                    '--healpix',
                    'cat/test_hpix.fits',
                    '--wavg_list',
                    'list/mepoch-ingest/wavg-band.list',
                    '--wavg_oclink_list',
                    'list/mepoch-ingest/wavg-oclink.list',
                    '--ccdgon_list',
                    'list/mepoch-ingest/ccdgon.list',
                    '--molygon_list',
                    'list/mepoch-ingest/molygon.list',
                    '--molygon_ccdgon_list',
                    'list/mepoch-ingest/molyccd.list',
                    '--coadd_object_molygon_list',
                    'list/mepoch-ingest/cobjmoly.list',
                    '--extinct',
                    'cat/test_ebv-cat.fits',
                    '--extinct_band_list',
                    'list/mepoch-ingest/xcorr.list']
        output = ''
        with capture_output() as (out, err):
            self.assertEqual(mei.main(), 0)
            output = out.getvalue().strip()

        outlines += output.split('\n')
        lookups = []
        table = None
        filename = None
        count = 0
        for line in outlines:
            if 'Working on' in line:
                filename = line[line.rfind('/') + 1:]
            elif 'Inserted' in line:
                temp = line.split()
                count = int(temp[4])
                table = temp[-1]
                lookups.append([filename, table, count])

        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        curs = dbh.cursor()
        for item in lookups:
            curs.execute("select count(*) from " + item[1] + " where filename='" + item[0] + "'")
            res = curs.fetchall()[0][0]
            self.assertEqual(res, item[2])

        sys.argv = temp


class TestIngest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sfile = 'services.ini'
        open(cls.sfile, 'w').write("""

[db-test]
USER    =   Minimal_user
PASSWD  =   Minimal_passwd
name    =   Minimal_name
sid     =   Minimal_sid
server  =   Minimal_server
type    =   test
port    =   0
""")
        os.chmod(cls.sfile, (0xffff & ~(stat.S_IROTH | stat.S_IWOTH | stat.S_IRGRP | stat.S_IWGRP)))

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
        self.assertEqual(ing.getNumObjects(), 0)
        self.assertIsNone(ing.generateRows())
        self.assertEqual(ing.numAlreadyIngested(), 0)
        self.assertFalse(ing.isLoaded())


class TestDatafile_Ingest_Utils(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sfile = 'services.ini'
        cls.table = 'DATAFILE_INGEST_TEST'
        open(cls.sfile, 'w').write("""

[db-test]
USER    =   Minimal_user
PASSWD  =   Minimal_passwd
name    =   Minimal_name
sid     =   Minimal_sid
server  =   Minimal_server
type    =   test
port    =   0
""")
        os.chmod(cls.sfile, (0xffff & ~(stat.S_IROTH | stat.S_IWOTH | stat.S_IRGRP | stat.S_IWGRP)))

        cls.metadata = OrderedDict({'TESTER': {'ra': {'datatype': 'float',
                                                      'format': None,
                                                      'columns': ['ra', 'ra2']},
                                               'count': {'datatype': 'int',
                                                         'format': None,
                                                         'columns': ['count']},
                                               'rownum': {'datatype': 'rnum',
                                                          'format': None,
                                                          'columns': ['rownum']},
                                               'comment': {'datatype': 'str',
                                                           'format': None,
                                                           'columns': ['comment']}
                                               }
                                    })

    @classmethod
    def tearDownClass(cls):
        try:
            os.unlink('test.fits')
        except:
            pass
        os.unlink(cls.sfile)
        MockConnection.destroy()

    def test_ci_get(self):
        indict = {'HeLlo': 5,
                  'BYE': 6}
        self.assertEqual(diu.ci_get(indict, 'hello'), 5)
        self.assertEqual(diu.ci_get(indict, 'bye'), 6)
        self.assertEqual(diu.ci_get(indict, 'HELLO'), 5)
        self.assertIsNone(diu.ci_get(indict, 'hellobye'))

    def test_print_node(self):
        indict = {'hello': {'good': 1,
                            'bye': 2},
                  'now': 10}
        output = ''
        with capture_output() as (out, _):
            diu.print_node(indict, 0, sys.stdout)
            output = out.getvalue().strip()
            for key, val in indict.items():
                if isinstance(val, dict):
                    self.assertTrue('<' + key + '>' in output)
                    self.assertTrue('</' + key + '>' in output)
                    for k, v in val.items():
                        self.assertTrue(k + '=' + str(v) in output)
                else:
                    self.assertTrue(key + '=' + str(val) in output)
            self.assertIsNone(re.search('\tnow', output))

        with capture_output() as (out, _):
            diu.print_node(indict, 1, sys.stdout)
            output = out.getvalue().strip()
            for key, val in indict.items():
                if isinstance(val, dict):
                    self.assertTrue('<' + key + '>' in output)
                    self.assertTrue('</' + key + '>' in output)
                    for k, v in val.items():
                        self.assertTrue(k + '=' + str(v) in output)
                else:
                    self.assertTrue(key + '=' + str(val) in output)
            self.assertIsNotNone(re.search(r'\tnow', output))

    def test_ingest_datfile_contents_errors(self):
        data = OrderedDict({'TEST_OBJECTS': {'image_name': {'datatype': 'char',
                                                            'format': None,
                                                            'columns': ['name']},
                                             'date': {'datatype': 'date',
                                                      'format': 'YYYY-MM-DD',
                                                      'columns': ['date']},
                                             'ra': {'datatype': 'double',
                                                    'format': None,
                                                    'columns': ['ra_j2000']},
                                             'count': {'datatype': 'int',
                                                       'format': None,
                                                       'columns': ['count']},
                                             'background': {'datatype': 'float',
                                                            'format': None,
                                                            'columns': ['background']},
                                             'date2': {'datatype': 'date',
                                                       'format': 'YYYY-MM-DD',
                                                       'columns': ['end_date']},
                                             'release_date': {'datatype': 'date',
                                                              'format': 'YYYY-MM-DD HH:MI:SS',
                                                              'columns': ['release']}
                                             }
                            })
        with capture_output() as (_, err):
            self.assertRaises(SystemExit, diu.ingest_datafile_contents, '', '', '', data, {}, None)
            output = err.getvalue().strip()
            self.assertTrue('ERROR' in output)

    def test_ingest_datafile_contents(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        cur = dbh.cursor()
        res = cur.execute(f"select count(*) from {self.table:s} where filename='test.fits'")
        self.assertEqual(res.fetchall()[0][0], 0)
        self.assertFalse(diu.is_ingested('test.fits', self.table, dbh))
        data = {'TESTER': make_fits().data}
        res = diu.ingest_datafile_contents('test.fits', 'test-ingest', self.table, self.metadata,
                                           data, dbh)
        self.assertEqual(res, 1000)
        self.assertTrue(diu.is_ingested('test.fits', self.table, dbh))
        res = cur.execute(f"select count(*) from {self.table:s} where filename='test.fits'")
        self.assertEqual(res.fetchall()[0][0], 1000)

        data = make_fits(lastcol=False).data
        res = diu.ingest_datafile_contents('test2.fits', 'test-ingest', self.table, self.metadata,
                                           {'TESTER': data}, dbh)
        res = cur.execute(f"select count(*) from {self.table:s} where filename='test2.fits'")
        self.assertEqual(res.fetchall()[0][0], 1000)

        res = diu.ingest_datafile_contents('test3.fits', 'test-ingest', self.table, {'TESTER':{}},
                                           {'TESTER': {}}, dbh)
        res = cur.execute(f"select count(*) from {self.table:s} where filename='test3.fits'")
        self.assertEqual(res.fetchall()[0][0], 0)

        res = diu.ingest_datafile_contents('test4.fits', 'test-ingest', self.table, self.metadata,
                                           {'TESTER': {}}, dbh)
        res = cur.execute(f"select count(*) from {self.table:s} where filename='test4.fits'")
        self.assertEqual(res.fetchall()[0][0], 1)

    def test_array_ingest(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        data = {'TESTER': [{'count':[0,2,3,4,5],
                            'ra': np.array([1.22345]),
                            'comment': 'None'},
                           {'count':[554],
                            'ra': np.array([]),
                            'comment': 'hello'}]}
        res = diu.ingest_datafile_contents('test5.fits', 'fits', self.table, self.metadata, data, dbh)
        self.assertTrue(diu.is_ingested('test5.fits', self.table, dbh))

    def test_get_fits_data(self):
        write_fits()
        data = diu.get_fits_data('test.fits', 'TESTER')
        self.assertEqual(1000, len(data['TESTER']))
        data1 = diu.get_fits_data('test.fits', 1)
        self.assertTrue(np.array_equal(data['TESTER'], data1[1]))

    def test_ingest_main(self):
        #print(make_xml())
        try:
            dbh = desdbi.DesDbi(self.sfile, 'db-test')
            open('test.xml', 'w').write(make_xml())
            numrows = diu.datafile_ingest_main(dbh, 'xml', 'test.xml', self.table, self.metadata)
            self.assertEqual(numrows, 1000)

            self.assertRaises(ValueError, diu.datafile_ingest_main, dbh, '', '', '', {'a':0, 'b':1})
        finally:
            try:
                os.unlink('test.xml')
            except:
                pass


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

    def test_resolveDbObject(self):
        dbh = MagicMock()
        dbh.cursor = MagicMock()
        dbh.cursor.return_value.execute = MagicMock(return_value=(('test', 'testtable'),))
        res = ingutil.IngestUtils.resolveDbObject('testtable', dbh)
        self.assertEqual(res[0], 'test')

        dbh.cursor.return_value.execute = MagicMock(return_value=())
        res = ingutil.IngestUtils.resolveDbObject('testtable2', dbh)
        self.assertIsNone(res[0])

        res = ingutil.IngestUtils.resolveDbObject('test.testtable', None)
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0], 'test')


class Testobjectcatalog(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sfile = 'services.ini'
        cls.table = 'DATAFILE_INGEST_TEST'
        write_fits()
        open(cls.sfile, 'w').write("""

[db-test]
USER    =   Minimal_user
PASSWD  =   Minimal_passwd
name    =   Minimal_name
sid     =   Minimal_sid
server  =   Minimal_server
type    =   test
port    =   0
""")
        os.chmod(cls.sfile, (0xffff & ~(stat.S_IROTH | stat.S_IWOTH | stat.S_IRGRP | stat.S_IWGRP)))

    @classmethod
    def tearDownClass(cls):
        try:
            os.unlink('test.fits')
        except:
            pass
        os.unlink(cls.sfile)
        MockConnection.destroy()

    def test_init(self):
        cat = ojc.ObjectCatalog(12345, 'fits_test', 'test.fits', None, self.table, 'TESTER', False,
                                'services.ini', 'db-test')
        self.assertTrue(cat.constDict)

        with patch('databaseapps.objectcatalog.ingestutils.resolveDbObject', side_effect=[('PROD', 'SE_OBJECT'), ('test','test_temp')]):
            with capture_output() as (out, _):
                cat = ojc.ObjectCatalog(12345, 'fits_test', 'test.fits', 'test_temp', self.table, 0, True,
                                        'services.ini', 'db-test')
                self.assertFalse(cat.constDict)
                output = out.getvalue().strip()
                self.assertTrue('test_temp' in output)
                self.assertTrue('PROD' in output)

    def test_getObjectColumns(self):
        self.assertRaises(SystemExit, ojc.ObjectCatalog, 12345, 'bad_test', 'test.fits', 'test_temp', self.table, 0, True,
                                        'services.ini', 'db-test')

        cat = ojc.ObjectCatalog(12345, 'fits_test2', 'test.fits', None, self.table, 'TESTER', False,
                                'services.ini', 'db-test')
        self.assertTrue(cat.dbDict)

    def test_checkForArrays(self):
        cat = ojc.ObjectCatalog(12345, 'fits_test', 'test.fits', None, self.table, 'TESTER', False,
                                'services.ini', 'db-test')
        cat.objhdu = 'blah'
        initial = copy.deepcopy(cat.dbDict)

        cat.checkForArrays(cat.dbDict)
        self.assertDictEqual(initial, cat.dbDict)

        test_dict = OrderedDict({'blah': {'a_3': [['temp'], 'h', 'float_external', ['0']],
                              'RA': [['ra'], 'h', 'float_external', ['0']],
                              'RA_1': [['ra'], 'h', 'float external', ['0']]},
                     })
        initial = copy.deepcopy(test_dict)
        cat.checkForArrays(test_dict)
        self.assertNotEqual(test_dict, initial)

    def test_info(self):
        cat = ojc.ObjectCatalog(12345, 'fits_test', 'test.fits', None, self.table, 'TESTER', False,
                                'services.ini', 'db-test')
        with capture_output() as (out, _):
            cat.info('MY TEST MESSAGE')
            output = out.getvalue().strip()
            self.assertTrue(output.endswith('TEST MESSAGE'))

    def test_getConstValuesFromHeader(self):
        cat = ojc.ObjectCatalog(12345, 'fits_test', 'test.fits', None, self.table, 'TESTER', False,
                                'services.ini', 'db-test')
        length = len(cat.constlist)
        cat.getConstValuesFromHeader('TESTER')
        self.assertNotEqual(len(cat.constlist), length)

    def test_loadingTarget(self):
        cat = ojc.ObjectCatalog(12345, 'fits_test', 'test.fits', None, self.table, 'TESTER', False,
                                'services.ini', 'db-test')
        self.assertFalse(cat.loadingTarget())
        cat.temptable = 'SE_OBJECT'
        self.assertTrue(cat.loadingTarget())
        cat.tempschema = 'test'
        self.assertFalse(cat.loadingTarget())

    def test_parseFitsTypeLength(self):
        formats = {'count': 'J',
                   'ra': 'E',
                   'comment': '2A',
                   'naxis': '3J'}
        cat = ojc.ObjectCatalog(12345, 'fits_test', 'test.fits', None, self.table, 'TESTER', False,
                                'services.ini', 'db-test')
        sizes, types = cat.parseFitsTypeLength(formats)
        self.assertEqual(sizes['naxis'], 3)
        self.assertEqual(sizes['ra'], 1)
        self.assertEqual(types['naxis'], 'J')
        self.assertEqual(types['ra'], 'E')

    def test_insert_many(self):
        cat = ojc.ObjectCatalog(12345, 'fits_test', 'test.fits', None, self.table, 'TESTER', False,
                                'services.ini', 'db-test')

        cat.insert_many('', [], {})

        rows = [('fname', 12.34, 2, 0)]
        columns = ['filename', 'ra', 'count', 'rownum']

        cat.insert_many('DATAFILE_INGEST_TEST', columns, rows)

    def test_numAlreadyIngested(self):
        cat = ojc.ObjectCatalog(12345, 'fits_test', 'test.fits', None, self.table, 'TESTER', False,
                                'services.ini', 'db-test')
        cat.targetschema = "'" + cat.targetschema
        cat.targettable += "'"
        self.assertEqual(cat.numAlreadyIngested()[0], 4)

        cat.shortfilename = 'test2.fits'
        self.assertEqual(cat.numAlreadyIngested()[0], 0)

        cat.targetschema += 'x'
        self.assertRaises(Exception, cat.numAlreadyIngested)


class TestCoaddCatalog(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sfile = 'services.ini'
        cls.table = 'DATAFILE_INGEST_TEST'
        write_fits()
        open(cls.sfile, 'w').write("""

[db-test]
USER    =   Minimal_user
PASSWD  =   Minimal_passwd
name    =   Minimal_name
sid     =   Minimal_sid
server  =   Minimal_server
type    =   test
port    =   0
""")
        os.chmod(cls.sfile, (0xffff & ~(stat.S_IROTH | stat.S_IWOTH | stat.S_IRGRP | stat.S_IWGRP)))

    @classmethod
    def tearDownClass(cls):
        try:
            os.unlink('test.fits')
        except:
            pass
        os.unlink(cls.sfile)
        MockConnection.destroy()


    def test_setCatalogInfo_corner(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')

        self.assertRaises(SystemExit, ccol.CoaddCatalog, ingesttype='band', filetype='cat_firstcut', datafile='/var/lib/jenkins/test_data/D00526157_r_c01_r3463p01_red-fullcat.fits', idDict={}, dbh=dbh)

        cur = dbh.cursor()
        cur.execute("insert into catalog (filename, filetype, band, tilename, pfw_attempt_id) values ('D00526157_r_c01_r3463p01_red-fullcat.fits', 'cat_firstcut', NULL, NULL, 123)")
        self.assertRaises(SystemExit, ccol.CoaddCatalog, ingesttype='band', filetype='cat_firstcut', datafile='/var/lib/jenkins/test_data/D00526157_r_c01_r3463p01_red-fullcat.fits', idDict={}, dbh=dbh)
        cur.execute("update catalog set band='r' where pfw_attempt_id=123")
        self.assertRaises(SystemExit, ccol.CoaddCatalog, ingesttype='band', filetype='cat_firstcut', datafile='/var/lib/jenkins/test_data/D00526157_r_c01_r3463p01_red-fullcat.fits', idDict={}, dbh=dbh)

    def test_retrieveCoaddObjectIds(self):
        os.environ['DES_SERVICES'] = self.sfile
        os.environ['DES_DB_SECTION'] = 'db-test'

        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        cur = dbh.cursor()
        cur.execute("insert into catalog (filename, filetype, band, tilename, pfw_attempt_id) values ('D00526157_r_c01_r3463p01_red-fullcat.fits', 'cat_firstcut', NULL, NULL, 123)")
        cur.execute("update catalog set tilename='abc' where pfw_attempt_id=123")
        ci = ccol.CoaddCatalog(ingesttype='band', filetype='cat_firstcut', datafile='/var/lib/jenkins/test_data/D00526157_r_c01_r3463p01_red-fullcat.fits', idDict={}, dbh=dbh)
        self.assertFalse(ci.idDict)
        cur.execute("insert into COADD_OBJECT_TEST (id, filename, object_number, band, tilename, pfw_attempt_id) values(123, 'D00526157_r_c01_r3463p01_red-fullcat.fits', 1234, 'r', 'acbd', 56789, 12345)")
        ci.retrieveCoaddObjectIds(pfwid=12345, table='COADD_OBJECT_TEST')
        self.assertTrue(ci.idDict)


if __name__ == '__main__':
    unittest.main()
