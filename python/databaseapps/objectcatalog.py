# $Id: catalog_ingest.py 11430 2013-04-12 21:41:16Z tomashek $
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.
"""
    Module for object ingestion into the database
"""

__version__ = "$Rev: 11430 $"

import time
import re
import copy
from collections import OrderedDict
import fitsio
from despydb import desdbi
from databaseapps.ingestutils import IngestUtils as ingestutils

class Timing(object):
    """ Class for timing

        Parameters
        ----------
        name : str
            The name of the timer
    """
    def __init__(self, name):
        self.start = time.time()
        self.current = time.time()
        self.name = name

    def report(self, text):
        """ Report the timing data

            Parameters
            ----------
            text : str
                Text to report with the results
        """
        txt = "TIMING: %s finished in %.2f seconds" % (text, time.time()-self.current)
        self.current = time.time()
        return txt

    def end(self):
        """ End the timer
        """
        return "TIMING: %s finished in %.2f seconds" % (self.name, time.time()-self.start)

class ObjectCatalog(object):
    """
        Class for ingesting objects -- needs to be updated
    """

    COLUMN_NAME = 0
    DERIVED = 1
    DATATYPE = 2
    POSITION = 3
    VALUE = 0
    QUOTE = 1

    dbh = None
    request = None
    fullfilename = None
    shortfilename = None
    filetype = None
    temptable = None
    tempschema = None
    targettable = None
    targetschema = None
    dump = False
    objhdu = 'LDAC_OBJECTS'

    constDict = None
    constlist = []
    funcDict = []
    dbDict = None
    fits = None
    dodebug = True
    debugDateFormat = '%Y-%m-%d %H:%M:%S'
    def __init__(self, request, filetype, datafile, temptable, targettable,
                 fitsheader, dumponly, services, section):

        self.debug("start CatalogIngest.init()")
        self.dbh = desdbi.DesDbi(services, section, retry=True)

        self.debug("opening fits file")
        self.fits = fitsio.FITS(datafile)
        self.debug("fits file opened")

        self.request = request
        self.filetype = filetype
        self.fullfilename = datafile
        self.shortfilename = ingestutils.getShortFilename(datafile)

        if fitsheader:
            if ingestutils.isInteger(fitsheader):
                self.objhdu = int(fitsheader)
            else:
                self.objhdu = fitsheader
        if dumponly:
            self.dump = True
        else:
            self.dump = False
        self.consts = []

        self.debug("start resolveDbObject() for target: %s" % targettable)
        (self.targetschema, self.targettable) = ingestutils.resolveDbObject(targettable, self.dbh)
        if not temptable:
            self.temptable = "DESSE_REQNUM%07d" % int(request)
            self.tempschema = self.targetschema
        else:
            self.debug("start resolveDbObject() for temp: %s" % temptable)
            (self.tempschema, self.temptable) = ingestutils.resolveDbObject(temptable, self.dbh)
        self.debug("target schema,table = %s, %s; temp= %s, %s" %
                   (self.targetschema, self.targettable, self.tempschema, self.temptable))

        if self.dump:
            self.constDict = {}
        else:
            self.constDict = {"FILENAME": [self.shortfilename, True],
                              "REQNUM": [request, False]
                             }
            self.constlist.append("FILENAME")
            self.constlist.append("REQNUM")
        self.debug("start getObjectColumns()")
        self.dbDict = self.getObjectColumns()
        self.debug("CatalogIngest.init() done")

    def __del__(self):
        if self.dbh:
            self.dbh.close()
        if self.fits:
            self.fits.close()

    def debug(self, msg):
        """ Print debugging messages

            Parameters
            ----------
            msg : str
                The message
        """
        if self.dodebug:
            print time.strftime(self.debugDateFormat) + " - " + msg

    def info(self, msg):
        """ Print info messages

            Parameters
            ----------
            msg : str
                The message
        """
        print time.strftime(self.debugDateFormat) + " - " + msg

    def getObjectColumns(self):
        """ Get the columns from the tables
        """
        results = OrderedDict()
        sqlstr = '''
            select hdu, UPPER(attribute_name), NVL(position,0),
                column_name, NVL(derived,'h'),
                case when datafile_datatype='int' THEN 'integer external'
                    when datafile_datatype='float' THEN 'float external'
                    when datafile_datatype='double' THEN 'decimal external'
                    when datafile_datatype='char' THEN 'char'
                end sqlldr_type
            from ops_datafile_metadata
            where filetype = :ftype
            order by 1,2,3 '''
        cursor = self.dbh.cursor()
        params = {
            'ftype':self.filetype,
            }
        cursor.execute(sqlstr, params)
        records = cursor.fetchall()
        #print records
        if not records:
            exit("No columns listed for filetype %s in ops_datafile_metadata, exiting" % (self.filetype))
        for rec in records:
            #print rec
            hdr = None
            if rec[0] is None:
                hdr = self.objhdu
            elif rec[0].upper() == 'PRIMARY':
                hdr = 0
            else:
                if ingestutils.isInteger(rec[0]):
                    hdr = int(rec[0])
                else:
                    hdr = rec[0]
            if hdr not in results:
                results[hdr] = OrderedDict()
            if rec[1] not in results[hdr]:
                results[hdr][rec[1]] = [[rec[3]], rec[4], rec[5], [str(rec[2])]]
            else:
                results[hdr][rec[1]][self.COLUMN_NAME].append(rec[3])
                results[hdr][rec[1]][self.POSITION].append(str(rec[2]))
        cursor.close()
        self.checkForArrays(results)

        return results


    def checkForArrays(self, records):
        """ Check the data for arrays
        """
        results = OrderedDict()
        pat = re.compile(r'^(.*)_(\d*)$', re.IGNORECASE)
        if self.objhdu in records:
            for k, v in records[self.objhdu].iteritems():
                attrname = None
                pos = 0
                m = pat.match(k)
                if m:
                    attrname = m.group(1)
                    pos = m.group(2)
                    if attrname not in results:
                        results[attrname] = [[k], v[1], v[2], [str(int(pos)-1)]]
                    else:
                        results[attrname][self.COLUMN_NAME].append(k)
                        results[attrname][self.POSITION].append(str(int(pos)-1))
                else:
                    results[k] = v
            records[self.objhdu] = results


    def getConstValuesFromHeader(self, hduName):
        """ Get constant values from the fits header
        """
        value = None
        quoteit = None
        hdr = self.fits[hduName].read_header()

        for attribute, dblist in self.dbDict[hduName].iteritems():
            for col in dblist[self.COLUMN_NAME]:
                if dblist[self.DERIVED] == 'c':
                    value = self.funcDict[col](hdr[attribute])
                elif dblist[self.DERIVED] == 'h':
                    value = str(hdr[attribute]).strip()
                if dblist[self.DATATYPE] == 'char':
                    quoteit = True
                else:
                    quoteit = False
                self.constDict[col] = [value, quoteit]
                self.constlist.append(col)

    def loadingTarget(self):
        """ Report if we are laoding into the target table
        """
        if self.targettable == self.temptable and self.targetschema == self.tempschema:
            return True
        else:
            return False


    def setStart(self):
        """ Set up constant values
        """
        for colname in self.constlist:
            self.consts.append(self.constDict[colname][self.VALUE])
            #    controlfile.write(colname + " CONSTANT '" + val[self.VALUE] + "',\n")
            #else:
            #    controlfile.write(colname + " CONSTANT " + str(val[self.VALUE]) + ",\n")


    def writeControlfileFooter(self, controlfile):
        """ Write the control file footer
        """
        controlfile.write(")\n")

    def parseFitsTypeLength(self, formatsByColumn):
        """ Parse fits types
        """
        colsizes = OrderedDict()
        coltypes = OrderedDict()
        for col, dtype in formatsByColumn.iteritems():
            m = re.search(r'^(\d*)(.*)$', dtype)
            if m.group(1) and m.group(2) != 'A':
                colsizes[col] = int(m.group(1))
            else:
                colsizes[col] = 1
            coltypes[col] = m.group(2)
        return [colsizes, coltypes]


    def executeIngest(self):
        """ Ingest the data
        """
        for hduName in self.dbDict.keys():
            if hduName not in [self.objhdu, 'WCL']:
                self.getConstValuesFromHeader(hduName)
        self.setStart()

        dbobjdata = self.dbDict[self.objhdu]
        orderedFitsColumns = self.fits[self.objhdu].get_colnames()
        columns = copy.deepcopy(self.constlist)

        for headerName in orderedFitsColumns:
            if headerName.upper() in dbobjdata.keys():
                for colname in dbobjdata[headerName.upper()][self.COLUMN_NAME]:
                    columns.append(colname)

        lastrow = self.fits[self.objhdu].get_nrows()
        attrsToCollect = self.dbDict[self.objhdu]


        attrs = attrsToCollect.keys()
        orderedFitsColumns = []
        allcols = self.fits[self.objhdu].get_colnames()
        for col in allcols:
            if col.upper() in attrs:
                orderedFitsColumns.append(col)
        datatypes = self.fits[self.objhdu].get_rec_dtype()[0]
        startrow = 0
        endrow = 0
        outdata = []
        while endrow < lastrow:
            startrow = endrow
            endrow = min(startrow+50000, lastrow)
            print startrow, endrow, lastrow
            data = fitsio.read(self.fullfilename,
                               rows=range(startrow, endrow),
                               columns=orderedFitsColumns,
                               ext=self.objhdu
                              )
            hdu = 'LDAC_OBJECTS'
            for row in data:
                outrow = {}
                for item, value in self.constDict.iteritems():
                    outrow[item] = value[0]
                for idx, col in enumerate(orderedFitsColumns):
                    # if this column is an array of values
                    name = col.upper()
                    if datatypes[col.upper()].subdtype:
                        for pos in attrsToCollect[col][self.POSITION]:
                            outrow[self.dbDict[hdu][name][0][int(pos)]] = str(row[idx][int(pos)])
                    # else it is a scalar
                    else:
                        outrow[self.dbDict[hdu][name][0][0]] = str(row[idx])

                # else if we are writing to a file
                outdata.append(outrow)
            # end for row in data
        # end while endrow < lastrow
        if outdata:
            self.insert_many(self.tempschema + '.' + self.temptable, columns, outdata)

    def insert_many(self, table, columns, rows):
        """ Insert many rows
        """
        if not rows:
            return
        if hasattr(rows[0], 'keys'):
            vals = ','.join([self.dbh.get_named_bind_string(c) for c in columns])
        else:
            bindStr = self.dbh.get_positional_bind_string()
            vals = ','.join([bindStr for c in columns])

        colStr = ','.join(columns)

        stmt = 'INSERT INTO %s (%s) VALUES (%s)' % (table, colStr, vals)
        #print stmt

        curs = self.dbh.cursor()
        try:
            curs.executemany(stmt, rows)
            #curs.execute('COMMIT WRITE BATCH NOWAIT')
            curs.execute('commit')
        finally:
            curs.close()


    def numAlreadyIngested(self):
        """ Get how many have already been ingested
        """
        sqlstr = '''
            select count(*), reqnum
            from %s
            where filename=:fname
            group by reqnum
            '''
        count = 0
        while count < 5:
            count += 1
            try:
                cursor = self.dbh.cursor()
                schtbl = self.targetschema + '.' + self.targettable
                cursor.execute(sqlstr % schtbl, {"fname":self.shortfilename})
                records = cursor.fetchall()

                if records:
                    return records[0]
                return [0, 0]
            except:
                if count == 5:
                    raise
                time.sleep(10)  # sleep 10 seconds and retry


    def getNumObjects(self):
        """ Get the number of objects
        """
        return self.fits[self.objhdu].get_nrows()

    def createIngestTable(self):
        """ Create the needed table
        """
        tablespace = "DESSE_REQNUM%07d_T" % int(self.request)

        cursor = self.dbh.cursor()
        self.info("Creating tablespace %s and table %s.%s if they do not already exist"
                  % (tablespace, self.tempschema, self.temptable))
        cursor.callproc("createObjectsTable", [self.temptable, tablespace, self.targettable])
        cursor.close()
        self.info("Temp table %s.%s is ready" % (self.tempschema, self.temptable))


    def isLoaded(self):
        """ See what is loaded
        """
        self.debug("starting isLoaded()")
        loaded = False
        exitcode = 0
        # short circuit the checking of loaded objects until a better query can be devised.
        return (loaded, exitcode)
        #if self.dump:
        #    self.debug("dump=True so skipping isLoaded() check")
        #else:
        #    self.debug("starting numAlreadyIngested()")
        #    (numDbObjects,dbReqnum) = self.numAlreadyIngested()
        #    self.debug("starting getNumObjects()")
        #    numCatObjects = self.getNumObjects()
        #    if numDbObjects > 0:
        #        loaded = True
        #        if numDbObjects == numCatObjects:
        #            self.info("WARNING: file " + self.fullfilename + " already ingested " +
        #                "with the same number of objects. " +
        #                "Original reqnum=" + str(dbReqnum) + ". Aborting new ingest")
        #            exitcode = 0
        #        else:
        #            errstr = ("ERROR: file " + self.fullfilename + " already ingested, but " +
        #                "the number of objects is DIFFERENT: catalog=" +
        #                str(numCatObjects) + "; DB=" + str(numDbObjects) +
        #                ", Original reqnum=" + str(dbReqnum))
        #            raise Exception(errstr)
        #            exitcode = 1
        #self.debug("finished isLoaded()")
        #return (loaded,exitcode)
