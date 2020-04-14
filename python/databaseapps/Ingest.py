"""
    Base class for data ingestion
"""
import time
import traceback
import sys
import collections
from databaseapps.ingestutils import IngestUtils as ingestutils
from despymisc import miscutils
from despydb import desdbi

class Ingest:
    """ General base class for ingesting data into the database

        Parameters
        ----------
        filetype : str
            The file type being ingested

        datafile : str
            The name of the file being ingested

        hdu : various, optional
            The HDU being ingested, default is None (not specifried)

        order : str, optional
            Any columns to order query results by. Default is None

        dbh : handle, optional
            The database handle to use. The default None makes the code
            create its own handle.
    """
    _debug = True
    debugDateFormat = '%Y-%m-%d %H:%M:%S'

    def __init__(self, filetype, datafile, hdu=None, order=None, dbh=None):
        self.objhdu = hdu
        if dbh is None:
            self.dbh = desdbi.DesDbi()
        else:
            self.dbh = dbh
        self.cursor = self.dbh.cursor()
        # get the table name that is being filled, based on the input data type
        self.cursor.execute(f"select table_name from ops_datafile_table where filetype='{filetype}'")
        self.targettable = self.cursor.fetchall()[0][0]
        self.filetype = filetype
        self.idColumn = None
        self.order = order
        self.fileColumn = None
        self.constants = {}
        self.orderedColumns = []
        self.sqldata = []
        self.fullfilename = datafile
        self.shortfilename = ingestutils.getShortFilename(datafile)
        self.status = 0

        # dictionary of table columns in db
        self.dbDict = self.getObjectColumns()

    def getstatus(self):
        """ Returns the status

            Returns
            -------
            int
        """
        return self.status

    def debug(self, msg):
        """ Generates a debug message, if debugging is turned on

            Parameters
            ----------
            msg : str
                The message to display in stdout
        """
        if self._debug:
            print(time.strftime(self.debugDateFormat) + " - " + msg)

    def info(self, msg):
        """ Generates an info message.

            Parameters
            ----------
            msg : str
                The message to display in stdout
        """
        print(time.strftime(self.debugDateFormat) + " - " + msg)

    def getObjectColumns(self):
        """ Get the database columns that are being filled, and their data type

        """
        results = {}
        sqlstr = f"select hdu, UPPER(attribute_name), position, column_name, datafile_datatype from ops_datafile_metadata where filetype = '{self.filetype}'"
        if self.order is not None:
            sqlstr += f" order by {self.order}"
        cursor = self.dbh.cursor()
        cursor.execute(sqlstr)
        records = cursor.fetchall()

        for rec in records:
            hdr = None
            if rec[0] is None:
                hdr = self.objhdu
            elif ingestutils.isInteger(rec[0]):
                hdr = int(rec[0])
            elif rec[0].upper() == 'PRIMARY':
                hdr = 0
            else:
                hdr = rec[0]
            if hdr not in results:
                results[hdr] = collections.OrderedDict()
            if rec[1] not in results[hdr]:
                results[hdr][rec[1]] = Entry(hdu=hdr, attribute_name=rec[1], position=rec[2], column_name=rec[3], dtype=rec[4])
            else:
                results[hdr][rec[1]].append(rec[3], rec[2])
        cursor.close()
        return results

    def getNumObjects(self):
        """ Get the number of items to be ingested, must be overloaded by child classes

        """
        return 0

    def generateRows(self):
        """ convert the input data into a list of lists for ingestion into the database
            must be overloaded by child classes to handle individual data types

        """
        return None

    def numAlreadyIngested(self):
        """ Determine the number of entries already ingested from the data source

        """
        num = 0
        while num < 5:
            num += 1
            try:
                sqlstr = f"select count(*) from {self.targettable} where filename='{self.shortfilename}'"
                cursor = self.dbh.cursor()
                cursor.execute(sqlstr)
                count = cursor.fetchone()[0]

                return count
            except:
                if num == 5:
                    raise
                time.sleep(10)  # sleep 10 seconds and retry


    def isLoaded(self):
        """ Determine if the data have already been loaded into the database,
            based on file name

            Returns
            -------
            bool
        """
        loaded = False

        numDbObjects = self.numAlreadyIngested()
        numCatObjects = self.getNumObjects()
        if numDbObjects > 0:
            loaded = True
            if numDbObjects == numCatObjects:
                self.info("INFO: file " + self.fullfilename +
                          " already ingested with the same number of" +
                          " objects. Skipping.")
            else:   # pragma: no cover
                miscutils.fwdebug_print("ERROR: file " + self.fullfilename +
                                        " already ingested, but the number of objects is" +
                                        " DIFFERENT: catalog=" + str(numCatObjects) +
                                        "; DB=" + str(numDbObjects) + ".")

        return loaded

    def executeIngest(self):
        """ Generic method to insert the data into the database

        """
        #pylint: disable=lost-exception
        if self.generateRows() == 1:
            return 1
        for k, v in self.constants.items():
            if isinstance(v, str):
                self.constants[k] = "'" + v + "'"
            else:
                self.constants[k] = str(v)
        columns = []
        for att in self.orderedColumns:
            columns += self.dbDict[self.objhdu][att].column_name
        places = []
        for i in range(len(columns)):
            places.append(f":{i + 1:d}")
        sqlstr = f"insert into {self.targettable} ( "
        sqlstr += ', '.join(list(self.constants.keys()) + columns)
        sqlstr += ") values ("
        sqlstr += ', '.join(list(self.constants.values()) + places)
        sqlstr += ")"
        cursor = self.dbh.cursor()
        cursor.prepare(sqlstr)
        offset = 0
        try:
            while offset < len(self.sqldata):
                chunk = min(1000000, len(self.sqldata) - offset)
                cursor.executemany(None, self.sqldata[offset:offset + chunk])
                offset += chunk
            cursor.close()
            self.dbh.commit()
            self.info(f"Inserted {len(self.sqldata):d} rows into table {self.targettable}")
            self.status = 0
        except:   # pragma: no cover
            se = sys.exc_info()
            e = str(se[1])
            tb = se[2]
            print("Exception raised: ", e.strip(), " while ingesting ", self.shortfilename)
            print("Traceback: ")
            traceback.print_tb(tb)
            print(" ")
            self.dbh.rollback()
            self.status = 1
        finally:
            return self.status

    def printinfo(self, msg):
        """ Generic print statement with time stamp

        """
        print(time.strftime(self.debugDateFormat) + " - " + msg)


class Entry:
    """ Simple light weight class to hold entries from the ops_datafile_metadata
        table

    """
    __slots__ = ["hdu", "attribute_name", "position", "column_name", "dtype"]
    def __init__(self, **kwargs):
        self.hdu = None
        self.attribute_name = None
        self.position = [0]
        self.column_name = []
        self.dtype = None

        for item in ["column_name", "position"]:
            if item in kwargs:
                setattr(self, item, [kwargs[item]])
                del kwargs[item]
        for kw, arg in kwargs.items():
            setattr(self, kw, arg)
        if len(self.position) != len(self.column_name):
            raise Exception(f"BAD MATCH {len(self.position):d}  {len(self.column_name):d}")

    def append(self, column_name, position):
        """ Method to append data to specific elements of the class

        """
        self.column_name.append(column_name)
        self.position.append(position)
