"""
    Mangle Ingestion
"""
import sys
import traceback
from databaseapps.Ingest import Ingest
from despymisc import miscutils

class Mangle(Ingest):
    """ Class to ingest the outputs from a Mangle run

    """
    def __init__(self, datafile, filetype, idDict, dbh, replacecol=None, checkcount=False, skipmissing=False):
        Ingest.__init__(self, filetype, datafile, "CSV", '3', dbh)
        self.hdu = "CSV"
        self.idDict = idDict
        self.coadd_id = None
        self.constants = {"FILENAME" : self.shortfilename}
        self.replacecol = replacecol
        self.checkcount = checkcount
        self.skipmissing = skipmissing

        if "COADD_OBJECT_ID" in self.dbDict[self.hdu]:
            self.coadd_id = self.dbDict[self.hdu]["COADD_OBJECT_ID"].position[0]

    def parseCSV(self, filename, types):
        """ Parse a CSV file, casting as needed into a list of lists

        """
        linecount = 0
        try:
            f = open(filename, 'r')
            lines = f.readlines()
            skip = 0
            for line in lines:
                drop = False
                linecount += 1
                tdata = line.split(",")
                if len(tdata) != len(types):
                    raise Exception("Incorrect number of columns.")
                # cast the data appropriately
                for i, d in enumerate(tdata):
                    if self.coadd_id is not None and i == self.coadd_id:
                        try:
                            tdata[i] = self.idDict[types[i](d)]
                        except KeyError:
                            se = sys.exc_info()
                            e = se[0]
                            if self.skipmissing:
                                skip += 1
                                drop = True
                            else:
                                raise
                    else:
                        tdata[i] = types[i](d)
                if self.replacecol is not None and tdata[self.replacecol] == -1:
                    tdata[self.replacecol] = None
                if not drop:
                    self.sqldata.append(tdata)
            if miscutils.fwdebug_check(10, "MANGLEINGEST_DEBUG"):
                miscutils.fwdebug_print(self.shortfilename)
                for d in self.sqldata:
                    miscutils.fwdebug_print(d)
            f.close()
            if skip > 0:
                print(f"Skipped {skip:d} items which were not found in the alternate table.")
        except:  # pragma: no cover
            se = sys.exc_info()
            e = se[0]
            tb = se[2]
            print("Exception raised:", e)
            print("Traceback: ")
            traceback.print_tb(tb)

            miscutils.fwdebug_print(f"Error in line {linecount:d} of {self.shortfilename}")
            raise

    def generateRows(self):
        """ Method to convert the input data into a list of lists

        """
        try:
            types = []
            # create a list of objects used to cast the data
            for item in self.dbDict[self.hdu].values():
                if item.dtype.upper() == "INT":
                    types.append(int)
                elif item.dtype.upper() == "FLOAT":
                    types.append(float)
                else:
                    types.append(str)
            self.parseCSV(self.fullfilename, types)
            self.orderedColumns = list(self.dbDict[self.hdu])
            if self.checkcount and len(self.idDict) != len(self.sqldata):
                self.status = 1
                miscutils.fwdebug_print(f"Incorrect number of rows in {self.shortfilename}. Count is {len(self.sqldata):d}, should be {len(self.idDict):d}")
                return 1
            return 0
        except:  # pragma: no cover
            se = sys.exc_info()
            e = se[1]
            tb = se[2]
            print("Exception raised:", e)
            print("Traceback: ")
            traceback.print_tb(tb)
            print(" ")
            self.status = 1
            return 1

    def getNumObjects(self):
        """ Get the number of objects to ingest

        """
        count = 0
        f = open(self.fullfilename, 'r')
        lines = f.readlines()
        count += len(lines)
        f.close()
        return count
