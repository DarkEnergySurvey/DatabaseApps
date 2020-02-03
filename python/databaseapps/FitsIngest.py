"""
    Module for ingesting fits data
"""
import sys
import traceback
import math

import fitsio
import numpy as np
from databaseapps.Ingest import Ingest, Entry
from despymisc import miscutils

class FitsIngest(Ingest):
    """
        Class for ingesting fits data files
    """
    # maximum number of rows to grap from a fits table at a time
    fits_chunk = 10000

    def __init__(self, filetype, datafile, idDict, generateID=False, dbh=None, matchCount=True,
                 hdu='OBJECTS'):
        """ Base class used to ingest data from fits tables

        """
        Ingest.__init__(self, filetype, datafile, hdu, '1,2,3', dbh)

        self.fits = fitsio.FITS(datafile)

        self.idDict = idDict

        self.generateID = generateID
        self.matchCount = matchCount
        self.coadd_ids = None

    def __del__(self):
        if hasattr(self, 'fits'):
            if self.fits:
                self.fits.close()

    def getNumObjects(self):
        """ Get the number of rows to be ingested

        """
        return self.fits[self.objhdu].get_nrows()

    def generateRows(self):
        """ Convert the input fits data into a list of lists

        """
        #pylint: disable=lost-exception
        retval = 0
        lastrow = self.fits[self.objhdu].get_nrows()

        # get the column headers to ingest
        attrsToCollect = self.dbDict[self.objhdu]
        linecount = 0
        try:
            attrs = list(attrsToCollect.keys())

            # get the actual columns in the fits table
            allcols = self.fits[self.objhdu].get_colnames()

            # trim down the columns to those that are acutally in the file
            for col in allcols:
                # need NUMBER to look up COADD_OBJECT_ID
                if col.upper() in attrs or col.upper() == "NUMBER":
                    self.orderedColumns.append(col)

            # get the datatypes
            datatypes = self.fits[self.objhdu].get_rec_dtype()[0]

            startrow = 0
            endrow = 0

            # go through all the data
            while endrow < lastrow:
                startrow = endrow
                endrow = min(startrow+self.fits_chunk, lastrow)

                data = fitsio.read(self.fullfilename,
                                   rows=range(startrow, endrow),
                                   columns=self.orderedColumns, ext=self.objhdu)

                for row in data:
                    linecount += 1
                    # IMPORTANT! Must convert numpy array to python list, or
                    # suffer big performance hit. This is due to numpy bug
                    # fixed in more recent version than one in EUPS.
                    # replace NaN's with None as cx_Oracle is now stupid about
                    # NaN's
                    # this has to be done manually as np.isnan cannot operate
                    # on the given np.void object, or any nested objects
                    #nrow = np.array([row.tolist()], dtype=data.dtype)[0]
                    # have to explicitly cast to a list because tolist() on a numpy.void
                    # object returns a tuple!
                    row = list(row.tolist())
                    for i, item in enumerate(row):
                        if isinstance(item, np.ndarray):
                            row[i] = np.where(np.isnan(item), None, item)
                        elif isinstance(item, list):
                            raise Exception("Unexpected list format encountered")
                        elif not isinstance(item, (str, bytes)) and (np.isnan(item) or math.isnan(item)):
                            row[i] = None
                    #row = nrow.tolist()

                    # array to hold values for this FITS row
                    outrow = []

                    for idx in range(0, len(self.orderedColumns)):
                        # if the COADD_OBJECT_ID dictionary is being created
                        if self.generateID and self.orderedColumns[idx] == "NUMBER":
                            if row[idx] in self.idDict:
                                outrow.insert(0, self.idDict[row[idx]])
                            else:
                                coadd_id = self.coadd_ids.pop()
                                self.idDict[row[idx]] = coadd_id
                                outrow.insert(0, coadd_id)
                            outrow.append(row[idx])
                        # if this is NUMBER column, look up COADD_OBJECT_ID and
                        # then skip it
                        elif self.orderedColumns[idx] == "NUMBER":
                            try:
                                outrow.append(self.idDict[row[idx]])
                            except KeyError:
                                miscutils.fwdebug_print(f"ERROR: Coadd number ({row[idx]:d}) specified that does not have a corresponding coadd id, found in row {linecount:d}.")
                                return 1

                        # if this column is an array of values
                        elif datatypes[self.orderedColumns[idx]].subdtype:
                            arrvals = row[idx]

                            # convert the array to a python list, and append
                            arrvals = arrvals.tolist()
                            for elem in arrvals:
                                outrow.append(elem)
                            # try +=
                        # else it is a scalar
                        else:
                            if 'S' in datatypes[self.orderedColumns[idx]].str:
                                outrow.append(row[idx].strip())
                            else:
                                outrow.append(row[idx])
                    self.sqldata.append(outrow)
        except:
            miscutils.fwdebug_print(f"Possible error in line {linecount:d} of {self.shortfilename}")
            se = sys.exc_info()
            e = se[1]
            tb = se[2]
            print("Exception raised: ", e)
            print("Traceback: ")
            traceback.print_tb(tb)
            print(" ")
            self.status = 1
            retval = 1
        finally:
            if self.generateID:
                self.dbDict[self.objhdu]['ID'] = Entry(column_name='ID', position=0)
                self.orderedColumns = ['ID'] + self.orderedColumns
            elif self.matchCount and len(self.idDict) != len(self.sqldata):
                self.status = 1
                retval = 1
                miscutils.fwdebug_print(f"Incorrect number of rows in {self.shortfilename}. Count is {len(self.sqldata):d}, should be {len(self.idDict):d}")

            return retval
