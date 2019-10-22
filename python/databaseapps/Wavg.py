"""
    Wavg ingestion
"""

import fitsio

from databaseapps.FitsIngest import FitsIngest


class Wavg(FitsIngest):
    """ Class to ingest wavg and wavg_oclink data

    """
    def __init__(self, filetype, datafile, idDict, dbh, matchCount=True):
        FitsIngest.__init__(self, filetype, datafile, idDict, dbh=dbh, matchCount=matchCount)

        header = fitsio.read_header(datafile, self.dbDict[self.objhdu]['BAND'].hdu)
        band = header['BAND'].strip()

        self.constants = {
            "BAND": band,
            "FILENAME": self.shortfilename,
        }
