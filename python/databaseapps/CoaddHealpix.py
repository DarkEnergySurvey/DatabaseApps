"""
    healpix inhesgtion
"""
from databaseapps.FitsIngest import FitsIngest

class CoaddHealpix(FitsIngest):
    """ Class to ingest Coadd Healpix data

    """

    def __init__(self, filetype, datafile, idDict, dbh):
        FitsIngest.__init__(self, filetype, datafile, idDict, dbh=dbh)

        self.constants = {
            "FILENAME": self.shortfilename,
            }
