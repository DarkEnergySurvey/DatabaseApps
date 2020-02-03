""" Module for ingesting coadd catalogs
"""
from databaseapps.FitsIngest import FitsIngest
from despydb import desdbi

class CoaddCatalog(FitsIngest):
    """ Class for ingesting coadd catalogs

    """
    def __init__(self, ingesttype, filetype, datafile, idDict, dbh):
        FitsIngest.__init__(self, filetype, datafile, idDict, True, dbh)

        self.catalogtable = 'CATALOG'
        self.idsequence = 'COADD_OBJECT_SEQ'

        # data retrieved from catalogtable
        self.band = None
        self.tilename = None
        self.pfw_attempt_id = None

        # grab the band, tile, and pfw_attempt_id for this file
        self.setCatalogInfo(ingesttype)

    def getIDs(self):
        """ doc
        """
        # retrieve all coadd objects ids needed for this band's ingest as
        # one list
        self.info("Grabbing block of coadd object ids from DB sequence")
        coadd_recs = self.getCoaddObjectIds(self.fits[self.objhdu].get_nrows())
        self.coadd_ids = [item[0] for item in coadd_recs]


    def setCatalogInfo(self, ingesttype):
        """ Grab info from catalog table based on the filename, and set corresponding
            class variables.
        """
        sqlstr = '''
            select band, tilename, pfw_attempt_id
            from {}
            where filename=:fname
            '''
        cursor = self.dbh.cursor()
        cursor.execute(sqlstr.format(self.catalogtable), {"fname" :self.shortfilename})
        records = cursor.fetchall()

        if records:
            (self.band, self.tilename, self.pfw_attempt_id) = records[0]

            # band won't be set for detection image, so set it to 'det'
            if ingesttype == 'det':
                self.band = 'det'
            elif self.band is None:
                sys.exit("Can't find band for file " + self.shortfilename + " in catalog table")

            if self.tilename is None:
                sys.exit("Can't find tilename for file " + self.shortfilename + " in catalog table")

            if self.pfw_attempt_id is None:
                sys.exit("Can't find pfw_attempt_id for file " + self.shortfilename + " in catalog table")
        else:
            sys.exit("File " + self.shortfilename + " missing from catalog table")
        self.constants = {"BAND": self.band,
                          "TILENAME": self.tilename,
                          "FILENAME": self.shortfilename,
                          "PFW_ATTEMPT_ID": self.pfw_attempt_id}

    def getCoaddObjectIds(self, numobjs):
        """ Get block of coadd object ids from db. Number of ids needed is passed
            in numobjs
        """
        # Oracle sql to get id block all at once
        sqlstr = '''
            select {}.nextval from dual
            connect by level < {:d}
            '''
        cursor = self.dbh.cursor()
        cursor.execute(sqlstr.format(self.idsequence, (numobjs+1)))
        records = cursor.fetchall()

        return records

    def retrieveCoaddObjectIds(self, services=None, section=None, pfwid=None, table=None):
        """ Get the coadd object id's if the data have already been ingested

        """
        if table is not None:
            self.printinfo('Getting Coadd IDs from alternate table')
            sqlstr = f"select object_number, coadd_object_id from {table} where pfw_attempt_id={pfwid}"
            tdbh = desdbi.DesDbi(services, section, retry=True)
            cursor = tdbh.cursor()
        else:
            self.printinfo("Getting Coadd IDs from database\n")
            sqlstr = f"select object_number, id from {self.targettable} where filename='{self.shortfilename}'"
            cursor = self.dbh.cursor()
        cursor.execute(sqlstr)
        records = cursor.fetchall()
        for r in records:
            if r[0] not in self.idDict:
                self.idDict[r[0]] = r[1]
