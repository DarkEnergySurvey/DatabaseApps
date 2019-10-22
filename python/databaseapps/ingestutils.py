"""
    Utility class methods
"""

import calendar

class IngestUtils(object):
    """ Class of static untility methods
    """
    @staticmethod
    def getShortFilename(longname):
        """ Get the short name of a file (e.g. remove the path part)

            Parameters
            ----------
            longname : str
                The full path to the file

            Returns
            -------
            str
                The file name

        """
        shortname = None
        if '/' in longname:
            idx = longname.rfind('/') + 1
            shortname = longname[idx:]
        else:
            shortname = longname
        return shortname.strip()
    # end getShortFilename

    @staticmethod
    def isInteger(s):
        """ Determine if the input is an integers (or can be cast to one)

            Parameters
            ----------
            s : various
                The item to test

            Returns
            -------
            bool
                Whether or not the input is an integer (or can be cast to one)

        """
        try:
            int(s)
            return True
        except ValueError:
            return False
    # end isInteger

    @staticmethod
    def func_getnite(dateobs):
        """ Get the night of observation from the input date

            Parameters
            ----------
            dateobs : str
                A string representation of the date

            Returns
            -------
            str
                The nite
        """
        v = dateobs.split(':')
        hh = int(v[0].split('-')[2][-2:])
        if hh > 14:
            nite = v[0][:-3].replace('-', '')
        else:
            y = int(v[0][0:4])
            m = int(v[0][5:7])
            d = int(v[0][8:10])-1
            if d == 0:
                m = m - 1
                if m == 0:
                    m = 12
                    y = y - 1
                d = calendar.monthrange(y, m)[1]
            nite = str(y).zfill(4) + str(m).zfill(2) + str(d).zfill(2)
        return nite
    # end func_getnite

    @staticmethod
    def func_getband(flter):
        """ Determine the band from the filter input

            Parameters
            ----------
            fltr : str
                The filter

            Returns
            -------
            str
                The band.
        """
        band = flter[0]
        if band not in ['u', 'g', 'r', 'i', 'z', 'Y']:
            raise KeyError("filter yields an invalid band")
        return band
    # end func_getband

    @staticmethod
    def resolveDbObject(objectname, dbh):
        """ Given an object name and an open DB handle, this routine returns
            the schema that owns the object and the object name

            Parameters
            ----------
            objectname : str
                The name of the object to find

            dbh : handle
                The database handle to use

            Returns
            -------
            tuple
                The schema name and the object name
        """
        obname = None
        schema = None
        arr = objectname.split('.')
        if len(arr) > 1:
            schema = arr[0]
            obname = arr[1]
        else:
            sqlstmt = '''
                select USER, table_name, 0 preference from user_tables where table_name=:obj
                UNION
                select USER, index_name, 1 from user_indexes where index_name=:obj
                UNION
                select table_owner, synonym_name, 2 from user_synonyms where synonym_name=:obj
                UNION
                select table_owner, synonym_name, 3 from all_synonyms where owner='PUBLIC' and synonym_name=:obj
                order by 3 '''
            cursor = dbh.cursor()
            res = cursor.execute(sqlstmt, {'obj': objectname})
            for rec in res:
                schema = rec[0]
                obname = rec[1]
                break
            cursor.close()
        return (schema, obname)
