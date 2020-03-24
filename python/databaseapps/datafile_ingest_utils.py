# $Id: datafile_ingest_utils.py 40946 2015-12-02 16:16:07Z mgower $
# $Rev:: 40946                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2015-12-02 10:16:07 #$:  # Date of last commit.

"""  Functions used to ingest non-metadata from a file into a database table based upon filetype """

import sys
from astropy.io import fits
import numpy

import despymisc.miscutils as miscutils
from despymisc.xmlslurp import Xmlslurper

DI_COLUMNS = 'columns'
DI_DATATYPE = 'datatype'
DI_FORMAT = 'format'


######################################################################
def ci_get(mydict, mykey):
    """ a case-insensitive dictionary getter """
    for key, val in mydict.items():
        if mykey.lower() == key.lower():
            return val
    return None
# end ci_get

######################################################################
def print_node(indict, level, filehandle):
    """ print a node """
    leveltabs = "\t" * level
    #leveltabs = ""
    #for i in range(level):
    #    leveltabs = leveltabs + "\t"

    for key, value in indict.items():
        if isinstance(value, dict):
            print(leveltabs + "<" + str(key) + ">", file=filehandle)
            print_node(value, level+1, filehandle)
            print(leveltabs + "</" + str(key) + ">", file=filehandle)
        else:
            print(leveltabs + str(key) + "=" + str(value), file=filehandle)
# end print_node

######################################################################
def ingest_datafile_contents(sourcefile, filetype, tablename, metadata, datadict, dbh):
    """ ingest contents of a data file """
    columnlist = []
    data = []
    indata = []
    if hasattr(datadict, "keys"):
        indata.append(datadict)
    else:
        indata = datadict

    dateformat = None

    for hdu, attrdict in metadata.items():
        for attribute, cols in attrdict.items():
            for indx, colname in enumerate(cols[DI_COLUMNS]):
                columnlist.append(colname)
                # handle timestamp format; does not support multiple formats in one input file
                if cols[DI_DATATYPE] == 'date':
                    if dateformat and dateformat != cols[DI_FORMAT]:
                        sys.stderr.write(f"ERROR: Unsupported configuration for filetype={filetype}: Multiple different date formats found\n")
                        sys.exit(1)
                    dateformat = cols[DI_FORMAT]
                ###
        columnlist.append('filename')

    # handle timestamp format; does not support multiple formats in one input file
    if dateformat:     # pragma: no cover
        cur = dbh.cursor()
        cur.execute(f"ALTER SESSION SET NLS_TIMESTAMP_FORMAT = '{dateformat}'")

    for hdu, attrdict in datadict.items():
        indata = []
        if hasattr(attrdict, "keys"):
            indata.append(attrdict)
        else:
            indata = attrdict

        rownum = 0  # counter used for rnum column
        for inrow in indata:
            row = {}
            rownum += 1
            for attribute, coldata in metadata[hdu].items():
                for indx, colname in enumerate(coldata[DI_COLUMNS]):
                    attr = None
                    if isinstance(inrow, dict):
                        attr = ci_get(inrow, attribute)
                    else:
                        fitscols = indata.columns.names
                        for k in fitscols:
                            if k.lower() == attribute.lower():
                                attr = inrow.field(k)
                                break
                    if attr is not None or coldata[DI_DATATYPE] == 'rnum':
                        if isinstance(attr, numpy.ndarray):
                            attr = attr.reshape(-1).tolist()
                        if isinstance(attr, list):
                            if indx < len(attr):
                                row[colname] = attr[indx]
                            else:
                                row[colname] = None
                        else:
                            if indx == 0:
                                if coldata[DI_DATATYPE] == 'int':
                                    row[colname] = int(attr)
                                elif coldata[DI_DATATYPE] == 'float':
                                    row[colname] = float(attr)
                                elif coldata[DI_DATATYPE] == 'rnum':
                                    row[colname] = rownum
                                else:
                                    row[colname] = attr
                            else:
                                row[colname] = None
                    else:
                        row[colname] = None
            if row:
                row["filename"] = sourcefile
                data.append(row)
    if data:
        dbh.insert_many_indiv(tablename, columnlist, data)
    return len(data)
# end ingest_datafile_contents


######################################################################
def is_ingested(filename, tablename, dbh):
    """ Check whether the data for a file is already ingested """

    sqlstr = "select 1 from dual where exists(select * from {} where filename={})"
    sqlstr = sqlstr.format(tablename, dbh.get_named_bind_string('fname'))

    found = False
    curs = dbh.cursor()
    curs.execute(sqlstr, {"fname":filename})
    for _ in curs:
        found = True
    curs.close()
    return found
# end is_ingested


######################################################################
def get_fits_data(fullname, whichhdu):
    """ Get data from fits file header"""

    hdu = None
    try:
        hdu = int(whichhdu)
    except ValueError:
        hdu = str(whichhdu)

    hdulist = fits.open(fullname)
    hdr = hdulist[hdu].header

    mydict = {}
    if 'XTENSION' in hdr and hdr['XTENSION'] == 'BINTABLE':
        mydict[whichhdu] = hdulist[hdu].data
    else:
        mydict[whichhdu] = dict(hdr)

    hdulist.close()

    return mydict


######################################################################
def datafile_ingest_main(dbh, filetype, fullname, tablename, didatadefs):
    """ Control process for ingesting data from a file """

    #sections_wanted = get_sections_for_filetype(filetype, dbh)
    sections_wanted = list(didatadefs.keys())

    if 'xml' in filetype:
        datadict = Xmlslurper(fullname, sections_wanted).gettables()
    else:
        if len(sections_wanted) > 1:
            raise ValueError("Multiple hdus not yet supported\n")
        datadict = get_fits_data(fullname, sections_wanted[0])

    filename = miscutils.parse_fullname(fullname, miscutils.CU_PARSE_FILENAME)

    numrows = ingest_datafile_contents(filename, filetype,
                                       tablename, didatadefs,
                                       datadict, dbh)

    return numrows
