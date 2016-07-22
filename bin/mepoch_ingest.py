#!/usr/bin/env python

import sys
import time
from despydb import desdbi
import argparse
from databaseapps.CoaddCatalog import CoaddCatalog
from databaseapps.CoaddHealpix import CoaddHealpix
from databaseapps.Mangle import Mangle
from databaseapps.Wavg import Wavg
from databaseapps.Extinction import Extinction

def checkParam(args,param,required):
    """ Check that a parameter exists, else return None

    """
    if args[param]:
        return args[param]
    else:
        if required:
            sys.stderr.write("Missing required parameter: %s\n" % param)
        else:
            return None

def printinfo(msg):
    """ Generic print statement with time stamp

    """
    print time.strftime(CoaddCatalog.debugDateFormat) + " - " + msg

def getfilelist(file):
    """ Convert a comma separated list of items in a file into a list

    """
    files = []
    f = open(file, 'r')
    lines = f.readlines()
    for line in lines:
        files.append(line.split(","))
        files[-1][-1] = files[-1][-1].strip()
    f.close()
    return files

if __name__ == '__main__':

    # var to hold to COADD_OBJECT_ID's
    coaddObjectIdDict = {}

    retval = 0

    parser = argparse.ArgumentParser(description='Ingest coadd objects from fits catalogs')
    parser.add_argument('--bandcat_list', action='store')
    parser.add_argument('--detcat', action='store')
    parser.add_argument('--extinct', action='store')
    parser.add_argument('--extinct_band_list', action='store')
    parser.add_argument('--healpix', action='store')
    parser.add_argument('--wavg_list', action='store')
    parser.add_argument('--wavg_oclink_list', action='store')
    parser.add_argument('--ccdgon_list', action='store')
    parser.add_argument('--molygon_list', action='store')
    parser.add_argument('--molygon_ccdgon_list', action='store')
    parser.add_argument('--coadd_object_molygon_list', action='store')
    parser.add_argument('--section', '-s', help='db section in the desservices file')
    parser.add_argument('--des_services', help='desservices file')


    args, unknown_args = parser.parse_known_args()
    args = vars(args)

    bandcat = checkParam(args,'bandcat_list',True)
    detcat = checkParam(args,'detcat',True)
    extinct = checkParam(args,'extinct',True)
    extinct_band = checkParam(args, 'extinct_band_list', True)
    healpix = checkParam(args,'healpix',True)
    wavg = checkParam(args,'wavg_list',True)
    wavg_oclink = checkParam(args,'wavg_oclink_list',True)
    ccdgon = checkParam(args,'ccdgon_list',True)
    molygon = checkParam(args,'molygon_list',True)
    molygon_ccdgon = checkParam(args,'molygon_ccdgon_list',True)
    coadd_object_molygon = checkParam(args,'coadd_object_molygon_list',True)
    section = checkParam(args,'section',False)
    services = checkParam(args,'des_services',False)

    dbh = desdbi.DesDbi(services, section)
    if detcat is not None:
        try:
            detobj = CoaddCatalog(ingesttype='det', datafile=detcat, idDict=coaddObjectIdDict, dbh=dbh)
            isLoaded = detobj.isLoaded()
            if isLoaded:
                print "Det catalog already loaded, continuing"
            else:
                printinfo("Preparing to load detection catalog " + detcat)
                detobj.executeIngest()
                retval += detobj.getstatus()
                printinfo("Ingest of detection catalog " + detcat + " completed\n")
        except:
            e = sys.exc_info()[1]
            print "Exception raised:", e
            print "Attempting to continue\n"
            retval += 1

    if bandcat is not None:
        bandfiles = getfilelist(bandcat)
        for bandfile in bandfiles:
            try:
                bfile = bandfile[0]
                bandobj = CoaddCatalog(ingesttype='band', datafile=bfile, idDict=coaddObjectIdDict, dbh=dbh)
                isLoaded = bandobj.isLoaded()
                if isLoaded:
                    print "Band catalog %s already loaded, continuing" % (bfile)
                else:
                    printinfo("Preparing to load band catalog " + bfile)
                    bandobj.executeIngest()
                    retval += bandobj.getstatus()
                    printinfo("Ingest of band catalog " + bfile + " completed\n")
            except:
                e = sys.exc_info()[1]
                print "Exception raised:", e
                print "Attempting to continue\n"
                retval += 1

    # do a sanity check, as these numbers are needed for the following steps
    if len(coaddObjectIdDict) == 0:
        print "Coadd Object Dict is empty, cannot continue"
        exit(1)

    if healpix is not None:
        try:
            healobj = CoaddHealpix(datafile=healpix, idDict=coaddObjectIdDict, dbh=dbh)
            isLoaded = healobj.isLoaded()
            if isLoaded:
                print "Healpix catalog already loaded, continuing"
            else:
                printinfo("Preparing to load healpix catalog " + healpix)
                healobj.executeIngest()
                retval += healobj.getstatus()
                printinfo("Ingest of healpix catalog " + healpix + " completed\n")
        except:
            e = sys.exc_info()[1]
            print "Exception raised:", e
            print "Attempting to continue\n"
            retval += 1

    if wavg is not None:
        wavgfiles = getfilelist(wavg)
        for file, band in wavgfiles:
            try:
                wavgobj = Wavg(filetype='wavg', datafile=file, idDict=coaddObjectIdDict, band=band, dbh=dbh)
                isLoaded = wavgobj.isLoaded()
                if isLoaded:
                    print "Wavg catalog %s already loaded, continuing" % (file)
                else:
                    printinfo("Preparing to load wavg catalog " + file)
                    wavgobj.executeIngest()
                    retval += wavgobj.getstatus()
                    printinfo("Ingest of wavg catalog " + file + " completed\n")
            except:
                e = sys.exc_info()[1]
                print "Exception raised:", e
                print "Attempting to continue\n"
                retval += 1

    if wavg_oclink is not None:
        wavgfiles = getfilelist(wavg_oclink)
        for file, band in wavgfiles:
            try:
                wavgobj = Wavg(filetype='wavg_oclink', datafile=file, idDict=coaddObjectIdDict, band=band, dbh=dbh)
                isLoaded = wavgobj.isLoaded()
                if isLoaded:
                    print "Wavg_oclink catalog %s already loaded, continuing" % (file)
                else:
                    printinfo("Preparing to load wavg_oclink catalog " + file)
                    wavgobj.executeIngest()
                    retval += wavgobj.getstatus()
                    printinfo("Ingest of wavg_oclink catalog " + file + " completed\n")
            except:
                e = sys.exc_info()[1]
                print "Exception raised:", e
                print "Attempting to continue\n"
                retval += 1

    if ccdgon is not None:
        ccdfiles = getfilelist(ccdgon)
        for file in ccdfiles:
            try:
                ccdobj = Mangle(datafile=file[0], filetype='ccdgon', idDict=coaddObjectIdDict, dbh=dbh)
                isLoaded = ccdobj.isLoaded()
                if isLoaded:
                    printinfo("ccdgon catalogs already loaded, continuing")
                else:
                    printinfo("Preparing to load ccdgon file " + file[0])
                    ccdobj.executeIngest()
                    retval += ccdobj.getstatus()
                    printinfo("Ingest of ccdgon file " + file[0] + " completed\n")
            except:
                e = sys.exc_info()[1]
                print "Exception raised:", e
                print "Attempting to continue\n"
                retval += 1

    if molygon is not None:
        molyfiles = getfilelist(molygon)
        for file in molyfiles:
            try:
                molyobj = Mangle(datafile=file[0], filetype='molygon', idDict=coaddObjectIdDict, dbh=dbh)
                isLoaded = molyobj.isLoaded()
                if isLoaded:
                    printinfo("molygon catalogs already loaded, continuing")
                else:
                    printinfo("Preparing to load molygon file " + file[0])
                    molyobj.executeIngest()
                    retval += molyobj.getstatus()
                    printinfo("Ingest of molygon file " + file[0] + " completed\n")
            except:
                e = sys.exc_info()[1]
                print "Exception raised:", e
                print "Attempting to continue\n"
                retval += 1

    if molygon_ccdgon is not None:
        mcfiles = getfilelist(molygon_ccdgon)
        for file in mcfiles:
            try:
                mcobj = Mangle(datafile=file[0], filetype='molygon_ccdgon', idDict=coaddObjectIdDict, dbh=dbh)
                isLoaded = mcobj.isLoaded()
                if isLoaded:
                    printinfo("molygon_ccdgon catalogs already loaded, continuing")
                else:
                    printinfo("Preparing to load molygon_ccdgon file " + file[0])
                    mcobj.executeIngest()
                    retval += mcobj.getstatus()
                    printinfo("Ingest of molygon_ccdgon file " + file[0] + " completed\n")
            except:
                e = sys.exc_info()[1]
                print "Exception raised:", e
                print "Attempting to continue\n"
                retval += 1

    if coadd_object_molygon is not None:
        cmfiles = getfilelist(coadd_object_molygon)
        for file in cmfiles:
            try:
                cmobj = Mangle(datafile=file[0], filetype='coadd_object_molygon', idDict=coaddObjectIdDict, dbh=dbh)
                isLoaded = cmobj.isLoaded()
                if isLoaded:
                    printinfo("coadd_object_molygon catalogs already loaded, continuing")
                else:
                    printinfo("Preparing to load coadd_object_molygon file " + file[0])
                    cmobj.executeIngest()
                    retval += cmobj.getstatus()
                    printinfo("Ingest of coadd_object_molygon file " + file[0] + " completed\n")
            except:
                e = sys.exc_info()[1]
                print "Exception raised:", e
                print "Attempting to continue\n"
                retval += 1

    if extinct is not None:
        try:
            extobj = Extinction(datafile=extinct, idDict=coaddObjectIdDict, filetype='extinct_ebv', dbh=dbh)
            isLoaded = extobj.isLoaded()
            if isLoaded:
                print "Extinction catalog already loaded, continuing"
            else:
                printinfo("Preparing to load extinction catalog " + extinct)
                extobj.executeIngest()
                retval += extobj.getstatus()
                printinfo("Ingest of detection catalog " + extinct + " completed\n")
        except:
            e = sys.exc_info()[1]
            print "Exception raised:", e
            print "Attempting to continue\n"
            retval += 1

    if extinct_band is not None:
        exfiles = getfilelist(extinct_band)
        for file in exfiles:
            try:
                extobj = Extinction(datafile=file[0], idDict=coaddObjectIdDict, filetype='extinct_band', dbh=dbh)
                isLoaded = extobj.isLoaded()
                if isLoaded:
                    print "Extinction catalog already loaded, continuing"
                else:
                    printinfo("Preparing to load extinction catalog " + file[0])
                    extobj.executeIngest()
                    retval += extobj.getstatus()
                    printinfo("Ingest of detection catalog " + file[0] + " completed\n")
            except:
                e = sys.exc_info()[1]
                print "Exception raised:", e
                print "Attempting to continue\n"
                retval += 1
    exit(retval)