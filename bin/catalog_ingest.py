#!/usr/bin/env python3
"""
    Ingest catalogs
"""
# $Id: catalog_ingest.py 11430 2013-04-12 21:41:16Z tomashek $
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

__version__ = "$Rev: 11430 $"

import sys
import time
import argparse
from databaseapps.objectcatalog import ObjectCatalog
from databaseapps.objectcatalog import Timing

def checkParam(_args, param, required):
    """ Check whether the required arguments are present

        Parameters
        ----------
        _args : dict
            The given command line arguments

        param : str
            The parameter to check for

        requires : bool
            Whether the parameter is required or not

        Returns
        -------
        various
            The value of the parameter, or None if missing
    """
    if _args[param]:
        return _args[param]
    if required:
        sys.stderr.write(f"Missing required parameter: {param}\n")
    else:
        return None
# end checkParam

def printinfo(msg):
    """ Print out info """
    print(time.strftime(ObjectCatalog.debugDateFormat) + " - " + msg)

def main():
    """
        main ingestion code
    """
    runtime = Timing('Full ingestion')
    parser = argparse.ArgumentParser(description='Ingest objects from a fits catalog')
    parser.add_argument('-request', action='store')
    parser.add_argument('-filename', action='store')
    parser.add_argument('-filetype', action='store')
    parser.add_argument('-temptable', action='store')
    parser.add_argument('-targettable', action='store')
    parser.add_argument('-fitsheader', action='store')
    parser.add_argument('-dump', action='store')
    parser.add_argument('-section', '-s', help='db section in the desservices file')
    parser.add_argument('-des_services', help='desservices file')

    args, _ = parser.parse_known_args()
    args = vars(args)

    request = checkParam(args, 'request', True)
    filename = checkParam(args, 'filename', True)
    filetype = checkParam(args, 'filetype', True)
    temptable = checkParam(args, 'temptable', False)
    targettable = checkParam(args, 'targettable', True)
    fitsheader = checkParam(args, 'fitsheader', False)
    dump = checkParam(args, 'dump', False)
    services = checkParam(args, 'des_services', False)
    section = checkParam(args, 'section', False)

    if request is None or filename is None or filetype is None or targettable is None:
        return 1
    printinfo(runtime.report("INITIALIZE"))
    objectcat = ObjectCatalog(request=request,
                              filetype=filetype,
                              datafile=filename,
                              temptable=temptable,
                              targettable=targettable,
                              fitsheader=fitsheader,
                              dumponly=dump,
                              services=services,
                              section=section)
    printinfo(runtime.report(f"READ {str(objectcat.getNumObjects())}"))

    objectcat.createIngestTable()
    printinfo(runtime.report("CREATE"))
    objectcat.executeIngest()
    printinfo(runtime.report(f"LOAD {str(objectcat.getNumObjects())}"))
    printinfo("catalogIngest load of " + str(objectcat.getNumObjects()) + " objects from " + filename + " completed")
    printinfo(runtime.end())
    return 0

if __name__ == '__main__':
    sys.exit(main())
