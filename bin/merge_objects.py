#!/usr/bin/env python3
"""
    Merge object tables
"""
# $Id: merge_objects.py 11430 2014-01-17 21:41:16Z tomashek $
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

__version__ = "$Rev: 11430 $"

import sys
import argparse
from despydb import desdbi

def parseTableName(inname):
    """ doc
    """
    tablename = None
    schemaname = None
    arr = inname.split('.', 1)
    if len(arr) > 1:
        schemaname, tablename = arr
    else:
        schemaname = None
        tablename = inname
    return [schemaname, tablename]

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Merge objects into main table')
    parser.add_argument('-request', action='store')
    parser.add_argument('-temptable', action='store')
    parser.add_argument('-targettable', action='store')

    args, unknown_args = parser.parse_known_args()
    args = vars(args)

    errors = []
    if args['targettable'] is None:
        errors.append("targettable is required")
    if args['temptable'] is None and args['request'] is None:
        errors.append("either temptable or request is required")
    if errors:
        sys.stderr.write("ERROR: " + "; ".join(errors) + "\n")
        exit(1)

    targetschema, targettable = parseTableName(args['targettable'])
    temptable = args['temptable']
    tempschema = None

    if temptable is None:
        temptable = f"DESSE_REQNUM{int(args['request']):07d}"
        tempschema = targetschema
    else:
        tempschema, temptable = parseTableName(args['temptable'])

    if tempschema:
        print(f"Merging {tempschema + '.' + temptable} into {args['targettable']}...")
    else:
        print(f"Merging {temptable} into {args['targettable']}...")

    dbh = desdbi.DesDbi()
    cursor = dbh.cursor()
    if targetschema is None:
        cursor.callproc("pMergeObjects", [temptable, targettable, tempschema, targetschema])
    else:
        cursor.callproc(f"{targetschema}.pMergeObjects", [temptable, targettable, tempschema, targetschema])

    cursor.close()
    print("Merge complete")
