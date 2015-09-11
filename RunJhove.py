#!/usr/bin/env python

"""
    RunJhove.py: This program queries the Dryad database to get a list of
    internal_ids for data files currently in the Dryad repository. It loops 
    through the data files and for each file runs JHOVE, a file validation 
    program that reports whether a file is "well-formed" and "valid." 
    
"""

__author__      = "Debra Fagan with functions written by Daisie Huang"

import sys
import getopt
import csv
from subprocess import Popen, PIPE
import os
import commands

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg


def dict_from_query(sql):
    '''
    # Execute the query passed in and return the results
    '''    
    cmd = "psql -A -U dryad_app dryad_repo -c \"%s\"" % sql
    output = [line.strip().split('|') for line in os.popen(cmd).readlines()]
    if len(output) == 1:
        return None
    else:
	resultkeys = output.pop(0)
        count = output.pop()[0]
        result = []
        for entry in output:
            result.append(dict(zip(resultkeys,entry)))
        return result


def get_int_id_dict():
    '''
    Returns a list of dicts: each one has a file_name and an internal id.
    '''
    sql = "select b.internal_id, b.name from collection2item cti left join item2bundle itb on (cti.item_id = itb.item_id) left join bundle2bitstream btb on (btb.bundle_id = itb.bundle_id) left join bitstream b on (btb.bitstream_id = b.bitstream_id) where cti.collection_id = '1'"
    return dict_from_query(sql)



def main(argv=None):
    if argv is None:
        argv = sys.argv
    try:

        int_id_dict_list = get_int_id_dict()

        cntr = 0
        out_file_name=str(cntr)

        for x in int_id_dict_list:

            print "===================================================="
            print "%s\t%s" % (x['internal_id'], x['name'])
            the_file_name = x['name']
            the_internal_id = x['internal_id']
            
            # Construct the full path plus file name for each item
            dir1 = the_internal_id[0:2]
            dir2 = the_internal_id[2:4]
            dir3 = the_internal_id[4:6]
            loc_string="/"
            loc_seq=("/opt/dryad/assetstore", dir1, dir2, dir3, the_internal_id)
            file_location=loc_string.join(loc_seq)

            # Use cntr to generate a unique file name for each output file 
            cntr=cntr+1
            loc_string="/"
            out_file_name=str(cntr)
            loc_seq=("/home/dfdryad/jhove_output_files", out_file_name)
            output_file_location=loc_string.join(loc_seq)
            print "===================================================="

            sys.stdout.write("Writing output to: ")
            print output_file_location

            # Run JHOVE
            handle = Popen(['/home/dfdryad/jhove/jhove', '-o', output_file_location, file_location], stdout=PIPE, stderr=PIPE)
            out, err = handle.communicate()

            sys.stdout.write(">>>Error(s): ")
            print err
            sys.stdout.write("Return Code: ")
            print handle.returncode
            print "\n\n"

            with open(output_file_location, "a") as f1:
                f1.write("\n")
                f1.write("File Name: ")
                f1.write(the_file_name)
                f1.write("\n\n")
            f1.close()



    except  Usage, err:
        print >>sys.stderr, err.msg
        print >>sys.stderr, "for help use --help"
        return 2


if __name__ == "__main__":
    sys.exit(main())

