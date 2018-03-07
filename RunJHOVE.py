#!/usr/bin/env python

"""
    RunJhove.py: This program queries the Dryad database to get a list of
    internal_ids for data files currently in the Dryad repository. It loops 
    through the data files and for each file runs JHOVE, a file validation 
    program that reports whether a file is "well-formed" and "valid." 

    This program is designed to be run on the Dryad server. To run the
    program from the command line of the server, type the following:
    python RunJHOVE.py
    
"""

__author__      = "Debra Fagan with functions written by Daisie Huang"

import sys
import getopt
import csv
from   subprocess import Popen, PIPE
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
    Makes SQL call to the collection2item table and returns a list of dicts: each dict entry has a file_name and an internal id.
    '''
    sql = "select b.internal_id, b.name from collection2item cti left join item2bundle itb on (cti.item_id = itb.item_id) left join bundle2bitstream btb on (btb.bundle_id = itb.bundle_id) left join bitstream b on (btb.bitstream_id = b.bitstream_id) where cti.collection_id = '1'"
    return dict_from_query(sql)
    
    '''
    Makes SQL call to the bitstream table and returns a dict with the internal id and the bitstream format id.
    '''
def getbformat(int_id):
    int_id_str = str(int_id)
    print "internal id: "
    print int_id_str
    sql = "select internal_id, bitstream_format_id from bitstream where internal_id = '\"%s\"'" % int_id_str
    return dict_from_query(sql)    


    '''
    Based on the bitstream_format_id stored in the Dryad database, run JHOVE to analyze the file formats supported. When a file is not of a JHOVE-supported format,  run JHOVE without specifying the format.
    '''
def runjhovecommand(bformat, output_file_location, file_location):
    if bformat == 16:
        handle = Popen(['/home/dfdryad/jhove/jhove', '-m', 'aiff-hul', '-o', output_file_location, file_location], stdout=PIPE, stderr=PIPE)
    elif bformat == 2 or bformat == 5 or bformat == 7 or bformat == 38 or bformat == 39:
        handle = Popen(['/home/dfdryad/jhove/jhove', '-m', 'aiff-hul', '-o', output_file_location, file_location], stdout=PIPE, stderr=PIPE)
    elif bformat == 13:
        handle = Popen(['/home/dfdryad/jhove/jhove', '-m', 'gif-hul', '-o', output_file_location, file_location], stdout=PIPE, stderr=PIPE)
    elif bformat == 6:
        handle = Popen(['/home/dfdryad/jhove/jhove', '-m', 'html-hul', '-o', output_file_location, file_location], stdout=PIPE, stderr=PIPE)
    elif bformat == 12:
        handle = Popen(['/home/dfdryad/jhove/jhove', '-m', 'jpeg-hul', '-o', output_file_location, file_location], stdout=PIPE, stderr=PIPE)
    elif bformat == 3:
        handle = Popen(['/home/dfdryad/jhove/jhove', '-m', 'pdf-hul', '-o', output_file_location, file_location], stdout=PIPE, stderr=PIPE)
    elif bformat == 15:
        handle = Popen(['/home/dfdryad/jhove/jhove', '-m', 'tiff-hul', '-o', output_file_location, file_location], stdout=PIPE, stderr=PIPE)
    elif bformat == 4:
        handle = Popen(['/home/dfdryad/jhove/jhove', '-m', 'xml-hul', '-o', output_file_location, file_location], stdout=PIPE, stderr=PIPE)
    else:
        handle = Popen(['/home/dfdryad/jhove/jhove', '-o', output_file_location, file_location], stdout=PIPE, stderr=PIPE)
    out, err = handle.communicate()
    return err




    '''
    Add the name of the file being analyzed to the output file.
    '''
def line_prepender(filename, line):
    with open(filename, 'r+') as f:
        content = f.read()
        f.seek(0, 0)
        f.write(line.rstrip('\r\n') + '\n' + content)
    f.close()



def main(argv=None):
    if argv is None:
        argv = sys.argv
    try:

        int_id_dict_list = get_int_id_dict()

        cntr = 0
        out_file_name=str(cntr)

        # Loop through the list of dictionaries and process each entry using the file name and the internal id.
        # Output for each entry goes to a file with a unique numerical name that is generated incrementally during processing.
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

            # Generates a unique numerical name for each output file. The loc_seq variable holds the path.
            loc_seq=("/home/dfdryad/jhove_output_files", out_file_name)
            cntr=cntr+1
            loc_string="/"
            out_file_name=str(cntr)
            output_file_location=loc_string.join(loc_seq)
            print "===================================================="

            #Write info for the current file bing processed to screen to let user know the progress of the script
            sys.stdout.write("Writing output to: ")
            print output_file_location

            # Get a dictionary containing the format of the current file
            bformatdict = getbformat(the_internal_id)
            # Retrieve the bitstream's format id from the dictionary and use it to run JHOVE
            bformat = bformatdict[0]
            bformatid = bformat['bitstream_format_id']
            anerr = runjhovecommand(bformat['bitstream_format_id'], output_file_location, file_location)            

            #Write the name of the file being processed to the output file
            loc_string=" "
            loc_seq=("File Name:", the_file_name)
            output_line=loc_string.join(loc_seq)
                        
            line_prepender(output_file_location, output_line)


    except  Usage, err:
        print >>sys.stderr, err.msg
        print >>sys.stderr, "for help use --help"
        return 2


if __name__ == "__main__":
    sys.exit(main())


