#This script generates a JSON schema for a given data file to be used with the 'bq load' command line tool.
#-------------------------------------------------------------

import sys
import string
import gzip
from dateutil.parser import parse
#-------------------------------------------------------------

#INPUT: path to local data file
#OUTPUT: JSON schema to stdout
#BigQuery data types = ['string','bytes','integer','float','boolean','record','timestamp']
#BigQuery modes = ['nullable','required','repeated'] , default is nullable
#-------------------------------------------------------------

#function to check is a given value is numeric
def isNumeric(val):
    try:
        float(val)
        return True
    except ValueError:
        return False

#--------------------------------------------------------------

#open data file to read header and 1st data row to infer data types of columns
try:
    if sys.argv[1].endswith('gz'):
        dataFile = gzip.open(sys.argv[1],"r")
    else:
        dataFile = open(sys.argv[1],"r")
except:
    print 'requires input filename as command-line parameter'
    sys.exit()

print " "
print "Parsing input file <%s>." % sys.argv[1]
print " "

#first line is expected to be the header
expectedHeader = dataFile.readline().strip().split('\t')

#if any numeric values in this first line, it is likely not a header: hence exit
if any([isNumeric(x) for x in expectedHeader]):
    print 'Numeric fields found in the first line. Perhaps the header is missing. Please check input file.'
    print expectedHeader
    sys.exit()

#if any of the header tokens are not unique, that's a problem ...
for ii in range(len(expectedHeader)):
    for jj in range(ii+1,len(expectedHeader)):
        if ( expectedHeader[ii].lower() == expectedHeader[jj].lower() ):
            print "ERROR: repeated header tokens.  Please check input file."
            print expectedHeader
            sys.exit(-1)
        if ( expectedHeader[ii] == '' ):
            print "ERROR: blank header token.  Please check input file."
            print expectedHeader
            sys.exit(-1)

#else read the first data row to infer column data types
firstDataRow = dataFile.readline().strip().split('\t')

if ( len(expectedHeader) != len(firstDataRow) ):
    print "The number of tab-separated tokens in the first row do not match? Please check file. "
    print len(expectedHeader), len(firstDataRow)
else:
    print "%d columns found in input file." % len(firstDataRow)

print " "
print " "
print "draft JSON schema : "
print " "

#print opening bracket
print '['

# the available data types are described in detail at: https://cloud.google.com/bigquery/data-types
# and include: STRING, BYTES, INTEGER, FLOAT, BOOLEAN ('true' or 'false'),
# RECORD, and TIMESTAMP
# here we will only try to infer STRING, INTEGER, FLOAT, or BOOLEAN

#loop through the columns
for index,item in enumerate(firstDataRow):

    # if we have a blank field, we can't really infer anything
    # so we'll assume string and move on ...
    if ( item == '' ):
        outStr = '    {"name": "'+expectedHeader[index]+'", "type": "string", "mode": "nullable"}'
        continue

    # the first test is for boolean
    if ( item.lower()=="true" or item.lower()=="false" ):
        outStr = '    {"name": "'+expectedHeader[index]+'", "type": "boolean", "mode": "nullable"}'
    else:
        try:
            # next try to cast it as an integer
            iVal = int(item)
            outStr = '    {"name": "'+expectedHeader[index]+'", "type": "integer", "mode": "nullable"}'
        except:
            try:
                # or a float ...
                fVal = float(item)
                outStr = '    {"name": "'+expectedHeader[index]+'", "type": "float", "mode": "nullable"}'
            except:
                try:
                    # use the dateutil parser to see if it looks like a date
                    parse(item)
                    outStr = '    {"name": "'+expectedHeader[index]+'", "type": "timestamp", "mode": "nullable"}'
                except:
                    # final catch-all is string type
                    outStr = '    {"name": "'+expectedHeader[index]+'", "type": "string", "mode": "nullable"}'
            
    if index < len(firstDataRow)-1:
        outStr+=','

    print outStr

#print closing bracket
print ']'


