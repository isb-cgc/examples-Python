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

#first line is expected to be the header
expectedHeader = dataFile.readline().strip().split('\t')

#if any numeric values in this first line, it is likely not a header: hence exit
if any([isNumeric(x) for x in expectedHeader]):
    print 'Numeric fields found in the first line. Perhaps the header is missing. Please check again'
    sys.exit()

#else read the first data row to infer column data types
firstDataRow = dataFile.readline().strip().split('\t')

#print opening bracket
print '['

#loop through the columns
for index,item in enumerate(firstDataRow):
    if item.isdigit(): #integer
        outStr = '{"name": "'+expectedHeader[index]+'", "type": "integer", "mode": "nullable"}'
    else:
        try:
            float(item)  #if successfully converted to floating point number
            outStr = '{"name": "'+expectedHeader[index]+'", "type": "float", "mode": "nullable"}'
        except:
            if item.lower()=='true' or item.lower()=='false':  #infer boolean
                outStr = '{"name": "'+expectedHeader[index]+'", "type": "boolean", "mode": "nullable"}'
            else:
                try:
                    parse(item) #datetime
                    outStr = '{"name": "'+expectedHeader[index]+'", "type": "timestamp", "mode": "nullable"}'
                except: #string is the catch-all type
                    outStr = '{"name": "'+expectedHeader[index]+'", "type": "string", "mode": "nullable"}'

    if index < len(firstDataRow)-1:
        outStr+=','

    print outStr

#print closing bracket
print ']'


