# This script generates a JSON schema for a given data file to 
# be used with the 'bq load' command line tool.
# -------------------------------------------------------------

import copy
import gzip
import sys
##import string

# -------------------------------------------------------------

# INPUT: path to local data file
# OUTPUT: JSON schema to stdout

# BigQuery data types = ['string','bytes','integer','float','boolean','record','timestamp']
# BigQuery modes = ['nullable','required','repeated'] , default is nullable

# -------------------------------------------------------------

def readJSONschemaFromFile ( jsonSchemaFilename ):

    schemaInfo = []

    try:
        fh = file ( jsonSchemaFilename )
        for aLine in fh:
            aLine = aLine.strip()
            if ( aLine.find ( '"name"' ) >= 0 ):
                aTokens = aLine.split('"')
                bTokens = []
                for a in aTokens:
                    a = a.strip()
                    if ( a!='' and a!=',' and a!=':' ): bTokens += [ a ]

                ## ['{', 'name', 'gs_url', 'type', 'string', 'mode', 'nullable', 'description', '<add description here>', '},']
                if ( bTokens[1] == 'name' ):
                    if ( bTokens[3] == 'type' ):
                        if ( bTokens[5] == 'mode' ):
                            schemaInfo += [ [ bTokens[2], bTokens[4], bTokens[6] ] ]
                            
    except:
        pass

    print " in readJSONschemaFromFile ... ", jsonSchemaFilename
    for ii in range(len(schemaInfo)):
        print ii, schemaInfo[ii]

    return ( schemaInfo )

# -------------------------------------------------------------

def splitListString ( aString ):

    # print " in splitListString : <%s> " % aString

    aTokens = []
    if ( aString.startswith("u'") ):
        ii = 2
        while ( ii < len(aString) ):
            jj = aString.find("'",ii)
            if ( jj > ii ):
                aTokens += [ aString[ii:jj] ]
                ii = jj
            ii = aString.find("'",jj+1)
            if ( ii < 0 ): ii = len(aString)

    else:
        aTokens = aString.split(',')

    return ( aTokens )

# --------------------------------------------------------------

def translateInputRow ( dataRow, schemaInfo ):

    # print " in translateInputRow ... "
    # print dataRow
    # print len(dataRow)

    ## start the output row with an open curly brace ...
    outRow = '{'

    ## now loop over the 'tokens' in the input 'dataRow' list ...
    for ii in range(len(dataRow)):

        # print ii, dataRow[ii], schemaInfo[ii]

        ## first handle NON repeated fields ...
        if ( schemaInfo[ii][2] != 'repeated' ):
            if ( schemaInfo[ii][1] == 'string' ):
                try:
                    outRow += '"%s":"%s",' % ( schemaInfo[ii][0], dataRow[ii].strip() )
                except:
                    print " FAILED TO WRITE string ??? ", schemaInfo[ii][0], dataRow[ii].strip()
                    sys.exit(-1)
            elif ( schemaInfo[ii][1] == 'integer' ):
                try:
                    outRow += '"%s":%d,' % ( schemaInfo[ii][0], int(dataRow[ii].strip()) )
                except:
                    print " FAILED TO WRITE integer ??? ", schemaInfo[ii][0], dataRow[ii].strip()
                    sys.exit(-1)
            elif ( schemaInfo[ii][1] == 'float' ):
                try:
                    outRow += '"%s":%f,' % ( schemaInfo[ii][0], float(dataRow[ii].strip()) )
                except:
                    print " FAILED TO WRITE float ??? ", schemaInfo[ii][0], dataRow[ii].strip()
                    sys.exit(-1)
            elif ( schemaInfo[ii][1] == 'boolean' ):
                print " BOOLEAN type TO BE IMPLEMENTED ... "
                sys.exit(-1)
            elif ( schemaInfo[ii][1] == 'bytes' ):
                print " BYTES type TO BE IMPLEMENTED ... "
                sys.exit(-1)
            elif ( schemaInfo[ii][1] == 'record' ):
                print " RECORD type TO BE IMPLEMENTED ... "
                sys.exit(-1)
            elif ( schemaInfo[ii][1] == 'timestamp' ):
                print " TIMESTAMP type TO BE IMPLEMENTED ... "
                sys.exit(-1)
            
        else:

            ## now we handle a REPEATED field  ...

            ## print " handle a repeated field !!! "
            ## print schemaInfo[ii]
            ## print dataRow[ii]

            ## it might be empty ...
            if ( len(dataRow[ii]) == 0 ):
                outRow += '"%s":null,' % schemaInfo[ii][0]

            elif ( dataRow[ii][0] == '[' ):

                outRow += '"%s":[' % schemaInfo[ii][0]

                ## dTok = dataRow[ii][1:-1].split(',')
                hasSingleQ = 0
                hasDoubleQ = 0
                if ( dataRow[ii].find("'") > 0 ): hasSingleQ = 1
                if ( dataRow[ii].find('"') > 0 ): hasDoubleQ = 1
                if ( hasSingleQ and hasDoubleQ ):
                    print " FATAL ERROR ??? !!! single and double quotes ??? !!! "
                    sys.exit(-1)

                if ( hasSingleQ ):
                    ## print " Handling repeated field with single quotes ... "
                    ## print dataRow[ii]
                    dTok = dataRow[ii][1:-1].split("'")
                    ## print len(dTok), dTok
                    d2 = []
                    for d in dTok:
                        d = d.strip()
                        if ( d!='u' and d!=', u' and len(d) > 0 ): 
                            d2 += [ d ]
                    ## print len(d2), d2
                    dTok = d2
                    ## print " "
                    ## if ( len(dTok) > 2 ): sys.exit(-1)

                elif ( hasDoubleQ ):
                    print " Handling repeated field with double quotes ... "
                    print dataRow[ii]
                    sys.exit(-1)

                else:
                    dTok = dataRow[ii][1:-1].split(',')

                sTok = copy.deepcopy(dTok)
                dTok.sort()
                if ( 0 ):
                    if ( sTok[0] != dTok[0] ):
                        print " sorting changed things !!! "
                        print dTok
                        print sTok
                        sys.exit(-1)

                # print dataRow[ii]
                # print dTok
                for d in dTok:
                    d = d.strip()
                    if ( schemaInfo[ii][1] == 'string' ):
                        if ( d.startswith("u'") ): d = d[2:-1]
                        if ( d == ',' ): continue
                        outRow += '"%s",' % d

                outRow = outRow[:-1] + '],'
                # print " "
                # print outRow

            else:
                print " ------ "
                print " hmmmmmmmm ... what do I do now ??? "
                print schemaInfo[ii]
                print " <%s> " % ( dataRow[ii] )
                print " ------ "
                sys.exit(-1)

    outRow = outRow[:-1]
    outRow += '}'

    # print " "
    # print " FINAL ROW "
    # print outRow

    return ( outRow )

# --------------------------------------------------------------
# --------------------------------------------------------------

if ( len(sys.argv) != 4 ):
    print " "
    print " Usage : %s <input-file> <json-schema-file> <json-output-file> "
    sys.exit(-1)

inFilename = sys.argv[1]
jsonSchemaFilename = sys.argv[2]
outFilename = sys.argv[3]

# first we need to read in the JSON schema ...
schemaInfo = readJSONschemaFromFile ( jsonSchemaFilename )

# open data file ...
try:
    if inFilename.endswith('gz'):
        dataFile = gzip.open(inFilename,"r")
    else:
        dataFile = open(inFilename,"r")
except:
    print 'requires input filename as command-line parameter'
    if ( len(inFilename) > 0 ):
        print ' --> failed to open <%s> ' % inFilename
    sys.exit()

print " "
print "Parsing input file <%s>." % inFilename
print " "

# first line is expected to be the header
aLine = dataFile.readline()
aLine = aLine.strip()
headerRow = aLine.split('\t')
## print headerRow

# make sure the headerRow matches the JSON schema ...
if ( len(headerRow) != len(schemaInfo) ):
    print " ERROR: number of tokens in the first row does not match input schema ... "
    sys.exit(-1)

else:
    allMatch = True
    for ii in range(len(headerRow)):
        ## print " comparing <%s> and <%s> " % ( headerRow[ii], schemaInfo[ii][0] )
        if (  headerRow[ii] != schemaInfo[ii][0] ): allMatch = False
    if ( not allMatch ):
        print " field names do not match, but that might be ok: "
        for ii in range(len(headerRow)):
            print headerRow[ii], " : ", schemaInfo[ii]

# open the output file ...
jsonFile = open(outFilename,"w")

done = 0
numRows = 0
while not done:

    # now we're going to read and 'translate' each line, one-by-one ...
    aLine = dataFile.readline()
    if ( len(aLine) == 0 ): 
        done = 1 
        continue

    try:
        if ( ord(aLine[-1]) < 32 ): aLine = aLine[:-1]
    except:
        pass

    ## print len(aLine)
    ## print " %d <%s> " % ( len(aLine), aLine )
    dataRow = aLine.split('\t')
    if ( len(dataRow) == 0 ): continue

    if ( len(dataRow) != len(schemaInfo) ):
        print " ERROR ??? # of values in data row is not as expected ??? ", len(dataRow), len(schemaInfo)
        print " "
        for ii in range(min(len(dataRow),len(schemaInfo))):
            print " %3d  %s  %s " % ( ii, schemaInfo[ii][0], dataRow[ii] )
        sys.exit(-1)

    outRow = translateInputRow ( dataRow, schemaInfo )
    jsonFile.write ( "%s\n" % outRow )

    numRows += 1
    if ( numRows % 10000 == 0 ): print numRows, " ... "

dataFile.close()
jsonFile.close()

# --------------------------------------------------------------
