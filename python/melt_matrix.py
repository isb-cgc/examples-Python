import argparse
import sys

## =============================================================================

def melt_matrix ( inputFilename, tidyFilename, \
                  nHeadRows, nHeadCols, separator ):

    try:
        inFh = file ( inputFilename, 'r' )
    except:
        print " ERROR: failed to open input file ??? "
        print " <%s> " % inputFilename
        sys.exit(-1)

    try:
        outFh = file ( tidyFilename, 'w' )
    except:
        print " ERROR: failed to open output file ??? "
        print " <%s> " % tidyFilename
        sys.exit(-1)

    # start by reading the header row(s)
    if ( nHeadRows > 0 ):
        numTok = -1
        hdrRows = [0] * nHeadRows
        for ii in range(nHeadRows):
            aLine = inFh.readline()
            tokenList = aLine.split(separator)
            for kk in range(len(tokenList)):
                tokenList[kk] = tokenList[kk].strip()
            ## print tokenList
            hdrRows[ii] = tokenList
            if ( numTok < 0 ):
                numTok = len(tokenList)
            else:
                if ( numTok != len(tokenList) ):
                    print " ERROR: inconsistent number of tokens ??? ", numTok, len(tokenList)
                    print "        please check input file "
                    sys.exit(-1)
            for jj in range(nHeadCols):
                if ( tokenList[jj] != '' ):
                    print " WARNING: non-blank token in column %d will be ignored (%s)." % ( jj, tokenList[jj] )

        ## and then construct the output headerTokens
        outColNames = [0] * (numTok - nHeadCols)
        for ii in range(numTok-nHeadCols):
            outColNames[ii] = hdrRows[0][ii+nHeadCols]
            for kk in range(1,nHeadRows):
                if ( hdrRows[kk][ii+nHeadCols] != '' ):
                    outColNames[ii] += "_%s" % hdrRows[kk][ii+nHeadCols]

    print " "
    print " output column labels : "
    print outColNames
    print " "

    # now we can write out a 'dummy' header row for the output file
    tidyRow = 'row_label\tcol_label\tvalue'
    outFh.write ( "%s\n" % tidyRow )

    # now read the rest of the file

    done = 0
    lineNo = 1
    while not done:
        aLine = inFh.readline()

        ## check if we've gotten to the end of the file ...
        if ( len(aLine.strip()) < 1 ):
            done = 1
            continue

        print "     handling data row #%d ... " % lineNo

        tokenList = aLine.split(separator)
        if ( numTok != len(tokenList) ):
            print " ERROR: inconsistent number of tokens ??? ", numTok, len(tokenList)
            print "        please check input file "
            sys.exit(-1)
        for kk in range(len(tokenList)):
            tokenList[kk] = tokenList[kk].strip()
        ## print tokenList

        ## first concatenate the header column(s) to create a 'column name'
        columnName = tokenList[0]
        for kk in range(1,nHeadCols):
            if ( tokenList[kk] != '' ):
                columnName += "_%s" % tokenList[kk]
        ## print columnName

        ## and now work our way through the "data" values ...
        for kk in range(nHeadCols,numTok):
            ## print tokenList[kk]
            tidyRow = ''
            tidyRow = outColNames[kk-nHeadCols]
            tidyRow += '\t' + columnName
            tidyRow += '\t' + tokenList[kk]
            ## print tidyRow
            ## print " "

            outFh.write ( "%s\n" % tidyRow )

        lineNo += 1
            
    print " "
    print " DONE ! "
    print " "

    outFh.close()
    inFh.close()

## =============================================================================

if __name__ == '__main__':

    parser = argparse.ArgumentParser ( description='Melt 2D data matrix into a tidy-format file.' )
    parser.add_argument ( '--inputFilename', '-f', action='store', required=True, type=str )
    parser.add_argument ( '--tidyFilename', '-t', action='store', required=True, type=str )
    parser.add_argument ( '--nHeadRows', '-n', action='store', default='1', type=int )
    parser.add_argument ( '--nHeadCols', '-m', action='store', default='1', type=int )
    parser.add_argument ( '--separator', '-s', action='store', default='\t', type=str )

    args = parser.parse_args ( )
    print args

    if ( args.nHeadRows < 1 ):
        print " ERROR: your input matrix must have at least one row (the first) containing column labels. "
        sys.exit(-1)

    if ( args.nHeadCols < 1 ):
        print " ERROR: your input matrix must have at least one column (the first) containing a row label. "
        sys.exit(-1)

    melt_matrix ( args.inputFilename, args.tidyFilename, \
                  args.nHeadRows, args.nHeadCols, \
                  args.separator )

## =============================================================================
