## =============================================================================
## This script reads a flat, delimited text file containing a 'matrix' of 
## some sort and 'melts' it into a 'tidy' format file.
## =============================================================================

import argparse
import sys

## =============================================================================

def melt_matrix ( inputFilename, tidyFilename, \
                  nSkipRows, nHeadRows, nHeadCols, 
                  separator, \
                  mergeRowLabels, mergeColLabels, \
                  ultraMelt ):

    print " in melt_matrix ... "
    print inputFilename
    print tidyFilename
    print nSkipRows, nHeadRows, nHeadCols
    print separator
    print mergeRowLabels, mergeColLabels, ultraMelt
    print " "
    print " "

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

    # start by skipping any leading rows ...
    if ( nSkipRows > 0 ):
        for ii in range(nSkipRows):
            aLine = inFh.readline()
            print " skipping this : ", aLine

    # start by reading the header row(s)
    if ( nHeadRows > 0 ):
        numTok = -1
        hdrRows = [0] * nHeadRows
        for ii in range(nHeadRows):
            aLine = inFh.readline()
            if ( aLine[-1] == '\n' ): aLine = aLine[:-1]
            print " --> got aLine : <%s> " % aLine
            print " separator : <%s> " % separator, len(separator)
            tokenList = aLine.split(separator)
            print " --> got tokenList : ", len(tokenList), tokenList
            for kk in range(len(tokenList)):
                tokenList[kk] = tokenList[kk].strip()
            print len(tokenList), tokenList
            hdrRows[ii] = tokenList
            if ( numTok < 0 ):
                numTok = len(tokenList)
            else:
                if ( numTok != len(tokenList) ):
                    print " ERROR: inconsistent number of tokens ??? ", numTok, len(tokenList)
                    print "        please check input file "
                    sys.exit(-1)
            ## DELETE for jj in range(nHeadCols):
            ## DELETE    if ( tokenList[jj] != '' ):
            ## DELETE        print " WARNING: non-blank token in column %d will be ignored (%s)." % ( jj, tokenList[jj] )

        print " "
        print " hdrRows: "
        print hdrRows

        ## and then construct the output headerTokens
        ## exactly how these will be constructed depends on whether the row-labels 
        ## and the column-labels are being merged, and also whether or not ultraMelt is ON

        if ( mergeRowLabels ):
            numOutCols = 1
        else:
            numOutCols = nHeadCols

        if ( ultraMelt ):
            numOutCols += 2
        else:
            numOutCols += (numTok - nHeadCols)
        print " numOutCols : ", numOutCols

        outColNames = [0] * numOutCols

        if ( mergeRowLabels ):
            tmpLabel = ''
            for ii in range(nHeadRows):
                print ' ii=%d ' % ii
                for kk in range(nHeadCols):
                    print '     kk=%d ' % kk
                    if ( tmpLabel != '' ): tmpLabel += '_'
                    tmpLabel += hdrRows[ii][kk]
            outColNames[0] = tmpLabel
            oo = 1
        else:
            for kk in range(nHeadCols):
                tmpLabel = ''
                for ii in range(nHeadRows):
                    if ( tmpLabel != '' ): tmpLabel += '_'
                    tmpLabel += hdrRows[ii][kk]
                outColNames[kk] = tmpLabel
            oo = nHeadCols

        ## we will need to construct the dataLabel values from the column headers
        ## no matter what ...
        dataLabels = [0] * (numTok-nHeadCols)
        for kk in range(nHeadCols,numTok):
            tmpLabel = ''
            for ii in range(nHeadRows):
                if ( tmpLabel != '' ): tmpLabel += '_'
                tmpLabel += hdrRows[ii][kk]
            dataLabels[kk-nHeadCols] = tmpLabel

        if ( ultraMelt ):
            outColNames[oo] = "dataLabel"
            outColNames[oo+1] = "dataValue"
        else:
            for kk in range(nHeadCols,numTok):
                outColNames[oo+kk-nHeadCols] = dataLabels[kk-nHeadCols]

    print " "
    print " output column labels : "
    print outColNames
    print " "

    # now we can write out a 'dummy' header row for the output file
    tidyRow = '\t'.join(outColNames)
    print " <%s> " % tidyRow
    outFh.write ( "%s\n" % tidyRow )


    #
    # now read the rest of the file
    #

    done = 0
    lineNo = 1
    while not done:
        aLine = inFh.readline()

        ## check if we've gotten to the end of the file ...
        if ( len(aLine.strip()) < 1 ):
            done = 1
            continue

        if ( lineNo%1000 == 1 ): print "     handling data row #%d ... " % lineNo

        if ( aLine[-1] == '\n' ): aLine = aLine[:-1]
        tokenList = aLine.split(separator)
        if ( numTok != len(tokenList) ):
            print " ERROR: inconsistent number of tokens ??? ", numTok, len(tokenList)
            print "        please check input file "
            sys.exit(-1)
        for kk in range(len(tokenList)):
            tokenList[kk] = tokenList[kk].strip()
        ## print tokenList

        ## figure out how many output rows per input row ...
        if ( ultraMelt ):
            nOut = numTok - nHeadCols
        else:
            nOut = 1

        ## initialize the ouput values ...
        outVals = [0] * nOut
        for nn in range(nOut):
            outVals[nn] = [0] * numOutCols

        ## first decide what to do with the header column(s)
        if ( mergeRowLabels ):
            tmpLabel = ''
            for kk in range(nHeadCols):
                if ( tmpLabel != '' ): tmpLabel += '_'
                tmpLabel += tokenList[kk]
            for nn in range(nOut):
                outVals[nn][0] = tmpLabel
            oo = 1
        else:
            for kk in range(nHeadCols):
                for nn in range(nOut):
                    outVals[nn][kk] = tokenList[kk]
            oo = nHeadCols

        ## and then decide what to do with the rest of the tokens
        if ( ultraMelt ):
            ## in this case, we will write out several rows for each input row
            for nn in range(nOut):
                outVals[nn][oo] = dataLabels[nn]
                outVals[nn][oo+1] = tokenList[nHeadCols+nn]
        else:
            for kk in range(nHeadCols,numTok):
                outVals[0][oo+kk-nHeadCols] = tokenList[kk]

        for nn in range(nOut):
            ## print outVals[nn]
            tidyRow = '\t'.join(outVals[nn])
            outFh.write ( "%s\n" % tidyRow )

        lineNo += 1
            
    print " "
    print " DONE!  %d data rows processed " % lineNo
    print " "

    outFh.close()
    inFh.close()

## =============================================================================

def str2bool(v):
    ## print " in str2bool ... <%s> " % v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

## =============================================================================

if __name__ == '__main__':

    parser = argparse.ArgumentParser ( description='Melt 2D data matrix into a tidy-format file.' )
    parser.add_argument ( '--inputFilename',  '-f',  action='store', required=True, type=str )
    parser.add_argument ( '--tidyFilename',   '-t',  action='store', required=True, type=str )
    parser.add_argument ( '--nSkipRows',      '-k',  action='store', default=0,     type=int )
    parser.add_argument ( '--nHeadRows',      '-n',  action='store', default=1,     type=int )
    parser.add_argument ( '--nHeadCols',      '-m',  action='store', default=1,     type=int )
    parser.add_argument ( '--separator',      '-s',  action='store', default='\t',  type=str )
    parser.add_argument ( '--mergeRowLabels', '-mr', action='store', default=False, type=str2bool )
    parser.add_argument ( '--mergeColLabels', '-mc', action='store', default=True,  type=str2bool )
    parser.add_argument ( '--ultraMelt',      '-u',  action='store', default=False, type=str2bool )

    args = parser.parse_args ( )
    ## print args

    if ( args.nHeadRows < 1 ):
        print " ERROR: your input matrix must have at least one row (the first) containing column labels. "
        sys.exit(-1)

    if ( args.nHeadCols < 1 ):
        print " ERROR: your input matrix must have at least one column (the first) containing a row label. "
        sys.exit(-1)

    if ( args.nHeadRows>1 and (not args.mergeColLabels) ):
        print args.nHeadRows, args.mergeColLabels
        print (args.nHeadRows>1)
        print (not args.mergeColLabels)
        print " ERROR: if you have more than one header row, the column labels must be merged to produce one label per output column. "
        sys.exit(-1)

    if ( args.separator == '\\t' ): 
        args.separator = chr(9)
    elif ( args.separator == ',' ):
        args.separator = ','
    else:
        print " ERROR: unknown args.separator ... <%s> " % args.separator
        sys.exit(-1)

    melt_matrix ( args.inputFilename, args.tidyFilename, \
                  args.nSkipRows, args.nHeadRows, args.nHeadCols, \
                  args.separator, \
                  args.mergeRowLabels, args.mergeColLabels, \
                  args.ultraMelt )

## =============================================================================
