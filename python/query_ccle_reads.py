import argparse
import json
import sys
import time

from apiclient import discovery
from oauth2client.client import GoogleCredentials
from pprint import pprint

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
# BRAF V600E mutation: g.chr7:140453136A>T
# according to TCGA somatic mutations table, the Ref_Context is: 
#       TCGAGATTTCACTGTAGCTAG
#                 ^          
#              TTCACTG
# becomes      TTCTCTG


def buildUpLocalContext (r, seqContext, pos, width):

    ## print " in buildUpLocalContext ... "
    ## print json.dumps ( r, indent=4 )

    # print " looping over alignments objects : ", len(r['alignments'])
    for a in r['alignments']:
        if ( 0 ):
            print " "
            print " "
            print json.dumps ( a, indent=4 )
            print "     duplicateFragment  : ", a['duplicateFragment']
            print "     numberReads        : ", a['numberReads']
            print "     readNumber         : ", a['readNumber']
            print "     fragmentLength     : ", a['fragmentLength']
            print "     alignedSequence    : ", a['alignedSequence']
            print "     alignment_position : ", a['alignment']['position']

        seq = a['alignedSequence']
        seqStart = int ( a['alignment']['position']['position'] )
        revFlag = a['alignment']['position']['reverseStrand']
        relPos = pos - (width/2) - seqStart

        if ( relPos >= 0 and relPos+width<len(seq) ): 
            # print "         relative position in read  %6d  <%s> " % ( relPos, seq[relPos:relPos+11] ), revFlag
            localContext = seq[relPos:relPos+width]
            if ( localContext in seqContext ):
                seqContext[localContext] += 1
            else:
                seqContext[localContext] = 1
        else:
            # print "         relative position in read  %6d " % ( relPos )
            pass

    return ( seqContext )

#------------------------------------------------------------------------------
def parseSampleIDs ( sampleList ):

    dnrgsIds = []
    rnrgsIds = []

    for rgsId in sampleList:
        if ( rgsId.find("DNA") >= 0 ): dnrgsIds += [ rgsId ]
        if ( rgsId.find("RNA") >= 0 ): rnrgsIds += [ rgsId ]

    return ( dnrgsIds, rnrgsIds )

#------------------------------------------------------------------------------

def analyzeSeqData ( mySeqData ):

    print " "
    print " in analyzeSeqData ... "
    print " "

    numPatients = len(mySeqData)

    # the input dict is patient-oriented, but let's turn it around and
    # make it seq-context oriented instead , with a threshold that a
    # particular seq-context has to be observed at least "minCount" times 
    # in a given sample

    contextD = {}
    minCount = 10

    for rgsName in mySeqData:
        for rgsId in mySeqData[rgsName]:
            for aSeq in mySeqData[rgsName][rgsId]:
                aCount = mySeqData[rgsName][rgsId][aSeq]
                if ( aCount >= minCount ):
                    if aSeq not in contextD:
                        contextD[aSeq] = [ (aCount, rgsName, rgsId) ]
                    else:
                        contextD[aSeq] += [ (aCount, rgsName, rgsId) ]

    for aSeq in contextD:
        ## print aSeq, contextD[aSeq]
        sampleList = []
        patientList = []
        totNumR = 0
        for aTuple in contextD[aSeq]:
            totNumR += aTuple[0]
            rgsName = aTuple[1]
            rgsId = aTuple[2]
            if ( rgsName not in patientList ): patientList += [ rgsName ]
            if ( rgsId not in sampleList ): sampleList += [ rgsId ]
        print aSeq, " seen in %5d reads, %4d patients, %5d samples " % ( totNumR, len(patientList), len(sampleList) )


#------------------------------------------------------------------------------
# In this demonstration script, we will use both the API service and the
# Genomics service to access data.

def main ( args ):

    if ( args.verbose ): print " calling get_application_default ... "
    credentials = GoogleCredentials.get_application_default()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # we can use a 'discovery' process to build a service to access
    # Google Genomics using the GA4GH API:
    try:
        ggSvc = discovery.build('genomics', 'v1', credentials=credentials)
        if (args.verbose): print " successfully built Genomics service ... "
    except:
        print " ERROR: failed to build genomics service "
        sys.exit(-1)

    payload = {'Project': 'CCLE', 'Study': ['LUSC', 'SKCM']}

    # current, publicly accessible ISB-CGC Google Genomics datasets:
    # ID                   NAME
    # 1175112317461194900  ccle-dna
    # 2592944257098811032  ccle-rna

    # first, let's get the list of publicly accessible ISB-CGC genomics datasets:
    try:
        r = ggSvc.datasets().list( projectId="isb-cgc" ).execute()
    except:
        print " ERROR: failed to get datasets list "
        sys.exit(-1)
    ## print json.dumps ( r, indent=4 )
    datasetList = r['datasets']

    # create a dictionary for the sequence data we're going to be getting ...
    mySeqData = {}

    # outer loop over datasets:
    for aDataset in datasetList:
        datasetId = aDataset['id']
        if (args.verbose): print " datasetId : ", datasetId

        # get the dataset
        try:
            r = ggSvc.datasets().get( datasetId=datasetId ).execute()
        except:
            print " ERROR: failed to get information about dataset ", datasetId
            continue

        if ( 0 ):
            print " what comes back from datasets.get : "
            print json.dumps ( r, indent=4 )

        # and 'search' for the readgroupsets ...
        if ( 0 ):
            print " "
            print " now calling readgroupsets.search ... "
        body = { "datasetIds": [datasetId], 
                 "pageSize": 2048 }
        try:
            r = ggSvc.readgroupsets().search( body=body ).execute()
        except:
            print " ERROR: failed to get all readgroupsets in dataset ", datasetId
            continue

        allRGS = r['readGroupSets']

        numRGS = len(allRGS)
        if (args.verbose): print " --> number of readgroupsets : ", numRGS

        # next loop is over readgroupsets :
        for iRGS in range(len(allRGS)):

            aRGS = allRGS[iRGS]

            ## print " looping over allRGS ... "

            rgsName = aRGS['name']
            rgsFilename = aRGS['filename']
            rgsId = aRGS['id']
            if ( 0 ):
                print " "
                print "     readgroupset name     : ", rgsName
                print "     readgroupset filename : ", rgsFilename
                print "     readgroupset Id       : ", rgsId

            # create a sub-dict for this cell-line by name
            if ( rgsName not in mySeqData ):
                mySeqData[rgsName] = {}

            # create a sub-dict for this readgroupset
            mySeqData[rgsName][rgsId] = {}

            # we're going to be building up a dictionary of 
            # sequence contexts around the position of interest
            seqContext = {}

            body = { "readGroupSetIds": [rgsId], 
                     "referenceName": args.chr,
                     "start": args.pos-2,
                     "end": args.pos+2,
                     "pageSize": 2048 }

            if ( args.verbose ):
                print " body of GA4GH request: "
                print json.dumps ( body, indent=4 )

            # call the GA4GH API reads.search method
            try:
                r = ggSvc.reads().search ( body=body ).execute()

                # and parse the returns into our seqContext
                seqContext = buildUpLocalContext ( r, seqContext, args.pos, args.width )
    
                # print json.dumps ( r, indent=4 )
                # print len(r['alignments'])
                numTot = len(r['alignments'])
    
                # if we got a 'nextPageToken' back, then there are more reads...
                while ( r['nextPageToken'] ):
                    body['pageToken'] = r['nextPageToken']
                    try:
                        ## print " calling reads.search ... "
                        r = ggSvc.reads().search ( body=body ).execute()
                        ## print " dumping json : "
                        ## print json.dumps ( r, indent=4 )
                        # print len(r['alignments'])
                        numTot += len(r['alignments'])
                        seqContext = buildUpLocalContext ( r, seqContext, args.pos, args.width )

                    except:
                        print " ERROR ??? genomics reads search (with nextpageToken) call failed "
                        print json.dumps ( body, indent=4 )
                        print " "

                if ( args.verbose ): 
                    print " --> numTot = ", numTot
                    print "              ", seqContext
                    print " "
    
                # save this seqContext information for later
                mySeqData[rgsName][rgsId] = seqContext

            except:
                if ( args.verbose ):
                    print " ERROR ??? genomics reads search call failed "
                    print json.dumps ( body, indent=4 )
                    print " "


    # now that we have collected all of the sequence data that we
    # were interested in, lets look through it and compile it
    analyzeSeqData ( mySeqData )

#------------------------------------------------------------------------------
# We'll just wrap the call to main() with same time-checks so we can see
# how long this program takes to run.  Note that response-times from Google
# Cloud Endpoints can vary depending on load.  Also, if the endpoint has not
# received a request in a long time, it may have gone 'cold' and may need
# to be 'warmed up' again.

# to look at the BRAF V600E (g.chr7:140453136A>T) mutation, for example, 
# try this:
#       python ./query_ccle_reads_v2.py  -c 7 -p 140453135 -w 5

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Look at CCLE DNA-seq and RNA-seq data")
    parser.add_argument ( "-v", "--verbose", action="store_true" )
    parser.add_argument ( "-c", "--chr", type=str, help="chromosome (eg 7 rather than ch7 or chr7)", \
                dest='chr', default='7' )
    parser.add_argument ( "-p", "--pos", type=int, help="base position (0-based)", \
                dest='pos', default=140453135 )
    parser.add_argument ( "-w", "--width", type=int, help="sequence context width (default=11)", \
                dest='width', default=11 )
    args = parser.parse_args()

    print " "
    print " "
    print " Looking at CCLE data using GA4GH API at %s:%d (%d) " % ( args.chr, args.pos, args.width )

    if ( args.chr=='7' and args.pos==140453135 and args.width==11 ):
        print " As-is, and using default parameters, this script will take ~4 minutes to run "
    print " "
    print " "

    t0 = time.time() 
    main ( args )
    t1 = time.time()

    print " "
    print " =============================================================== "
    print " "
    print " time taken in seconds : ", (t1-t0)
    print " "
    print " =============================================================== "

#------------------------------------------------------------------------------
