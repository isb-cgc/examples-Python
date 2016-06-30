import argparse
import httplib2
import json
import requests
import sys
import time

from apiclient import discovery
from googleapiclient.errors import HttpError
from oauth2client.client import GoogleCredentials

#------------------------------------------------------------------------------

def buildUpLocalContext ( r, seqContext, pos, width ):

    # print json.dumps ( r, indent=4 )

    # print " looping over alignments objects : ", len(r['alignments'])
    for a in r['alignments']:
        if ( 0 ):
            print " "
            print " "
            print json.dumps ( a, indent=4 )
            print "     duplicateFragment  : ", a['duplicateFragment']
            print "     nextMatePosition   : ", a['nextMatePosition']
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

    # print seqContext
    return ( seqContext )

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

    for aPatient in mySeqData:
        for aSample in mySeqData[aPatient]:
            for aSeq in mySeqData[aPatient][aSample]:
                aCount = mySeqData[aPatient][aSample][aSeq]
                if ( aCount >= minCount ):
                    if aSeq not in contextD:
                        contextD[aSeq] = [ (aCount, aPatient, aSample) ]
                    else:
                        contextD[aSeq] += [ (aCount, aPatient, aSample) ]

    for aSeq in contextD:
        ## print aSeq, contextD[aSeq]
        sampleList = []
        patientList = []
        totNumR = 0
        for aTuple in contextD[aSeq]:
            totNumR += aTuple[0]
            aPatient = aTuple[1]
            aSample = aTuple[2]
            if ( aPatient not in patientList ): patientList += [ aPatient ]
            if ( aSample not in sampleList ): sampleList += [ aSample ]
        print aSeq, " seen in %5d reads, %4d patients, %5d samples " % ( totNumR, len(patientList), len(sampleList) )


#------------------------------------------------------------------------------

def ParseHTTPerrorString ( errStr ):

    tokenList = errStr.split(' ')
    print "     HttpError code : ", tokenList[1]
    
    ii = errStr.find("json returned ")
    print "     Reason : ", errStr[ii+15:-2]

#------------------------------------------------------------------------------
# In this demonstration script, we will use both the API service and the
# Genomics service to access data.

def main ( args ):

    if ( args.verbose ): print " calling get_application_default ... "
    credentials = GoogleCredentials.get_application_default()

    if ( 0 ):
        if ( args.verbose ): print " creating http object ... "
        http = httplib2.Http()
        if ( args.verbose ): print " authorizing credentials ... "
        http = credentials.authorize(http)

    # and then we will use a similar 'discovery' process to build a service to
    # access Google Genomics using the GA4GH API
    try:
        ggSvc = discovery.build('genomics', 'v1', credentials=credentials)
        if ( args.verbose ): print " successfully built Genomics service ... "
    except:
        print " ERROR: failed to build genomics service "
        sys.exit(-1)

    # this is the dictionary we will use to accumulate the sequence data
    mySeqData = {}

    # the next bits are hard-coded for a specific patient, samples from that
    # patient, and Genomics readgroupset ids ... the next version of this sample
    # code will use the ISB-CGC API to query for this type of information
    aPatient = "TCGA-E2-A15K"
    sampleList = [ "TCGA-E2-A15K-01A", "TCGA-E2-A15K-06A", "TCGA-E2-A15K-10A", "TCGA-E2-A15K-11A" ]

    rgsIds = {}
    rgsIds["TCGA-E2-A15K-01A" ] = [ "CN3p5__bCBDTurCq3cas1qEB", "CN3p5__bCBD006vtorPArRg", "CN3p5__bCBCa9vvEueTK5h4", "CN3p5__bCBDB2eiOqrf3y_EB", "CN3p5__bCBCquIfGmJm21h8" ]
    rgsIds["TCGA-E2-A15K-06A" ] = [ "CN3p5__bCBDRp6DXj_uf72I", "CN3p5__bCBCM6qHHrPiOnqUB", "CN3p5__bCBDtz96R09inr2o", "CN3p5__bCBDiopmS87nmmboB", "CN3p5__bCBDv2-7zmbfYi2w" ]
    rgsIds["TCGA-E2-A15K-10A" ] = [ "CN3p5__bCBCojNGOtJulnBQ", "CN3p5__bCBCYg5uc_rqnhmE", "CN3p5__bCBD25siRnILJpT8" ]
    rgsIds["TCGA-E2-A15K-11A" ] = [ "CN3p5__bCBDUlOm7uqP9sVs", "CN3p5__bCBDg-53Utdv5_yI", "CN3p5__bCBDDtrid7qOKv9gB", "CN3p5__bCBCeoKKVxoiXrZYB", "CN3p5__bCBCwqvff8unO38gB" ]

    # and now we can start querying the reads data ... 
    # first we create an entry in 'mySeqData' for this patient
    mySeqData[aPatient] = {}

    # next loop is over samples associated with this patient
    for aSample in sampleList:

        if ( aSample not in rgsIds ): continue

        # next create an entry in 'mySeqData' for this sample
        mySeqData[aPatient][aSample] = {}

        # and loop over the readgroupset ids for this sample ...
        for aRGS in rgsIds[aSample]:

            # we're going to be building up a dictionary of 
            # sequence contexts around the position of interest
            seqContext = {}

            readGroupSetId = aRGS
            body = { "readGroupSetIds": [readGroupSetId], 
                     "referenceName": args.chr,
                     "start": args.pos-2,
                     "end": args.pos+2,
                     "pageSize": 256 }

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
                        print " calling ggSvc.reads() ... "
                        r = ggSvc.reads().search ( body=body ).execute()
                        print r
                        # print len(r['alignments'])
                        numTot += len(r['alignments'])
                        seqContext = buildUpLocalContext ( r, seqContext, args.pos, args.width )

                    except:
                        print " ERROR ??? genomics reads search (with nextpageToken) call failed "
                        print json.dumps ( body, indent=4 )
                        print " "

                if ( args.verbose ): print " --> numTot = ", numTot

                # save this seqContext information for later
                mySeqData[aPatient][aSample] = seqContext

            except HttpError as err:

                print " "
                print " ERROR returned from genomics service request: "
                ParseHTTPerrorString ( str(err) )
                print " "


    # now that we have collected all of the sequence data that we
    # were interested in, lets look through it and compile it
    analyzeSeqData ( mySeqData )

#------------------------------------------------------------------------------
# We'll just wrap the call to main() with same time-checks so we can see
# how long this program takes to run.
#
# The particular patient hard-coded into this example has a PIK3CA mutation
# to find it, invoke like this:
#
#     python ./query_tcga_reads_short.py -c 3 -p 178952084 -w 11
# or  python ./query_tcga_reads_short.py -c chr3 -p 178952084 -w 11
#
# note that the UNC RNA-seq data expects the name to be "chr3" while the 
# other readgroupsets are expecting just "3"


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Look at TCGA DNA-seq and RNA-seq data")
    parser.add_argument ( "-v", "--verbose", action="store_true" )
    parser.add_argument ( "-c", "--chr", type=str, help="chromosome (eg 7 rather than ch7 or chr7)", \
                dest='chr', required=True, default='7' )
    parser.add_argument ( "-p", "--pos", type=int, help="base position (0-based)", \
                dest='pos', required=True, default=140453136 )
    parser.add_argument ( "-w", "--width", type=int, help="sequence context width (default=11)", \
                dest='width', default=11 )
    args = parser.parse_args()

    print " "
    print " "
    print " Looking at TCGA data using GA4GH API at %s:%d " % ( args.chr, args.pos )
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
