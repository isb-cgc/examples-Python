import argparse
import json
import sys
import time

from apiclient import discovery
from oauth2client.client import GoogleCredentials

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
# BRAF V600E mutation: g.chr7:140453136A>T
# according to TCGA somatic mutations table, the Ref_Context is: 
#       TCGAGATTTCACTGTAGCTAG
#                 ^          
#              TTCACTG
# becomes      TTCTCTG

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
def parseSampleIDs ( sampleList ):

    dnaSamples = []
    rnaSamples = []

    for aSample in sampleList:
        if ( aSample.find("DNA") >= 0 ): dnaSamples += [ aSample ]
        if ( aSample.find("RNA") >= 0 ): rnaSamples += [ aSample ]

    return ( dnaSamples, rnaSamples )

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
# In this demonstration script, we will use both the API service and the
# Genomics service to access data.

def main ( args ):

    if ( args.verbose ): print " calling get_application_default ... "
    credentials = GoogleCredentials.get_application_default()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # first we will create the cohort API service
    # you need to know the base address of this endpoint, and the name
    # and version of this api
    site = 'api-dot-isb-cgc.appspot.com'
    api = 'isb_cgc_api'
    version = 'v2'

    # now you can assemble the discovery url and then build the service object
    discovery_url = 'https://%s/_ah/api/discovery/v1/apis/%s/%s/rest' % ( site, api, version )
    try:
        apiSvc = discovery.build(api, version, discoveryServiceUrl=discovery_url, credentials=credentials)
        if ( args.verbose ): print " successfully built API service ... "
    except:
        print " ERROR: failed to build api service "
        sys.exit(-1)

    # and then we will use a similar 'discovery' process to build a service to
    # access Google Genomics using the GA4GH API
    try:
        ggSvc = discovery.build('genomics', 'v1', credentials=credentials)
        if ( args.verbose ): print " successfully built Genomics service ... "
    except:
        print " ERROR: failed to build genomics service "
        sys.exit(-1)

    payload = { 'Project':'CCLE', 'Study':['LUSC','SKCM'] }
    # payload = { 'Project':'CCLE' }
    try:
        r = apiSvc.cohorts().preview(body=payload).execute()
        if (args.verbose):
            print " results from cohorts().preview call: "
            print json.dumps (r, indent=4)
    except:
        print " ERROR: cohort preview endpoint call failed "
        sys.exit(-1)

    try:
        patientList = r['patients']
        sampleList = r['samples']
        if (args.verbose):
            print " # of patients and # of samples: "
            print len(patientList), len(sampleList)
    except:
        print " nothing returned from cohorts preview "

    # let's have a look at some "patient details"
    if ( args.verbose ):
        print " "
        print " "
        aPatient = patientList[0]
        try:
            r = apiSvc.patients().get(patient_barcode=aPatient).execute()
            print json.dumps (r, indent=4)
        except:
            print " ERROR: patients().get endpoint call failed for patient " + aPatient
            sys.exit(-1)

        print " some patient details : "
        print "     project ....... ", r['clinical_data'].get('Project')
        print "     study ......... ", r['clinical_data'].get('Study')
        print "     patient id .... ", r['clinical_data'].get('ParticipantBarcode')
        print "     gender ........ ", r['clinical_data'].get('gender')
        print "     tumor site .... ", r['clinical_data'].get('tumor_tissue_site')
        print "     histology ..... ", r['clinical_data'].get('histological_type')
        print "     sample id(s) .. ", r['samples']

    # this is the dictionary we will use to accumulate the sequence data
    mySeqData = {}

    # outer loop is over patients
    for aPatient in patientList:

        try:
            r = apiSvc.patients().get(patient_barcode=aPatient).execute()
            if (args.verbose): print json.dumps(r, indent=4)
        except:
            print " ERROR: patient_details endpoint call failed ... continuing to next patient for the patient " + aPatient
            continue

        try:
            sampleList = r['samples']
            if (args.verbose): print " --> this patient has %d samples " % len(sampleList)
        except:
            sampleList = []
            if (args.verbose): print " --> this patient has NO samples "
            continue

        # next we're going to get additional details about each sample -- this will include
        # information about all of the data that is available for this sample
        rgsIds = {}
        numRGS = 0
        for aSample in sampleList:

            # call the sample_details endpoint
            try:
                r = apiSvc.samples().get(sample_barcode=aSample).execute()
                if (args.verbose): print json.dumps (r, indent=4)
            except:
                print " ERROR ??? samples.get endpoint call failed ... continuing anyway "
                continue

            try:
                if (int(r['data_details_count']) == 0): continue
                data_details = r['data_details']
                if (args.verbose): print " --> this sample has %d data files " % len(data_details)

                # next we will loop over all of the "data files" associated with this sample
                for aData in data_details:

                    # and in particular we want to know if this datafile is associated with
                    # a readgroupset id in Google Genomics -- if so, we will query it
                    if (aData.get('GG_readgroupset_id') not in ['', None, 'None']):
                        if (aSample not in rgsIds):
                            rgsIds[aSample] = [aData['GG_readgroupset_id']]
                            numRGS += 1
                        else:
                            rgsIds[aSample] += [aData['GG_readgroupset_id']]
                            numRGS += 1

            except:
                print " no information returned from sample_details endpoint ??? ", aSample
                if (args.verbose): print json.dumps (r, indent=4)


        # we're only go to query the reads if we have at least two readgroupsets
        # from this sample (this is an arbitrary choice, but for the CCLE data
        # this typically means that we have both DNAseq and RNAseq data)
        if (numRGS < 2):
            if (args.verbose): print " SKIPPING because number of readgroupsets for this patient (cell-line) is only %d " % numRGS
            continue
        else:
            if (args.verbose): print " Continuing with %d readgroupsets for this patient (cell-line) " % numRGS
            

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
                x
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
                            r = ggSvc.reads().search ( body=body ).execute()
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

# to look at the BRAFV600E (g.chr7:140453136A>T) mutation, for example, 
# try this:
#       python ./query_ccle_reads_v2.py  -c 7 -p 140453135 -w 5

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Look at CCLE DNA-seq and RNA-seq data")
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
    print " Looking at CCLE data using GA4GH API at %s:%d " % ( args.chr, args.pos )
    print "  (note that as-is, this script will take ~5 minutes to run)"
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
