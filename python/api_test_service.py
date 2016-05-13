import httplib2
import json
import requests
import sys
import time

from apiclient import discovery

#------------------------------------------------------------------------------
# This is a wrapper script that will call the "preview_cohort" endpoint
# using the service object built by the apiclient discovery module.
# ( Documentation for the Python Google API Client Library can be found at
#   https://developers.google.com/api-client-library/python/start/get_started )

def test_preview_cohort_service ( service, payload, fields = None ):

    print " "
    print " *** calling preview_cohort endpoint *** ", payload

    # construct the googleapiclient.http.HttpRequest object
    q = service.cohort_endpoints().cohorts().preview_cohort( body = payload, fields = fields )

    # and execute 
    data = q.execute()

    # a cohort is a list of patients and samples -- let's find out how many
    # patients and samples we have in our cohort, and have a look at some
    # of the identifiers:
    patientList = data['patients']
    sampleList = data['samples']
    print len(patientList), patientList[:5]
    print len(sampleList), sampleList[:5]

    return ( data )

#------------------------------------------------------------------------------
# This is a wrapper script that will call the "patient_details" endpoint
# using the service object built by the apiclient discovery module.

def test_patient_details_service ( service, patient_barcode ):

    print " "
    print " *** calling patient_details endpoint *** ", patient_barcode

    q = service.cohort_endpoints().cohorts().patient_details ( patient_barcode=patient_barcode )
    data = q.execute()

    return ( data )

#------------------------------------------------------------------------------
# This is a wrapper script that will call the "sample_details" endpoint
# using the service object built by the apiclient discovery module.

def test_sample_details_service ( service, sample_barcode ):

    print " "
    print " *** calling sample_details endpoint *** ", sample_barcode

    q = service.cohort_endpoints().cohorts().sample_details ( sample_barcode=sample_barcode )
    data = q.execute()

    return ( data )

#------------------------------------------------------------------------------
# This is a wrapper script that will call the "datafilenamekey_list_from_sample" 
# endpoint # using the service object built by the apiclient discovery module.

def test_datafile_list_from_sample_service ( service, **kwargs ):

    print " "
    print " *** calling datafile_list_from_sample endpoint *** ", kwargs

    q = service.cohort_endpoints().cohorts().datafilenamekey_list_from_sample ( **kwargs )
    data = q.execute()

    return ( data )

#------------------------------------------------------------------------------
# This is the main part of this demonstration script.  We will walk through
# a simple example of using some of the ISB-CGC endpoints using the API service
# object built by the apiclient discovery module.

def main():

    # create an Http object ( http://bitworking.org/projects/httplib2/doc/html )
    http = httplib2.Http()

    # you need to know the base address of this endpoint, and the name
    # and version of this api
    site = 'api-dot-isb-cgc.appspot.com'
    api = 'cohort_api'
    version = 'v1'

    # now you can assemble the discovery url and then build the service object
    discovery_url = 'https://%s/_ah/api/discovery/v1/apis/%s/%s/rest' % ( site, api, version )
    service = discovery.build ( api, version, discoveryServiceUrl=discovery_url, http=http )

    # The "preview_cohort" API allows us to search the patient and sample
    # metadata to identify a cohort of interest.  The ISB-CGC web-app will
    # allow you to do this interactively, but you can also do it
    # programmatically.  For example, say we are interested in a cohort
    # of deceased, female LGG patients:
    payload = { 'Project'     : 'TCGA',
                'Study'       : ['LGG'],
                'gender'      : 'Female',
                'vital_status': 'Dead' }
    data = test_preview_cohort_service ( service, payload )
    print json.dumps ( data, indent=4 )

    # Note that you can specify multiple projects for the endpoint above,
    # as a list, like this:
    #          'Study'       : ['OV','BRCA','GBM','LGG'],

    # Now we can loop over each of the patient identifiers and ask for
    # additional details about each patient:
    patientList = data['patients']
    for aPatient in patientList:

        data = test_patient_details_service ( service, aPatient )
        print json.dumps ( data, indent=4 )

        # Most patients have provided two samples to the TCGA project:
        # one normal (typically blood) sample, and one tumor sample.
        # However some patients have provided 3 or more samples.  Let's
        # say we are interested in finding one of these patients:
        numSamples = len(data['samples'])
        print " numSamples = %d " % numSamples

        if ( numSamples > 2 ):

            sampleList = data['samples']
            print sampleList

            # Now let's loop over each of these samples and get
            # additional details about each sample:
            for aSample in sampleList:

                data = test_sample_details_service ( service, aSample )
                print json.dumps ( data, indent=4 )

                # The "sample_details" will include lots of information
                # about the biospecimen, but also about the data files
                # that describe that sample.  We could also use the
                # datafilenamekey_list_from_sample endpoint to request
                # samples from a specific platform:

                # note the usage of kwargs to pass a variable number of 
                # keyword arguments ( if you are not familiar with this
                # Python idiom, see 
                # http://thepythonguru.com/python-args-and-kwargs/ )
                kwargs = { 'sample_barcode': aSample, 'platform': 'HumanMethylation450' }
                data = test_datafile_list_from_sample_service ( service, **kwargs )
                print json.dumps ( data, indent=4 )

            return 

#------------------------------------------------------------------------------
# We'll just wrap the call to main() with some time-checks so we can see
# how long this program takes to run.  Note that response-times from Google
# Cloud Endpoints can vary depending on load.  Also, if the endpoint has not
# received a request in a long time, it may have gone 'cold' and may need
# to be 'warmed up' again.

if __name__ == '__main__':

    t0 = time.time() 
    main()
    t1 = time.time()

    print " "
    print " =============================================================== "
    print " "
    print " time taken in seconds : ", (t1-t0)
    print " "
    print " =============================================================== "

#------------------------------------------------------------------------------
