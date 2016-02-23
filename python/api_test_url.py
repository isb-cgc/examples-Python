import json
import requests
import sys
import time

#------------------------------------------------------------------------------
# This is a wrapper script that will call the "preview_cohort" endpoint
# using the Python requests module. http://docs.python-requests.org/en/master/

def test_preview_cohort_url ( payload ):

    print " "
    print " *** calling preview_cohort endpoint *** ", payload

    # assemble the url for the endpoint and the payload into a post request:
    url = 'https://api-dot-isb-cgc.appspot.com/_ah/api/cohort_api/v1/preview_cohort'
    resp = requests.post ( url, json=payload )

    # check for a 4xx client error or 4xx server error response
    resp.raise_for_status()

    # we know the response content-type is json, but we can check using
    # either one of two approaches:
    #           print resp.headers['Content-Type']
    #           print resp.headers.get('Content-Type')

    # other handy commands include:
    #          resp.status_code
    #          resp.text
    #          resp.encoding

    # extract the json content from the response using the builtin json decoder
    data = resp.json()

    # if you want to print this out in an easy-to-read-format, try this:
    ## print json.dumps ( data, indent=4 )

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

def test_patient_details_url ( payload ):

    print " "
    print " *** calling patient_details endpoint *** ", payload

    url = 'https://api-dot-isb-cgc.appspot.com/_ah/api/cohort_api/v1/patient_details'
    resp = requests.get ( url, params=payload )
    resp.raise_for_status()

    return ( resp.json() )

#------------------------------------------------------------------------------
# This is a wrapper script that will call the "sample_details" endpoint

def test_sample_details_url ( payload ):

    print " "
    print " *** calling sample_details endpoint *** ", payload

    url = 'https://api-dot-isb-cgc.appspot.com/_ah/api/cohort_api/v1/sample_details'
    resp = requests.get ( url, params=payload )
    resp.raise_for_status()

    return ( resp.json() )

#------------------------------------------------------------------------------
# This is a wrapper script that will call the "datafilenamekey_list_from_sample" endpoint

def test_datafile_list_from_sample_url ( payload ):

    print " "
    print " *** calling datafile_list_from_sample endpoint *** ", payload

    url = 'https://api-dot-isb-cgc.appspot.com/_ah/api/cohort_api/v1/datafilenamekey_list_from_sample'
    resp = requests.get ( url, params=payload )
    resp.raise_for_status()

    return ( resp.json() )

#------------------------------------------------------------------------------
# This is the main part of this demonstration script.  We will walk through
# a simple example of using some of the ISB-CGC endpoints.

def main():

    # The "preview_cohort" API allows us to search the patient and sample
    # metadata to identify a cohort of interest.  The ISB-CGC web-app will
    # allow you to do this interactively, but you can also do it 
    # programmatically.  For example, say we are interested in a cohort 
    # of deceased, female, breast cancer patients in the TCGA project:
    payload = { 'Project'     : 'TCGA', 
                'Study'       : 'BRCA',
                'gender'      : 'Female',
                'vital_status': 'Dead' }
    data = test_preview_cohort_url ( payload )
    # print json.dumps ( data, indent=4 )


    # Now we can loop over each of the patient identifiers and ask for
    # additional details about each patient:
    patientList = data['patients']
    for aPatient in patientList:

        payload = {'patient_barcode': aPatient}
        data = test_patient_details_url ( payload )
        # print json.dumps ( data, indent=4 )

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
                payload = {'sample_barcode': aSample}
                data = test_sample_details_url ( payload )
                # print json.dumps ( data, indent=4 )


                # The "sample_details" will include lots of information
                # about the biospecimen, but also about the data files
                # that describe that sample.  We could also use the
                # datafilenamekey_list_from_sample endpoint to request
                # samples from a specific platform:
                payload = {'sample_barcode': aSample,
                           'platform': 'HumanMethylation450'}
                data = test_datafile_list_from_sample_url ( payload )
                print json.dumps ( data, indent=4 )

            return

#------------------------------------------------------------------------------
# We'll just wrap the call to main() with same time-checks so we can see
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
