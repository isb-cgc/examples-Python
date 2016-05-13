import httplib2
import json
import os
import time

from apiclient import discovery
from oauth2client.client import OAuth2WebServerFlow
from oauth2client import tools
from oauth2client.file import Storage

from api_test_service import test_preview_cohort_service

# the CLIENT_ID for the ISB-CGC site
CLIENT_ID = '907668440978-0ol0griu70qkeb6k3gnn2vipfa5mgl60.apps.googleusercontent.com'
# The google-specified 'installed application' OAuth pattern 
CLIENT_SECRET = 'To_WJH7-1V-TofhNGcEqmEYi'
# The google defined scope for authorization
EMAIL_SCOPE = 'https://www.googleapis.com/auth/userinfo.email'
# where a default token file will be stored for use by the endpoints
DEFAULT_STORAGE_FILE = os.path.join(os.path.expanduser("~"), '.isb_credentials')

#------------------------------------------------------------------------------
# This validates the credentials of the current user against the ISB-CGC site

def get_credentials():
    oauth_flow_args = ['--noauth_local_webserver']
    storage = Storage(DEFAULT_STORAGE_FILE)
    credentials = storage.get()
    if not credentials or credentials.invalid:
        flow = OAuth2WebServerFlow(CLIENT_ID, CLIENT_SECRET, EMAIL_SCOPE)
        flow.auth_uri = flow.auth_uri.rstrip('/') + '?approval_prompt=force'
        credentials = tools.run_flow(flow, storage, tools.argparser.parse_args(oauth_flow_args))
    return credentials

#------------------------------------------------------------------------------
# This is a wrapper script that will call the "delete" endpoint
# using the service object built by the apiclient discovery module.
# The cohort will no longer be available.

def test_delete_cohort_service ( service, cohort_id ):

    print " "
    print " *** calling delete endpoint *** ", cohort_id

    q = service.cohort_endpoints().cohorts().delete ( cohort_id=cohort_id )
    data = q.execute()

    return ( data )

#------------------------------------------------------------------------------
# This is a wrapper script that will call the "list" endpoint
# using the service object built by the apiclient discovery module.
# If no cohort_id is supplied, this api will list all cohorts that 
# the current user can access

def test_list_cohort_service ( service, cohort_id = None):

    print " "
    print " *** calling list endpoint *** ", cohort_id

    q = service.cohort_endpoints().cohorts().list ( cohort_id=cohort_id )
    data = q.execute()

    return ( data )

#------------------------------------------------------------------------------
# This is a wrapper script that will call the "save" endpoint using the service 
# object built by the apiclient discovery module.

def test_create_cohort_service ( service, **kwargs ):

    print " "
    print " *** calling save endpoint *** ", kwargs

    q = service.cohort_endpoints().cohorts().save_cohort( **kwargs )
    data = q.execute()

    return ( data )

#------------------------------------------------------------------------------
# This is the main part of this demonstration script.  We will walk through
# a simple example of using some of the ISB-CGC endpoints using the API service
# object built by the apiclient discovery module.  These examples require that 
# the user be authorized by the ISB-CGC site.

def main():

    # create an Http object ( http://bitworking.org/projects/httplib2/doc/html )
    http = httplib2.Http()

    # get the authorization
    credentials = get_credentials()
    http = credentials.authorize(http)
    
    if credentials.access_token_expired:
        credentials.refresh(http)
    
    # you need to know the base address of this endpoint, and the name
    # and version of this api
    site = 'api-dot-isb-cgc.appspot.com'
    api = 'cohort_api'
    version = 'v1'

    # now you can assemble the discovery url and then build the service object
    discovery_url = 'https://%s/_ah/api/discovery/v1/apis/%s/%s/rest' % ( site, api, version )
    service = discovery.build ( api, version, discoveryServiceUrl=discovery_url, http=http )

    # The "save" API allows us to search the patient and sample
    # metadata to identify a cohort of interest to save.  The ISB-CGC web-app will
    # allow you to do this interactively, but you can also do it
    # programmatically.  For example, say we are interested in a cohort
    # of patients with stomach cancer from Russia or the United States:
    payload = { 
                'name': 'example cohort',
                'body': {
                    'Project'     : 'TCGA',
                    'Study'       : 'BRCA',
                    'country'     : [
                        'Russia',
                        'United States'
                    ]
                }
              }
    data = test_create_cohort_service( service, **payload )
    print json.dumps ( data, indent=4 )
    cohort_id = data['id']
    
    # Now we can obtain the information about the cohort,
    # includingthe filters used to create it:
    data = test_list_cohort_service( service, cohort_id )
    print json.dumps ( data, indent=4 )

    # And lastly, delete the cohort:
    data = test_delete_cohort_service ( service, cohort_id )
    print json.dumps ( data, indent=4 )
    
    # The preview API can allow getting a list of samples.  These barcodes can then 
    # be passed into save to create the cohort.  We'll select patients who have 
    # Adrenocortical carcinoma and are Hispanic or Latino.  We'll specify in the fields parameter
    # to return only the patient and sample count and the patient and sample barcodes 
    payload = { 
                'Project'     : 'TCGA',
                'Study'       : 'ACC',
                'ethnicity'   : 'HISPANIC OR LATINO'
              }
    fields = 'patients, patient_count, samples, sample_count'
    data = test_preview_cohort_service(service, payload, fields)
    print json.dumps( data, indent=4 )

    # Now we'll use the returned sample barcodes to create a cohort
    payload = { 
                'name': 'example cohort from samples',
                'body': {
                    'SampleBarcode': data['samples']
                }
              }
    data = test_create_cohort_service( service, **payload )
    print json.dumps ( data, indent=4 )
    cohort_id = data['id']
    
    # Now we can obtain the information about the cohort,
    # including the filters used to create it:
    data = test_list_cohort_service( service, cohort_id )
    print json.dumps ( data, indent=4 )

    # And lastly, delete the cohort:
    data = test_delete_cohort_service ( service, cohort_id )
    print json.dumps ( data, indent=4 )

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
