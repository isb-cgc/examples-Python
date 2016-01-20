import sys
import httplib2
import argparse
import pprint
import isb_auth, isb_curl
from apiclient import discovery

def main(argv):
  # Parse command line flags used by the oauth2client library.

  # Acquire and store oauth token.
  credentials = isb_auth.get_credentials()
  http = httplib2.Http()
  http = credentials.authorize(http)

  # Build a service object for interacting with the API.
  api_root = 'https://mvm-dot-isb-cgc.appspot.com/_ah/api'
  api = 'cohort_api'
  version = 'v1'
  discovery_url = '%s/discovery/v1/apis/%s/%s/rest' % (api_root, api, version)

  service = discovery.build(
      api, version, discoveryServiceUrl=discovery_url, http=http)

  # list cohorts
  response = service.cohort_endpoints().cohorts().list().execute()
  pprint.pprint(response)
  
  # get patient details for a particular patient barcode
  response = service.cohort_endpoints().cohorts().patient_details(patient_barcode="TCGA-02-0043").execute()
  pprint.pprint(response)

if __name__ == '__main__':
  main(sys.argv)
