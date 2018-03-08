# -#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#
'''
Copyright 2018, Institute for Systems Biology.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''
# -#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#

from argparse import ArgumentParser
from googleapiclient.discovery import build
from oauth2client.client import OAuth2WebServerFlow
from oauth2client import tools
from oauth2client.file import Storage

import httplib2
import os
import pprint
import sys

# -#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#

# the CLIENT_ID for the ISB-CGC site
CLIENT_ID = '907668440978-0ol0griu70qkeb6k3gnn2vipfa5mgl60.apps.googleusercontent.com'
# The google-specified 'installed application' OAuth pattern
CLIENT_SECRET = 'To_WJH7-1V-TofhNGcEqmEYi'
# The google defined scope for authorization
EMAIL_SCOPE = 'https://www.googleapis.com/auth/userinfo.email'
# where a default credentials file will be stored for use by the endpoints
DEFAULT_STORAGE_FILE = os.path.join(os.path.expanduser("~"), '.isb_credentials')

# -#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#
# this API does not require the user to be authenticated ...

def get_unauthorized_service():
    api = 'isb_cgc_tcga_api'
    version = 'v3'
    site = "https://api-dot-isb-cgc.appspot.com"
    discovery_url = '%s/_ah/api/discovery/v1/apis/%s/%s/rest' % (site, api, version)
    return build(api, version, discoveryServiceUrl=discovery_url, http=httplib2.Http())

# -#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#

# the example uses the TCGA-specific endpoint but the same functionality
# exists in the TARGET and CCLE endpoints as well

def get(service, barcode):
    """
    Usage: python isb_cgc_api_v3_samples.py -e get -b TCGA-W5-AA2R-01A
    """
    data = service.samples().get(sample_barcode=barcode).execute()
    print '\nresults from samples().get()'
    pprint.pprint(data)

def cloud_storage_file_paths(service, barcode=None):
    """
    Usage: python isb_cgc_api_v3_samples.py -e cloud_storage_file_paths -b TCGA-01-0642-11A
    """
    data = service.samples().cloud_storage_file_paths(sample_barcode=barcode).execute()
    print '\nresults from samples().cloud_storage_file_paths()'
    pprint.pprint(data)

# -#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#

def print_usage():
    print " "
    print " Usage: python %s --endpoint <endpoint_name> --barcode <sample_barcode> " % sys.argv[0]
    print " "
    print " Examples: "
    print "     python %s --endpoint get --barcode TCGA-W5-AA2R-01A " % sys.argv[0]
    print "     python %s --endpoint cloud_storage_file_paths --barcode TCGA-W5-AA2R-01A " % sys.argv[0]
    print " "

# -#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#

def main():
    parser = ArgumentParser()
    parser.add_argument('--endpoint', '-e',
                        help='Name of samples endpoint to execute. '
                             'Options: get, cloud_storage_file_paths')
    parser.add_argument('--barcode', '-b',
                        help='Sample barcode. Examples: TCGA-W5-AA2R-01A, TCGA-01-0642-11A')
    args = parser.parse_args()
    if args.endpoint not in ['get', 'cloud_storage_file_paths']:
        print_usage()
        return

    service = get_unauthorized_service()
    globals()[args.endpoint](service, barcode=args.barcode)

# -#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#

if __name__ == '__main__':
    main()

# -#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#
