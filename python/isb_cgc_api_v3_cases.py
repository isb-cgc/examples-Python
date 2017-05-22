'''
Copyright 2017, Institute for Systems Biology.

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
from argparse import ArgumentParser
from googleapiclient.discovery import build
import httplib2
import pprint
import os

# the CLIENT_ID for the ISB-CGC site
CLIENT_ID = '907668440978-0ol0griu70qkeb6k3gnn2vipfa5mgl60.apps.googleusercontent.com'
# The google-specified 'installed application' OAuth pattern
CLIENT_SECRET = 'To_WJH7-1V-TofhNGcEqmEYi'
# The google defined scope for authorization
EMAIL_SCOPE = 'https://www.googleapis.com/auth/userinfo.email'
# where a default credentials file will be stored for use by the endpoints
DEFAULT_STORAGE_FILE = os.path.join(os.path.expanduser("~"), '.isb_credentials')

def get_unauthorized_service():
    api = 'isb_cgc_tcga_api'
    version = 'v3'
    site = "https://api-dot-isb-cgc.appspot.com"
    discovery_url = '%s/_ah/api/discovery/v1/apis/%s/%s/rest' % (site, api, version)
    return build(api, version, discoveryServiceUrl=discovery_url, http=httplib2.Http())

# the API uses the isb_cgc_tcga_api endpoint but is also part of the isb_cgc_ccle_api and isb_cgc_target_api endpoints
def get(service, barcode):
    """
    Usage: python python/isb_cgc_api_v3_cases.py -b TCGA-W5-AA2R
    """
    data = service.cases().get(case_barcode=barcode).execute()
    print '\nResults from cases().get()'
    pprint.pprint(data)

def main():
    parser = ArgumentParser()
    parser.add_argument('--barcode', '-b',
                        help='Case barcode. Example: TCGA-W5-AA2R')
    args = parser.parse_args()
    service = get_unauthorized_service()
    get(service, args.barcode)

if __name__ == '__main__':
    main()
