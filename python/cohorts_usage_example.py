'''
Copyright 2017, Institute for Systems Biology.

This example demonstrates creating a cohort then using its id to get
information about the cohort and the BAM files associated with the
samples in the cohort.

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
from oauth2client.client import OAuth2WebServerFlow
from oauth2client import tools
from oauth2client.file import Storage
import httplib2
import pprint
import json
import os

# the CLIENT_ID for the ISB-CGC project site.  for other projects, replace the values for 
# CLIENT_ID and CLOIENT_SECRET appropriately.
CLIENT_ID = '907668440978-0ol0griu70qkeb6k3gnn2vipfa5mgl60.apps.googleusercontent.com'
# The google-specified 'installed application' OAuth pattern
CLIENT_SECRET = 'To_WJH7-1V-TofhNGcEqmEYi'
# The google defined scope for authorization
EMAIL_SCOPE = 'https://www.googleapis.com/auth/userinfo.email'
# where a default credentials file will be stored for use by the endpoints
DEFAULT_STORAGE_FILE = os.path.join(os.path.expanduser("~"), '.isb_credentials')

# ------------------------------------------------------------------------------
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

def get_authorized_service(api_tag):
    api = 'isb_cgc{}_api'.format(api_tag)
    version = 'v3'
    site = "https://api-dot-isb-cgc.appspot.com"
#    site = "https://isb-cgc-test.appspot.com"
    discovery_url = '%s/_ah/api/discovery/v1/apis/%s/%s/rest' % (site, api, version)

    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())

    if credentials.access_token_expired or credentials.invalid:
        credentials.refresh(http)

    authorized_service = build(api, version, discoveryServiceUrl=discovery_url, http=http)

    return authorized_service

# case endpoints
def case_get(service, barcode):
    """
    Usage: python python/isb_cgc_api_v3_samples.py -e get -b TCGA-W5-AA2R-01A
    """
    return service.cases().get(case_barcode=barcode).execute()

# sample endpoints
def sample_get(service, barcode):
    """
    Usage: python python/isb_cgc_api_v3_samples.py -e get -b TCGA-W5-AA2R-01A
    """
    return service.samples().get(sample_barcode=barcode).execute()

def sample_cloud_storage_file_paths(service, barcode=None):
    """
    Usage: python python/isb_cgc_api_v3_samples.py -e cloud_storage_file_paths -b TCGA-01-0642-11A
    """
    return service.samples().cloud_storage_file_paths(sample_barcode=barcode).execute()

# the following four APIs can be cross-program and are part of the isb_cgc_api endpoint
def cohort_get(service, cohort_id=1):
    """
    Usage: python python/isb_cgc_api_v3_cohorts.py -e get -c 24
    """
    return service.cohorts().get(cohort_id=cohort_id).execute()

def cohort_list(service):
    """
    Usage: python python/isb_cgc_api_v3_cohorts.py -e list
    """
    return service.cohorts().list().execute()

def cohort_delete(service, cohort_id):
    """
    Usage: python python/isb_cgc_api_v3_cohorts.py -e delete -c 24
    """
    return service.cohorts().delete(cohort_id=cohort_id).execute()

def cohort_cloud_storage_file_paths(
        service, cohort_id=None, 
        analysis_workflow_type=None, 
        data_category=None, 
        data_format=None, 
        data_type=None, 
        experimental_strategy=None,
        genomic_build=None,
        limit=None,
        platform=None,
        fields=None
    ):
    """
    Usage: python python/isb_cgc_api_v3_cohorts.py -e cloud_storage_file_paths -c 24
    """
    return service.cohorts().cloud_storage_file_paths(
        cohort_id=cohort_id, 
        analysis_workflow_type=analysis_workflow_type, 
        data_category=data_category, 
        data_format=data_format, 
        data_type=data_type,
        experimental_strategy=experimental_strategy,
        genomic_build=genomic_build,
        limit=limit,
        platform=platform,
        fields=fields
    ).execute()

# the following two APIs are specific to a program endpoint.  the examples use the isb_cgc_ccle_api endpoint but
# are also part of the isb_cgc_ccle_api and isb_cgc_target_api endpoints
# the APIs use the isb_cgc_tcga_api endpoint but are also part of the isb_cgc_ccle_api and isb_cgc_target_api endpoints
def cohort_preview(service, body=None):
    """
    Usage: python python/isb_cgc_api_v3_cohorts.py -e preview -b '{"project_short_name": ["TCGA-BRCA", "TCGA-UCS"], "age_at_diagnosis_gte": 90}'
    """
    return service.cohorts().preview(**body).execute()

def cohort_create(service, name=None, body=None):
    """
    Usage: python python/isb_cgc_api_v3_cohorts.py -e preview -n mycohortname -b '{"project_short_name": ["TCGA-BRCA", "TCGA-UCS"], "age_at_diagnosis_gte": 90}'
    """
    return service.cohorts().create(name=name, body=body).execute()

def main():
    params = {
        "project_short_name": "CCLE-BRCA",
        "hist_subtype": "ductal_carcinoma",
        "source": "ATCC"
    }
    ccle_service = get_authorized_service('_ccle')
    service = get_authorized_service('')

    cohort_info = cohort_preview(ccle_service, params)
    print('\ncohort preview:')
    pprint.pprint(cohort_info)
    
    cohort_info = cohort_create(ccle_service, 'ccle_brca_rnaseq', params)
    print('\ncohort create:')
    pprint.pprint(cohort_info)
    
    cohort_listing = cohort_list(service)
    print('\ncohort list:')
    pprint.pprint(cohort_listing)
    
    print('\ncohort get:')
    cohort_id = cohort_info['id']
    cohort_info = cohort_get(service, cohort_id)
    pprint.pprint(cohort_info)
    
    print('\ncase get:')
    case_barcode = cohort_info['cases'][0]
    case_info = case_get(ccle_service, case_barcode)
    pprint.pprint(case_info)
    
    print('\nsample get:')
    sample_barcode = cohort_info['samples'][0]
    sample_info = sample_get(ccle_service, sample_barcode)
    pprint.pprint(sample_info)
    
    print('\nsample get files:')
    sample_info = sample_cloud_storage_file_paths(ccle_service, sample_barcode)
    pprint.pprint(sample_info)

    print('\ncohort delete:')
    cohort_info = cohort_delete(service, cohort_id)
    pprint.pprint(cohort_info)

if __name__ == '__main__':
    main()
