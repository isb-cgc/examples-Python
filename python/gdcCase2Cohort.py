#!/usr/bin/env python
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


Create ISB-CGC cohorts from a GDC Case JSON file

From the command line use the following options:

  -c/--credentialsfile <Google credentials file>    OPTIONAL: This defaults to the .isb_credentials file in the user's home directory.  If one
													does not exist, it will be created even if this option is not used.
													
  -i/--inputfile <GDC case JSON file>				REQUIRED:  This is the JSON file obtained from the GDC that will be parsed to obtain the
													case IDs.
													
  -n/--cohortname									REQUIRED: This is the name that will be assigned to the cohort.  While this has no impact on
													cohort creation, it should be descriptive enough to be useful.

'''

from oauth2client.client import OAuth2WebServerFlow
from oauth2client import tools
from oauth2client.file import Storage
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import os
import argparse
import httplib2
import json

# the CLIENT_ID for the ISB-CGC site
CLIENT_ID = '907668440978-0ol0griu70qkeb6k3gnn2vipfa5mgl60.apps.googleusercontent.com'
# The google-specified 'installed application' OAuth pattern
CLIENT_SECRET = 'To_WJH7-1V-TofhNGcEqmEYi'
# The google defined scope for authorization
EMAIL_SCOPE = 'https://www.googleapis.com/auth/userinfo.email'
# where a default credentials file will be stored for use by the endpoints
DEFAULT_STORAGE_FILE = os.path.join(os.path.expanduser("~"), '.isb_credentials')


def get_credentials(credFile):
	oauth_flow_args = ['--noauth_local_webserver']
	if credFile is None:
		storage = Storage(DEFAULT_STORAGE_FILE)
	else:
		storage = Storage(credFile)
		
	credentials = storage.get()
	if not credentials or credentials.invalid:
		flow = OAuth2WebServerFlow(CLIENT_ID, CLIENT_SECRET, EMAIL_SCOPE)
		flow.auth_uri = flow.auth_uri.rstrip('/') + '?approval_prompt=force'
		credentials = tools.run_flow(flow, storage, tools.argparser.parse_args(oauth_flow_args))
	return credentials
   

def get_authorized_service(api, version, site, credentials):
    discovery_url = '%s/_ah/api/discovery/v1/apis/%s/%s/rest' % (site, api, version)
    http = credentials.authorize(httplib2.Http())
    if credentials.access_token_expired or credentials.invalid:
        credentials.refresh(http)
    authorized_service = build(api, version, discoveryServiceUrl=discovery_url, http=http)
    return authorized_service
    
def parseGDCCase(filename):
	inputfile = open(filename,'r')
	data = json.load(inputfile)
	uuids = []
	
	for entry in data:
		uuids.append(entry['case_id'])
	
	return uuids
	
def cohortsCreate(service, name, body):
	try:
		data = service.cohorts().create(name=name, body=body).execute()
		return data
	except HttpError as exception:
		raise exception
    
def main(args):
	#Main variables
	api = "isb_cgc_tcga_api"
	version = "v3"
	site = "https://api-dot-isb-cgc.appspot.com"
	
	#Set up credentials and API service
	credentials = get_credentials(args.credentialsfile)
	service = get_authorized_service(api, version, site, credentials)
	
	#Parse the case IDs from the GDC case file
	uuids = parseGDCCase(args.inputfile)
	
	#Create the cohort
	query = {"case_gdc_id" : uuids}
	try:
		data = cohortsCreate(service, args.cohortname, query)
	except HttpError as exception:
		print exception
	
	
	
    
if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("-c", "--credentialsfile", nargs = '?', const = None , help="File to use for credentials, will default to ~/.isb_credentials if left blank")
	parser.add_argument("-i", "--inputfile", required = True, help = "GDC Case JSON file")
	parser.add_argument("-n", "--cohortname", nargs = '?', const = None, required = True, help = "Provide a name for the cohort")
	args = parser.parse_args()

	main(args)
