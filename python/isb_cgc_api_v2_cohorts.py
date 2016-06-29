from argparse import ArgumentParser
from googleapiclient.discovery import build
from oauth2client.client import OAuth2WebServerFlow
from oauth2client import tools
from oauth2client.file import Storage
import httplib2
import pprint
import json
import sys
import os

# the CLIENT_ID for the ISB-CGC site
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


def get_unauthorized_service():
	api = 'isb_cgc_api'
	version = 'v2'
	site = "https://api-dot-isb-cgc.appspot.com"
	discovery_url = '%s/_ah/api/discovery/v1/apis/%s/%s/rest' % (site, api, version)
	return build(api, version, discoveryServiceUrl=discovery_url, http=httplib2.Http())


def get_authorized_service():
	api = 'isb_cgc_api'
	version = 'v2'
	site = "https://api-dot-isb-cgc.appspot.com"
	discovery_url = '%s/_ah/api/discovery/v1/apis/%s/%s/rest' % (site, api, version)

	credentials = get_credentials()
	http = credentials.authorize(httplib2.Http())

	if credentials.access_token_expired or credentials.invalid:
		credentials.refresh(http)

	authorized_service = build(api, version, discoveryServiceUrl=discovery_url, http=http)

	return authorized_service


## resource methods

def get(service, cohort_id=1, body=None, name=None):
	"""
	Usage: python python/isb_cgc_api_v2_cohorts.py -e get -c 24
	"""
	data = service.cohorts().get(cohort_id=cohort_id).execute()
	print '\nresults from cohorts().get()'
	pprint.pprint(data)


def list(service, cohort_id=None, body=None, name=None):
	"""
	Usage: python python/isb_cgc_api_v2_cohorts.py -e list
	"""
	data = service.cohorts().list().execute()
	print '\nresults from cohorts().list()'
	pprint.pprint(data)


def preview(service, cohort_id=None, body=None, name=None):
	"""
	Usage: python python/isb_cgc_api_v2_cohorts.py -e preview -b {"Study": ["BRCA", "UCS"], "age_at_initial_pathologic_diagnosis_gte": 90}
	"""
	data = service.cohorts().preview(body=body).execute()
	print '\nresults from cohorts().preview()'
	pprint.pprint(data['sample_count'])


def create(service, cohort_id=None, body=None, name=None):
	"""
	Usage: python python/isb_cgc_api_v2_cohorts.py -e preview -n mycohortname -b {"Study": ["BRCA", "UCS"], "age_at_initial_pathologic_diagnosis_gte": 90}
	"""
	data = service.cohorts().create(name=name, body=body).execute()
	print '\nresults from cohorts().create()'
	pprint.pprint(data)


def delete(service, cohort_id=None, body=None, name=None):
	"""
	Usage: python python/isb_cgc_api_v2_cohorts.py -e delete -c 24
	"""
	data = service.cohorts().delete(cohort_id=cohort_id).execute()
	print '\nresults from cohorts().delete()'
	pprint.pprint(data)

def cloud_storage_file_paths(service, cohort_id=None, body=None, name=None):
	"""
	Usage: python python/isb_cgc_api_v2_cohorts.py -e cloud_storage_file_paths -c 24
	"""
	data = service.cohorts().cloud_storage_file_paths(cohort_id=cohort_id).execute()
	print '\nresults from cohorts().cloud_storage_file_paths()'
	pprint.pprint(data)


def googlegenomics(service, cohort_id=None, body=None, name=None):
	"""
	Usage: python python/isb_cgc_api_v2_cohorts.py -e googlegenomics -c 24
	"""
	data = service.cohorts().googlegenomics(cohort_id=cohort_id).execute()
	print '\nresults from cohorts().googlegenomics()'
	pprint.pprint(data)


def main():
	parser = ArgumentParser()
	parser.add_argument('--endpoint', '-e',
						help='Name of cohorts endpoint to execute. '
							 'Options: get, list, preview, create, delete, datafilenamekeys, googlegenomics')
	parser.add_argument('--cohort_id', '-c',
						help='Id of cohort to use in get, delete, cloud_storage_file_paths, or googlegenomics endpoints')
	parser.add_argument('--body', '-b',
						help='Payload to use in preview or create endpoints. Example: '
							 '{"Study": ["BRCA", "UCS"], "age_at_initial_pathologic_diagnosis_gte": 90}')
	parser.add_argument('--name', '-n',
						help='The name of the cohort to create in the create endpoint.')
	args = parser.parse_args()
	if args.endpoint not in ['get', 'list', 'preview', 'create', 'delete', 'cloud_storage_file_paths', 'googlegenomics']:
		return

	service = get_unauthorized_service() if args.endpoint == 'preview' else get_authorized_service()
	cohort_id = args.cohort_id if args.cohort_id is None else int(args.cohort_id)
	body = json.loads(args.body) if args.body is not None else args.body

	globals()[args.endpoint](service, cohort_id=cohort_id, body=body, name=args.name)


if __name__ == '__main__':
	main()