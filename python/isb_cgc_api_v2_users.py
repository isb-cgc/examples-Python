from googleapiclient.discovery import build
from oauth2client.client import OAuth2WebServerFlow
from oauth2client import tools
from oauth2client.file import Storage
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


def get_authorized_service():
	api = 'isb_cgc_api'
	version = 'v2'
	site = "https://api-dot-mvm-dot-isb-cgc.appspot.com"
	discovery_url = '%s/_ah/api/discovery/v1/apis/%s/%s/rest' % (site, api, version)

	credentials = get_credentials()
	http = credentials.authorize(httplib2.Http())

	if credentials.access_token_expired or credentials.invalid:
		credentials.refresh(http)

	authorized_service = build(api, version, discoveryServiceUrl=discovery_url, http=http)

	return authorized_service


def get(service):
	"""
	Usage: python python/isb_cgc_api_v2_users.py
	"""
	data = service.users().get().execute()
	print '\nresult of users().get()'
	pprint.pprint(data)


def main():
	service = get_authorized_service()
	get(service)


if __name__ == '__main__':
	main()