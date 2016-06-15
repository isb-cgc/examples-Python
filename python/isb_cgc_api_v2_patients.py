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

def get_unauthorized_service():
	api = 'isb_cgc_api'
	version = 'v2'
	site = "http://localhost:8080"
	discovery_url = '%s/_ah/api/discovery/v1/apis/%s/%s/rest' % (site, api, version)
	return build(api, version, discoveryServiceUrl=discovery_url, http=httplib2.Http())

def get_authorized_service():
	api = 'isb_cgc_api'
	version = 'v2'
	site = "http://localhost:8080"
	discovery_url = '%s/_ah/api/discovery/v1/apis/%s/%s/rest' % (site, api, version)

	credentials = get_credentials()
	http = credentials.authorize(httplib2.Http())

	if credentials.access_token_expired or credentials.invalid:
		credentials.refresh(http)

	authorized_service = build(api, version, discoveryServiceUrl=discovery_url, http=http)

	return authorized_service

def preview(service):

	payload = {'age_at_initial_pathologic_diagnosis_lte': '10'}

	data = service.cohorts().preview(body=payload).execute()

	print '\n\nresults from preview'
	print data.get('sample_count')
	print data.get('patient_count')

def list_cohorts(service):

    data = service.cohorts().list().execute()
    print '\nresults from list_cohorts'
    pprint.pprint(data)

def create_cohort(service):
	body = {
		'Project': 'CCLE',
		'Study': 'MESO'
	}
	data = service.cohorts().create(name='lte15v2', body=body).execute()
	pprint.pprint(data)

def get_cohort(service):
	data = service.cohorts().get(cohort_id=1).execute()
	print '\npatient_count from cohort 1: ' + data.get('patient_count')
	return data.get('patients')[0], data.get('samples')[0]

def delete_cohort(service):
	data = service.cohorts().delete(cohort_id=9).execute()
	print '\nresult of delete cohort'
	print data

def datafilenamekeys_from_cohort(service):
	data = service.cohorts().datafilenamekeys(cohort_id=10).execute()
	print '\nresult of datafilenamekeys'
	print data.get('count')

def googlegenomics_from_cohort(service):
	data = service.cohorts().googlegenomics(cohort_id=11).execute()
	print 'result of googlegenomics'
	pprint.pprint(data)

def patient_details(service, barcode):
	data = service.patients().get(patient_barcode=barcode).execute()
	pprint.pprint(data)

def sample_details(service, barcode):
	data = service.samples().get(sample_barcode=barcode).execute()
	pprint.pprint(data)

def datafilenamekeys_from_sample(service, barcode):
	data = service.samples().datafilenamekeys(sample_barcode='TCGA-W5-AA2R-01A').execute()
	pprint.pprint(data)

def googlegenomics_from_sample(service):
	data = service.samples().googlegenomics(sample_barcode='CCLE-ACC-MESO-1-DNA-08').execute()
	print '\ngooglegenomics from sample'
	pprint.pprint(data)

def user_get(service):
	data = service.users().get().execute()
	print '\nresult of users.get:'
	print data

def main():
	unauthorized_service = get_unauthorized_service()

	authorized_service = get_authorized_service()
	# preview(authorized_service)
	# create_cohort(authorized_service)
	patient_barcode, sample_barcode = get_cohort(authorized_service)
	# delete_cohort(authorized_service)
	# list_cohorts(authorized_service)
	# datafilenamekeys_from_cohort(authorized_service)
	# googlegenomics_from_cohort(authorized_service)
	patient_details(authorized_service, patient_barcode)
	# sample_details(authorized_service, sample_barcode)
	# datafilenamekeys_from_sample(authorized_service, sample_barcode)
	# googlegenomics_from_sample(authorized_service)
	# user_get(authorized_service)


if __name__ == '__main__':
	main()