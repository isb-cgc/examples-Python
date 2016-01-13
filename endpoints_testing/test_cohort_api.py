import time
import requests
import unittest

API_URL = "https://mvm-dot-isb-cgc.appspot.com/_ah/api/cohort_api/v1/"
HEADERS = {
	"Authorization": "Bearer {access_token}",
	"Content-type": "application/json"
}

class TestCohortApi(unittest.TestCase):
	def setUp(self):
		endpoint = "save_cohort/"
		
		# create and save some cohorts and assert that the saving was successful
		self.cohort_requests = cohort_requests
		self.cohort_ids = []
		expected_response = {
			
		}
		for cohort_request in self.cohort_requests: # I may want to do these all concurrently using multiprocessing
			create_cohort = requests.post(API_URL + endpoint, headers=HEADERS, json=cohort_request) 
			assert create_cohort.status_code == 201
			self.assertEqual(expected_response, create_cohort.json()) # may have to check individual fields in the response
			self.cohort_ids.append(response["id"])
	
	def tearDown(self):
		delete_endpoint = "delete_cohort/"
		list_endpoint = "cohorts_list/"
		# delete all cohorts that were created for this test, and assert that the deletion was successful
		for cohort_id in self.cohort_ids:
			delete_cohort = requests.delete(API_URL + delete_endpoint + cohort_id, headers=HEADERS)
			assert delete_cohort.status_code == 200
			list_deleted_cohort = requests.get(API_URL + list_endpoint + "?cohort_id={cohort_id}".format(cohort_id=cohort_id))
			assert list_deleted_cohort.status_code == 404
		
	def test_cohorts_list(self):
		# list all cohorts
		endpoint = "cohorts_list/"
		list_cohorts = requests.get(API_URL + endpoint)
		assert list_cohorts.status_code == 200
		listed_cohort_ids = [ cohort_id for response_key, cohort_id in list_cohorts.json() ]
		self.assertEqual(self.cohort_ids, listed_cohort_ids)
		
	def test_cohorts_patients_samples_list(self):
		# list patients and samples for a cohort
		
	
	def test_patient_details(self):
		pass
		
	def test_sample_details(self):
		pass
		
	def test_datafilenamekey_list(self):
		pass
			
		
def main():
	# final report should include length of all responses and time taken for each test
	# for each user, run the tests
	# run single instances of each test first to ensure that they are functional
	### run test_save_cohort (the single cohort that exists at this point should be as simple as possible)
	### run test_cohorts_list
	### run test_cohorts_patients_samples_list
	### run test_patient_details
	### run test_sample_details
	### run test_datafilenamekey_list
	### run test_delete_cohort (or, alternatively, just assert that the created cohort no longer exists)
	# load/stress tests
	### create 10 cohorts simultaneously
	### run test_cohorts_list
	### create 100 cohorts simultaneously
	### run test_cohorts_list
	### create 1000 cohorts simultaneously
	### run test_cohorts_list
	### create a cohort with ~10 samples
	### run test_cohorts_patients_samples_list 
	### run test_patient_details 
	### run test_sample_details 
	### run test_datafilenamekey_list
	### run test_delete_cohort (or, alternatively, just assert that the created cohort no longer exists)
	### create a cohort with ~100 samples
	### run test_cohorts_patients_samples_list 
	### run test_patient_details 
	### run test_sample_details 
	### run test_datafilenamekey_list
	### run test_delete_cohort (or, alternatively, just assert that the created cohort no longer exists)
	### create a cohort with ~1000 samples
	### run test_cohorts_patients_samples_list 
	### run test_patient_details 
	### run test_sample_details 
	### run test_datafilenamekey_list
	### run test_delete_cohort (or, alternatively, just assert that the created cohort no longer exists)
	pass
	
if __name__ == "__main__":
	main()