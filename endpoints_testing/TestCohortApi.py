import time
import os
import json
import random
import requests
import unittest
from multiprocessing import Pool
from ParametrizedApiTest import ParametrizedApiTest

API_URL = "https://mvm-dot-isb-cgc.appspot.com/_ah/api/cohort_api/v1/"
HEADERS = {
	"Authorization": "Bearer {access_token}",
	"Content-type": "application/json"
}

class TestSaveCohort(ParametrizedApiTest):
	def setUp(self):
		# authenticate (or not, depending on whether the "auth" dict contains an entry or is None)
		# set up the test
		self.endpoint = "save_cohort/"
		self.json_requests = []
		self.cohort_ids = []
		self.process_pool = Pool(self.num_requests)
		
		# select a random group (num_requests many) of files containing cohort requests from the given config directory
		all_files = [os.listdir(self.config_dir)]
		count = 0
		while count < self.num_requests:
			next_file = random.choice(all_files)
			self.json_requests.append(json.load(next_file))
			all_files.pop(next_file)
			count += 1
		
	def tearDown(self):
		# delete cohorts if the setUp authenticated a user (auth is not None) or if the cohort ids array contains anything
		if self.auth is not None:
			for cohort_id in self.cohort_ids:
				response = requests.delete(API_URL + "delete_cohort/?cohort_id={cohort_id}".format(cohort_id=cohort_id))
				# should I make any assertions about the response?  if so, can this be an implicit test of the delete operation?
				
		
	@unittest.skipIf(self.auth is None or self.config != "minimal")
	def test_save_cohort_authenticated(self): # use for load testing
		expected_response = {
			"kind": "cohort_api#cohortsItem",
			"id": "",
			"name": "",
			"active": "",
			"last_date_saved": "",
			"user_id": ""
		}
		results = self.process_pool.map(self.save_cohort, self.json_requests)
		for r, execution_time in results:
			assert r.status_code == 201
			# assert that kind is in the response, and kind == cohort_api#cohortsItem
			# assert that id is in the response, and is of type string
			# assert that name is in the response, is of type string, and matches the name given in the request
			# assert that active is in the response, and active == True
			# assert that last_date_saved is in the response, is of type string (is it possible to match it with anything?)
			# assert that user_id is in the response, is of type string, and matches the user name of the authenticated user
			self.cohort_ids.append(r["id"])
	
	@unittest.skipIf(self.auth is not None)
	def test_save_cohort_unauthenticated(self): 
		pass

	@unittest.skipIf(self.auth is None or self.config != "missing_required_params")
	def test_save_cohort_missing_required_params(self):
		pass
	
	@unittest.skipIf(self.auth is None or self.config != "optional_params")
	def test_save_cohort_optional_params(self): 
		pass
	
	@unittest.skipIf(self.auth is None or self.config != "undefined_params") # necessary?
	def test_save_cohort_undefined_params(self): 
		pass
		
	@unittest.skipIf(self.auth is None or self.config != "incorrect_param_types")
	def test_save_cohort_incorrect_param_types(self):
		pass
		
	def save_cohort(self): # not a test, just a helper function
		start = time.time()
		response = requests.post(API_URL + endpoint, headers=HEADERS, json=cohort_request)
		end = time.time()
		execution_time = start - end
		return response, execution_time

class TestPreviewCohort(ParametrizedApiTest):
	def setUp(self):
		pass
	def tearDown(self):
		pass
			
class TestCohortsList(ParametrizedApiTest):
	def setUp(self):
		pass
	def tearDown(self):
		pass
		
class TestCohortsPatientsSamplesList(ParametrizedApiTest):
	def setUp(self):
		pass
	def tearDown(self):
		pass
		
class TestPatientDetails(ParametrizedApiTest):
	def setUp(self):
		pass
	def tearDown(self):
		pass
		
class TestSampleDetails(ParametrizedApiTest):
	def setUp(self):
		pass
	def tearDown(self):
		pass
		
class TestDatafilenamekeyList(ParametrizedApiTest):
	def setUp(self):
		pass
	def tearDown(self):
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
	suite = unittest.TestSuite()
	suite.addTest(ParametrizedTestCase.parametrize(TestSaveCohort, config_dir="", num_requests=1, auth=True)
	
if __name__ == "__main__":
	main()