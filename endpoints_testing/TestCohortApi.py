import os
import time
import json
import random
import pprint
import httplib2
import isb_auth
import isb_curl
import requests 
import unittest
from multiprocessing import Pool
from apiclient import discovery
from jsonspec.reference import resolve
from ParametrizedApiTest import ParametrizedApiTest

API_URL = "https://mvm-dot-isb-cgc.appspot.com/_ah/api/cohort_api/v1/"

def skipIfAuthenticated(f):
	def _skipIfAuthenticated(self):
		if self.auth is not None:
			return unittest.skip("Skipping test... Reason: User is authenticated")
		else:
			return f(self)
		return _skipIfAuthenticated

def skipIfUnauthenticated(f):
	def _skipIfUnauthenticated(self):
		if self.auth is None:
			return unittest.skip("Skipping test... Reason: User is unauthenticated")
		else:
			return f(self)
		return _skipIfUnauthenticated
		
def skipIfConfigMismatch(config):
	def _skipIfConfigMismatch(f):
		def wrapper(self):
			if config!= self.config:
				return unittest.skip("Skipping test... Reason: Test configuration mismatch".format(reason=reason))
			else:
				return f(self)
		return wrapper
	return _skipIfConfigMismatch
		
class IsbCgcApiTest(ParametrizedApiTest):
	def setUp(self):
		# need to keep track of any items that were created by the test so they can be deleted later
		self.created_items = []
			
		# authenticate (or not, depending on whether the "auth" dict contains an entry or is None)
		if self.auth is not None:
			# simulate user login with selenium
			pass
		
		# build an API service object for the testing
		credentials = isb_auth.get_credentials()
		http = httplib2.Http()
		http = credentials.authorize(http)
		
		# build a an API service object and get information about the endpoint method to test
		api_root = 'https://mvm-dot-isb-cgc.appspot.com/_ah/api'
		discovery_url = '%s/discovery/v1/apis/%s/%s/rest' % (api_root, "{api}_api".format(api=self.api), self.version) # consider refactoring api and version to ParametrizedApiTest
		discovery_doc = resolve(requests.get(discovery_url).json())
		method_info = discovery_doc["resources"]["{api}_endpoints".format(api=self.api)]["resources"]["{resource}".format(resource=self.resource)]["methods"]["{method}".format(self.method)]
		self.service = discovery.build(
      self.api, self.version, discoveryServiceUrl=discovery_url, http=http)
		
		# get expected response
		self.response_body_dict = {}
		expected_response = method_info["response"]
		for property_name, property_attr in expected_response["properties"].iteritems():
			self.response_body_dict[property_name] = property_attr["type"]

		# set up the test
		self.json_requests = []
		self.process_pool = Pool(self.num_requests)
		method_grandparent = getattr(self.service, "{api}_endpoints".format(api=self.api))
		method_parent = getattr(method_grandparent, "{resource}".format(resource=self.resoure))
		self.method_to_call = getattr(method_parent, "{method}".format(self.method))
		
		# select a random group (num_requests many) of files containing cohort requests from the given config directory
		# note to self: may need to update the format of the config files
		all_files = [os.listdir(self.config_dir)]
		count = 0
		while count < self.num_requests:
			next_file = random.choice(all_files)
			self.json_requests.append(json.load(next_file))
			all_files.pop(next_file)
			count += 1
			
		# if the operation type is "DELETE", create some items and store the item ids in the created_items array
		if self.crud_op == "DELETE":
			
		
	def tearDown(self):
		# delete items if the setUp authenticated a user (auth is not None) or if the created items array contains anything
		if self.auth is not None or if len(self.created_items) > 0:
			method_grandparent = getattr(self.service, "{api}_endpoints".format(api=self.api))
			method_parent = getattr(method_grandparent, "{resource}".format(resource=self.resoure))
			method_to_call = getattr(method_parent, self.delete_method_name)
			for item in self.created_items:
				item_to_delete = item[self.delete_key]
				response = method_to_call(item_to_delete)
				
	@skipIfUnauthenticated
	@skipIfConfigMismatch("minimal")
	def test_authenticated(self): # use for load testing
		start = time.time()
		results = self.process_pool.map(self.method_to_call, self.json_requests)
		end = time.time()
		execution_time = end - start
		# log execution time
		for r in results:
			# make an assertion about the status code
			for key, value in self.response_body_dict.iteritems():
				self.assertIn(key,r.keys())
				self.assertIs(type(r[key]), value)
			if self.operation_type == "CREATE":
				self.created_items.append(r)
	
	@skipIfAuthenticated
	def test_unauthenticated(self): 
		pass
		
	@skipIfUnauthenticated
	@skipIfConfigMismatch("incorrect_permissions")
	def test_incorrect_permissions(self):
		pass
	
	@skipIfUnauthenticated
	@skipIfConfigMismatch("missing_required_params")
	def test_missing_required_params(self):
		pass
	
	@skipIfUnauthenticated
	@skipIfConfigMismatch("optional_params")
	def test_optional_params(self): 
		pass
	
	@skipIfUnauthenticated
	@skipIfConfigMismatch("undefined_param_values")
	def test_undefined_param_values(self): 
		pass

	@skipIfUnauthenticated
	@skipIfConfigMismatch("undefined_param_names")
	def test_undefined_param_names(self):
		pass
	
	@skipIfUnauthenticated
	@skipIfConfigMismatch("incorrect_param_types")
	def test_incorrect_param_types(self):
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
	suite.addTest(ParametrizedTestCase.parametrize(TestSaveCohort, config="", config_dir="", num_requests=1, auth=True))
	
if __name__ == "__main__":
	main()
