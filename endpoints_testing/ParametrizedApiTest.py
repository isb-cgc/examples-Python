'''
Copyright 2015, Institute for Systems Biology.

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
import unittest

class ParametrizedApiTest(unittest.TestCase):
	def __init__(self, methodName="runTest", api=None, version=None, endpoint=None, resource=None, item_delete_key=None, deletes_resource=None, request=None, expected_response=None, expected_status_code=None, num_requests=None, auth=None):
		super(ParametrizedTestCase, self).__init__(methodName)
		self.api = api
		self.config = config  
		self.num_requests = num_requests 
		self.auth = auth 
		
	@staticmethod
	def parametrize(testcase_class, api=None, version=None, endpoint=None, resource=None, item_delete_key=None, deletes_resource=None, request=None, expected_response=None, expected_status_code=None, num_requests=None, auth=None):
		testloader = unittest.TestLoader()
		testnames = testloader.getTestCaseNames(testcase_class)
		suite = unittest.TestSuite()
		for name in testnames:
			suite.addTest(testcase_class(name, api=api, version=version, endpoint=endpoint, resource=resource, item_delete_key=item_delete_key, deletes_resource=deletes_resource, request=request, expected_response=expected_response, expected_status_code=expected_status_code, num_requests=num_requests, auth=auth))
		return suite
		
		# NOTES: 
		# name: the name of the testcase class
		# api: the name of the api to test (e.g., "cohort")
		# config: the test configuration type (one of "minimal", "missing_required_params", "optional_params", "undefined_param_names", "undefined_param_values", "incorrect_param_types", "incorrect_permissions")
		# num_requests: the number of concurrent requests to run -- to use for load testing; default=1
		# auth: a dict containing user credentials for logging in using selenium (e.g., { "username":"...", "password": "..." } )