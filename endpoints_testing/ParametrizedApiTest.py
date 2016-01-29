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
	def __init__(self, methodName="test_run", test_config_dict=None, num_requests=None, auth=None):
		super(ParametrizedApiTest, self).__init__(methodName)
		self.api = test_config_dict['api']
		self.version = test_config_dict['version']
		self.base_resource = test_config_dict['base_resource']
		self.resource = test_config_dict['resource']
		self.endpoint = test_config_dict['endpoint']
		self.type_test = test_config_dict['type_test']
		self.discovery_url = test_config_dict['discovery_url']
		self.deletes_resource = test_config_dict['deletes_resource']
		self.request = test_config_dict['request']
		self.expected_response = test_config_dict['expected_response']
		self.num_requests = num_requests 
		self.auth = auth 
		
	@staticmethod
	def parametrize(testcase_class, test_name, test_config_dict=None, num_requests=None, auth=None):
		testloader = unittest.TestLoader()
		testnames = testloader.getTestCaseNames(testcase_class)
		suite = unittest.TestSuite()
		suite.addTest(testcase_class(test_name, test_config_dict, num_requests=num_requests, auth=auth))
		return suite
