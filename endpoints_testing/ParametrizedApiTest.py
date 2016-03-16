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
	def __init__(self, methodName, test_config, auth):
		super(ParametrizedApiTest, self).__init__(methodName)
		self.api = test_config['api']
		self.version = test_config['version']
		self.base_resource = test_config['base_resource']
		self.client_id = test_config['client_id']
		self.client_secret = test_config['client_secret']
		self.credential_file_name = test_config['credential_file_name']
		self.resource = test_config['resource']
		self.endpoint = test_config['endpoint']
		self.type_test = test_config['type_test']
		self.discovery_url = test_config['discovery_url']
		self.deletes_resource = test_config['deletes_resource']
		self.test = test_config['test']
		self.auth = auth 
		
	@staticmethod
	def parametrize(testcase_class, test_name, test_config, auth):
		suite = unittest.TestSuite()
		suite.addTest(testcase_class(test_name, test_config, auth=auth))
		return suite
