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

import sys
import time
import json
import pprint
import argparse
import httplib2
import isb_auth
import isb_curl
import requests 
import unittest
from datetime import datetime
from multiprocessing import Pool
from apiclient import discovery
from jsonspec.reference import resolve
from ParametrizedApiTest import ParametrizedApiTest

# TODO: Write browser automation with selenium modules (user login)

CREATED_ITEMS = {}
		
# this hack resolves an issue with TextTestRunner where it expects a stream to have a writeln method
# from: http://svn.python.org/projects/python/trunk/Lib/unittest/runner.py
# issue: https://bugs.python.org/issue16739
class _WritelnDecorator(object):
	"""Used to decorate file-like objects with a handy 'writeln' method"""
	def __init__(self,stream):
		self.stream = stream

	def __getattr__(self, attr):
		if attr in ('stream', '__getstate__'):
			raise AttributeError(attr)
		return getattr(self.stream,attr)

	def writeln(self, arg=None):
		if arg:
			self.write(arg)
		self.write('\n') # text-mode streams translate to \r\n if needed

class IsbCgcApiTest(ParametrizedApiTest):
	def test_run(self):
		# authenticate (or not, depending on whether the "auth" dict contains an entry or is None)
		if self.auth is not None:
			# simulate user login with selenium
			pass
		
		print '%s: testing %s.%s as %s' % (datetime.now(), self.resource, self.endpoint, self.type_test)
		# build an API service object for the testing
		credentials = isb_auth.get_credentials()
		http = httplib2.Http()
		http = credentials.authorize(http)
		
		if credentials.access_token_expired:
			credentials.refresh(http)
		
		self.service = discovery.build(
      		self.api, self.version, discoveryServiceUrl=self.discovery_url, http=http)	

		# set up and run the test
		self.process_pool = Pool()
		self.method_to_call = getattr(getattr(getattr(self.service, "{api}_endpoints".format(api=self.api)), "{resource}".format(resource=self.resource)), "{method}".format(method=self.endpoint))
		self.requests = []
		count = 0
		while count < self.num_requests:
			if self.deletes_resource == 'true':
				self.request[self.delete_key] == CREATED_ITEMS[self.delete_key][count] 
			self.requests.append(self.request)
			count += 1
			
		results, execution_time = self._query_endpoint()
		for r in results:
			self._check_response(r)
			if self.item_delete_key is not None:
				CREATED_ITEMS[self.resource].append(r[self.delete_key])
			
		# log execution time
		print '%s: finished testing %s.%s as %s' % (datetime.now(), self.resource, self.endpoint, self.type_test)

	# helper methods
	def _query_api(self):
		def wrap_method_call(self, params):
			self.method_to_call(**params).execute()

		start = time.time()
		results = self.process_pool.map(wrap_method_call, self.requests)
		end = time.time()
		execution_time = end - start
		return results, execution_time
		
	def _check_response(self, response):
		self.assertEqual(response.status_code, self.expected_status_code)
		for key, details in self.expected_response.iteritems():
			if "value" in details.keys():
				self.assertEqual(response[key], details["value"])
			else:
				if "format" in details.keys():
					if details["format"] == "int64":
						self.assertRegexpMatches(response[key], "[0-9]+")
					# probably will need to include other formats later
						
				else:
					self.assertIsInstance(response[key], str)
		
def main():
	# final report should include length of all responses and time taken for each test
	parser = argparse.ArgumentParser(description="Unit test module for ISB CGC endpoints")
	parser.add_argument("api_name", help="The name of the API to run the unit tests against")
	parser.add_argument("--test_user_credentials", help="A list of objects containing credentials for test users", nargs = '+')
	args = parser.parse_args()
	
	with open("endpoints_testing/config/{api_config}.json".format(api_config=args.api_name)) as f:
		json_config = json.load(f)

	endpoints_url_base = json_config["endpoints_url_base"]
	for api_name, api_config in json_config["apis"].iteritems():
# 		api_config = json_config["apis"][args.api_name]
		discovery_url = endpoints_url_base + api_config["discovery_uri"]
		version = api_config["version"]
		# get the endpoints test order
		endpoint_test_ordering = []
		
		for reference in api_config["test_ordering"]:
			# TODO: why bother with partition?  can just split and take [2] index
			resource_name = reference.partition("resources")[2].split('/')[1]
			endpoint_name = reference.split('/')[-1]
			test_config = resolve(api_config, reference)
			test_config["resource"] = resource_name
			test_config["endpoint"] = endpoint_name
			endpoint_test_ordering.append(test_config)
		
		test_unauthorized = unittest.TestSuite()
		test_authorized = unittest.TestSuite()
		load_test_authorized = unittest.TestSuite()
		
		for test_user_credentials in args.test_user_credentials:
			# add tests without authorization first
			for endpoints_test_config in endpoint_test_ordering:
				resource = endpoints_test_config["resource"] 
				endpoint_name = endpoints_test_config["endpoint"]
				requires_auth = endpoints_test_config["requires_auth"]
				
				if endpoints_test_config["creates_resource"] == "true":
					CREATED_ITEMS[resource] = []
					item_delete_key = resolve(api_config, "#/resources/{resource}/delete_key".format(resource=resource))
				else:
					item_delete_key=None
				
				for test_config_name, test_config_dict in endpoints_test_config['test_config'].iteritems():
					if test_config_name == "minimal":
						test_unauthorized.addTest(ParametrizedApiTest.parametrize(IsbCgcApiTest, api=args.api_name, version=version, endpoint=endpoint_name, resource=resource, discovery_url=discovery_url, type_test=test_config_name, item_delete_key=item_delete_key, deletes_resource=endpoints_test_config["deletes_resource"], request=test_config_dict["request"], expected_response=test_config_dict["expected_response"], expected_status_code=test_config_dict["expected_status_code"], num_requests=1, auth=None))
						load_test_authorized.addTest(ParametrizedApiTest.parametrize(IsbCgcApiTest, api=args.api_name, version=version, endpoint=endpoint_name, resource=resource, discovery_url=discovery_url, type_test=test_config_name, item_delete_key=item_delete_key, deletes_resource=endpoints_test_config["deletes_resource"], request=test_config_dict["request"], expected_response=test_config_dict["expected_response"], expected_status_code=test_config_dict["expected_status_code"], num_requests=10, auth=test_user_credentials))
						load_test_authorized.addTest(ParametrizedApiTest.parametrize(IsbCgcApiTest, api=args.api_name, version=version, endpoint=endpoint_name, resource=resource, discovery_url=discovery_url, type_test=test_config_name, item_delete_key=item_delete_key, deletes_resource=endpoints_test_config["deletes_resource"], request=test_config_dict["request"], expected_response=test_config_dict["expected_response"], expected_status_code=test_config_dict["expected_status_code"], num_requests=50, auth=test_user_credentials))
						load_test_authorized.addTest(ParametrizedApiTest.parametrize(IsbCgcApiTest, api=args.api_name, version=version, endpoint=endpoint_name, resource=resource, discovery_url=discovery_url, type_test=test_config_name, item_delete_key=item_delete_key, deletes_resource=endpoints_test_config["deletes_resource"], request=test_config_dict["request"], expected_response=test_config_dict["expected_response"], expected_status_code=test_config_dict["expected_status_code"], num_requests=100, auth=test_user_credentials))
						load_test_authorized.addTest(ParametrizedApiTest.parametrize(IsbCgcApiTest, api=args.api_name, version=version, endpoint=endpoint_name, resource=resource, discovery_url=discovery_url, type_test=test_config_name, item_delete_key=item_delete_key, deletes_resource=endpoints_test_config["deletes_resource"], request=test_config_dict["request"], expected_response=test_config_dict["expected_response"], expected_status_code=test_config_dict["expected_status_code"], num_requests=500, auth=test_user_credentials))
						load_test_authorized.addTest(ParametrizedApiTest.parametrize(IsbCgcApiTest, api=args.api_name, version=version, endpoint=endpoint_name, resource=resource, discovery_url=discovery_url, type_test=test_config_name, item_delete_key=item_delete_key, deletes_resource=endpoints_test_config["deletes_resource"], request=test_config_dict["request"], expected_response=test_config_dict["expected_response"], expected_status_code=test_config_dict["expected_status_code"], num_requests=1000, auth=test_user_credentials))
						
					# set these up so the test can at least run unsuccessfully until these are added to the config file 
					request=test_config_dict["request"] if "request" in test_config_dict else None
					expected_response=test_config_dict["expected_response"] if "expected_response" in test_config_dict else None
					expected_status_code=test_config_dict["expected_status_code"] if "expected_status_code" in test_config_dict else None
					test_authorized.addTest(ParametrizedApiTest.parametrize(IsbCgcApiTest, api=api_name, version=version, endpoint=endpoint_name, resource=resource, discovery_url=discovery_url, type_test=test_config_name, item_delete_key=item_delete_key, deletes_resource=endpoints_test_config["deletes_resource"], request=request, expected_response=expected_response, expected_status_code=expected_status_code, num_requests=1, auth=test_user_credentials))
					
	stream = _WritelnDecorator(sys.stdout)
	results_unautherized = unittest.TextTestResult(stream = stream, descriptions = True, verbosity = 2)
	test_unauthorized.run(results_unautherized)
	results_unautherized
	CREATED_ITEMS.clear()
	
	results_autherized = unittest.TextTestResult(stream = stream, descriptions = True, verbosity = 2)
	test_authorized.run(results_autherized)
	results_autherized
	CREATED_ITEMS.clear()

	results_load_test_autherized = unittest.TextTestResult(stream = stream, descriptions = True, verbosity = 2)
	load_test_authorized.run(results_load_test_autherized)
	results_load_test_autherized
	CREATED_ITEMS.clear()
	
if __name__ == "__main__":
# 	main(sys.argv[1:])
	main()
