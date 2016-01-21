import os
import sys
import time
import json
import random
import pprint
import argparse
import httplib2
import isb_auth
import isb_curl
import requests 
import unittest
from copy import deepcopy
from multiprocessing import Pool
from apiclient import discovery
from jsonspec.reference import resolve
from ParametrizedApiTest import ParametrizedApiTest

# TODO: Write browser automation with selenium modules (user login)
# TODO: Create config files for each method, and categorize by config type

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
		
		self.requests = []
		count = 0
		while count < self.num_requests:
			r = {}
			for request_param in self.request_params:
				# create an entity of type "type" with format "format", and insert it into the request object under key "name"
				value = self._create_entity(type, format)
				r[name] = value
			self.requests.append(r)
			
		# authenticate (or not, depending on whether the "auth" dict contains an entry or is None)
		if self.auth is not None:
			# simulate user login with selenium
			pass
		
		# build an API service object for the testing
		credentials = isb_auth.get_credentials()
		http = httplib2.Http()
		http = credentials.authorize(http)
		
		if credentials.access_token_expired:
			credentials.refresh(http)
		
		#api_root = 'https://mvm-dot-isb-cgc.appspot.com/_ah/api'
		api_root = "https://20160115t172707-dot-mvm-dot-isb-cgc.appspot.com/_ah/api"
		discovery_url = '%s/discovery/v1/apis/%s/%s/rest' % (api_root, "{api}_api".format(api=self.api), self.version)
		discovery_doc = requests.get(discovery_url).json()
		self.service = discovery.build(
      		self.api, self.version, discoveryServiceUrl=discovery_url, http=http)		

		# get information about the endpoint method to test
		def resolve_json_document_references(document): # this could probably be moved to a utils module, since it should be general enough to be used anywhere
			resolved_document = deepcopy(document)
			for key, value in document.iteritems():
				if type(value) is dict and "$ref" in value.keys():
					new_value = resolve(document, "#/{key}".format(key=key))

				else:
					new_value = resolve_json_document_references(value)	
				
				resolved_document[key] = new_value

			return resolved_document

		resolved_disovery_doc = resolve_json_document_references(discovery_doc)
		method_info = resolved_discovery_doc["resources"]["{api}_endpoints".format(api=self.api)]["resources"]["{resource}".format(resource=self.resource)]["methods"]["{method}".format(self.method_name)]

		# get expected response
		self.response_body_dict = {}
		expected_response = method_info["response"]
		for property_name, property_attr in expected_response["properties"].iteritems():
			if "required" in property_attr.keys():
				required = True
			else: 
				required = False

			self.response_body_dict[property_name] = {
				"type": property_attr["type"],
				"format": property_attr["format"],
				"required": required
			}

		# set up the test
		self.process_pool = Pool(self.num_requests)
		self.method_to_call = getattr(getattr(getattr(self.service, "{api}_endpoints".format(api=self.api)), "{resource}".format(resource=self.resource)), "{method}".format(method=self.method_name))
		self.create_method = getattr(method_parent, "{create_method}".format(create_method=self.create_method_name))
		self.delete_method = getattr(method_parent, "{delete_method}".format(delete_method=self.delete_method_name))
			
		# if the operation type is "DELETE" create and configuration type is one of "minimal", "large_data" or "optional_params", create some items and store the item ids in the created_items array
		if self.method_name == self.delete_method_name and self.config == "minimal" or self.config == "optional_params":
			# create some items using the resource's create method's "minimal" configuration 
			with open("config/{api}/minimal/{method}.json".format(api=self.api, config_type=self.config, method=self.create_method)) as f:
				json_config = json.load(f)
			
			for request in json_config["resource_requests"]:
				response = self.create_item(request)
				self.created_items.append(response[self.delete_key])
		
	def tearDown(self):
		# delete items if the setUp authenticated a user (auth is not None) or if the created items array contains anything
		if self.auth is not None or len(self.created_items) > 0:
			delete_kwargs = {}
			for item in self.created_items:
				delete_kwargs[self.delete_key] = item
				self.delete_method(**delete_kwargs)
				
	@skipIfUnauthenticated
	def test_authenticated(self): # use for load testing
		results, execution_time = self._query_endpoint()
		# log execution time
		for r in results:
			self.assertEqual(r.status_code, 200)
			self._check_response(r)
			
	
	@skipIfAuthenticated
	def test_unauthenticated(self): 
		results, execution_time = self._query_endpoint()
		# log execution time
		for r in results:
			if self.requires_auth:
				self.assertEqual(r.status_code, 401)
			else:
				self.assertEqual(r.status_code, 200)
				self._check_response(r)
	
	@skipIfUnauthenticated
	@skipIfConfigMismatch("incorrect_param_types") # parameter values given are the wrong type
	def test_incorrect_param_types(self):
		results, execution_time = self._query_endpoint()
		# log execution time
		for r in results:
			self.assertEqual(r.status_code, 400)

	@skipIfUnauthenticated
	@skipIfConfigMismatch("incorrect_param_values") # parameter values given are the wrong format
	def test_incorrect_param_values(self):
		results, execution_time = self._query_endpoint()
		# log execution time
		for r in results:
			self.assertEqual(r.status_code, 400)

	@skipIfUnauthenticated
	@skipIfConfigMismatch("incorrect_permissions") # the user doesn't have the correct permissions -- is this even testable in for our apis?
	def test_incorrect_permissions(self):
		results, execution_time = self._query_endpoint()
		# log execution time
		for r in results:
			self.assertEqual(r.status_code, 401)
	
	@skipIfUnauthenticated
	@skipIfConfigMismatch("missing_required_params") # required params are missing from the request
	def test_missing_required_params(self):
		results, execution_time = self._query_endpoint()
		# log execution time
		for r in results:
			self.assertEqual(r.status_code, 400)
	
	@skipIfUnauthenticated
	@skipIfConfigMismatch("optional_params") # optional params are provided in the request
	def test_optional_params(self): 
		results, execution_time = self._query_endpoint()
		# log execution time
		for r in results:
			self.assertEqual(r.status_code, 200)
			self._check_response(r)
	
	@skipIfUnauthenticated
	@skipIfConfigMismatch("undefined_param_values") # undefined (not found) param values are provided in the request
	def test_undefined_param_values(self): 
		results, execution_time = self._query_endpoint()
		# log execution time
		for r in results:
			self.assertEqual(r.status_code, 404)

	@skipIfUnauthenticated
	@skipIfConfigMismatch("undefined_param_names") # undefined param names are given in the request
	def test_undefined_param_names(self):
		results, execution_time = self._query_endpoint()
		# log execution time
		# log the length of the response in bytes
		for r in results:
			self.assertEqual(r.status_code, 400)


	# helper methods
	def _create_entity(self, type, format):
		# this method should return an object of type "type", format "format"
		if type is "string":
			if format is "int64":
				character_choices = string.digits
			else:
				character_choices = string.ascii_uppercase + string.ascii_lowercase + string.digits
			
			entity = ''.join(random.choice(character_choices) for _ in range(16))
				
		elif type is "number":
			if format is "double":
				entity = random.uniform(0, (pow(2, 53) - 1))

		elif type is "boolean":
			pass
		
		elif type is "array":
			pass
			
		return entity
		
	def _query_api(self):
		def wrap_method_call(self, params):
			self.method_to_call.__call__(**params).execute()

		start = time.time()
		results = self.process_pool.map(wrap_method_call, self.requests_params)
		end = time.time()
		execution_time = end - start
		return results, execution_time
		
	def _check_response(self, response):
		for key, value in self.response_body_dict.iteritems():
				self.assertIn(key, response.keys())
				self.assertIs(type(response[key]), value)
			if self.method_name == self.create_method_name:
				self.created_items.append(response[self.delete_key])
		
		
def main():
	# final report should include length of all responses and time taken for each test
	# for each user, run the tests
	# run single instances of each test first to ensure that they are functional
	# select a group (num_requests many) of requests from the given config
	
	parser = argparse.ArgumentParser(description="Unit test module for ISB CGC endpoints")
	parser.addArgument("api_name", help="The name of the API to run the unit tests against")
	parser.addArgument("--test_user_credentials", help="A list of objects containing credentials for test users")
	args = parser.parse_args()
	
	with open("config/{api_config}.json".format(api_config=args.api_name)) as f:
		json_config = json.load(f)
		
		
	suite = unittest.TestSuite()
	
	for test_user_credentials in args.test_user_credentials:
		for endpoint_name, endpoint_config in json_config["endpoints"].iteritems():
			resource = json_config[endpoint_name]["resource"] # need to resolve references in json config
			create_method = resource["create_method"]
			delete_method = resource["delete_method"]
			delete_key = resource["delete_key"]
			requires_auth = json_config[endpoint_name]["requires_auth"]
			request_params = json_config[endpoint_config]["request_params"]
			
			if resource["type"] is object:
				pass
			elif resource["type"] is array:
				pass
				
			for test_config_name, test_config in endpoint_config["test_config"].iteritems():
				# create a test with authorization
				suite.addTest(ParametrizedTestCase.parametrize(IsbCgcApiTest, api=args.api_name, version=json_config["version"], endpoint=endpoint_name, resource=resource, request_params=request_params, create_method=create_method, delete_method=delete_method, delete_key=delete_key, requires_auth=requires_auth, config=test_config_name, num_requests=1, auth=test_user_credentials))
				
				# create a test without authorization
				suite.addTest(ParametrizedTestCase.parametrize(IsbCgcApiTest, api=args.api_name, version=json_config["version"], endpoint=endpoint_name, resource=resource, request_params=request_params, create_method=create_method, delete_method=delete_method, delete_key=delete_key, requires_auth=requires_auth, config=test_config_name, num_requests=1, auth=None))
				
				# create load tests (many requests/responses)
				suite.addTest(ParametrizedTestCase.parametrize(IsbCgcApiTest, api=args.api_name, version=json_config["version"], endpoint=endpoint_name, resource=resource, request_params=request_params, create_method=create_method, delete_method=delete_method, delete_key=delete_key, requires_auth=requires_auth, config=test_config_name, num_requests=10, auth=test_user_credentials))
				suite.addTest(ParametrizedTestCase.parametrize(IsbCgcApiTest, api=args.api_name, version=json_config["version"], endpoint=endpoint_name, resource=resource, request_params=request_params, create_method=create_method, delete_method=delete_method, delete_key=delete_key, requires_auth=requires_auth, config=test_config_name, num_requests=50, auth=test_user_credentials))
				suite.addTest(ParametrizedTestCase.parametrize(IsbCgcApiTest, api=args.api_name, version=json_config["version"], endpoint=endpoint_name, resource=resource, request_params=request_params, create_method=create_method, delete_method=delete_method, delete_key=delete_key, requires_auth=requires_auth, config=test_config_name, num_requests=100, auth=test_user_credentials))
				suite.addTest(ParametrizedTestCase.parametrize(IsbCgcApiTest, api=args.api_name, version=json_config["version"], endpoint=endpoint_name, resource=resource, request_params=request_params, create_method=create_method, delete_method=delete_method, delete_key=delete_key, requires_auth=requires_auth, config=test_config_name, num_requests=500, auth=test_user_credentials))
				suite.addTest(ParametrizedTestCase.parametrize(IsbCgcApiTest, api=args.api_name, version=json_config["version"], endpoint=endpoint_name, resource=resource, request_params=request_params, create_method=create_method, delete_method=delete_method, delete_key=delete_key, requires_auth=requires_auth, config=test_config_name, num_requests=1000, auth=test_user_credentials))
	
	
if __name__ == "__main__":
	main(sys.argv[1:])