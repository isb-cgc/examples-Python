import unittest

class ParametrizedApiTest(unittest.TestCase):
	def __init__(self, methodName="runTest", api=None, version=None, config=None, num_requests=None, auth=None):
		super(ParametrizedTestCase, self).__init__(methodName)
		self.api = api
		self.config = config  
		self.num_requests = num_requests 
		self.auth = auth 
		
	@staticmethod
	def parametrize(testcase_class, api=None, version=None, config=None, num_requests=None, auth=None):
		testloader = unittest.TestLoader()
		testnames = testloader.getTestCaseNames(testcase_class)
		suite = unittest.TestSuite()
		for name in testnames:
			suite.addTest(testcase_class(name, api=api, version=version, resource=resource, test_method_name=test_method_name, crud_op=crud_op, delete_method_name=delete_method_name, delete_key=delete_key, config=config, num_requests=num_requests, auth=auth))
		return suite
		
		# NOTES: 
		# name: the name of the testcase class
		# api: the name of the api to test (e.g., "cohort")
		# version: the api version to test (e.g., "v1")
		# resource: the name of the resource under which to choose methods to run (e.g., "cohort")
		# test_method_name: the name of the endpoint method to test (e.g., "save")
		# crud_op: the CRUD operation (CREATE, READ, UPDATE, DELETE) of the method being tested
		# create_method_name: the name of the create method for an item; only required if crud_op=DELETE
		# delete_method_name: the name of the delete method for an item created during the test; only required if crud_op=CREATE
		# delete_key: a parameter to use to delete created objects during the teardown phase; only required if crud_op=CREATE
		# config: the test configuration type (one of "minimal", "missing_required_params", "optional_params", "undefined_param_names", "undefined_param_values", "incorrect_param_types", "incorrect_permissions")
		# num_requests: the number of concurrent requests to run -- to use for load testing; default=1
		# auth: a dict containing user credentials for logging in using selenium (e.g., { "username":"...", "password": "..." } )
