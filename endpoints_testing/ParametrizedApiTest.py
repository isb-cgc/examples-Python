import unittest

class ParametrizedApiTest(unittest.TestCase):
	def __init__(self, methodName="runTest", api=None, version=None, resource=None, test_method_name=None, crud_op=None, create_method_name=None, delete_method_name=None, delete_key=None, config=None, config_dir=None, num_requests=None, auth=None):
		super(ParametrizedTestCase, self).__init__(methodName)
		self.api = api
		self.version = version
		self.resource = resource
		self.test_method_name = test_method_name
		self.crud_op = crud_op
		self.delete_method_name = delete_method_name
		self.delete_key = delete_key
		self.config = config # this is the "type" of configuration for the test (one of "minimal", "missing_required_params", "optional_params", "undefined_param_names", "undefined_param_values", "incorrect_param_types")
		self.config_dir = config_dir # this is the directory location of the requests (json files) which match the given configuration
		self.num_requests = num_requests # the number of requests to make simultaneously for the test
		self.auth = auth # will be a dictionary containing a username/password combo -- do I need to use a copy() method for this?
		
	@staticmethod
	def parametrize(testcase_class, api=None, version=None, resource=None, test_method_name=None, crud_op=None, delete_method_name=None, delete_key=None, config=None, config_dir=None, num_requests=None, auth=None):
		testloader = unittest.TestLoader()
		testnames = testloader.getTestCaseNames(testcase_class)
		suite = unittest.TestSuite()
		for name in testnames:
			suite.addTest(testcase_class(name, api=api, version=version, resource=resource, test_method_name=test_method_name, crud_op=crud_op, delete_method_name=delete_method_name, delete_key=delete_key, config=config, config_dir=config_dir, num_requests=num_requests, auth=auth))
		return suite
		
		# NOTES: 
		# name: the name of the testcase class
		# api: the name of the api to test (e.g., "cohort")
		# version: the api version to test (e.g., "v1")
		# resource: the name of the resource under which to choose methods to run (e.g., "cohort")
		# test_method_name: the name of the endpoint method to test (e.g., "save")
		# crud_op: the CRUD operation (CREATE, READ, UPDATE, DELETE) for the test method
		# create_method_name: the name of the create method for an item; only required if crud_op=DELETE
		# delete_method_name: the name of the delete method for an item created during the test; only required if crud_op=CREATE
		# delete_key: a parameter to use to delete created objects during the teardown phase; only required if crud_op=CREATE
		# config: the test configuration type (one of "minimal", "missing_required_params", "optional_params", "undefined_param_names", "undefined_param_values", "incorrect_param_types", "incorrect_permissions")
		# config_dir: the directory containing the JSON formatted config files for each test
		# num_requests: the number of concurrent requests to run -- to use for load testing; default=1
		# auth: a dict containing user credentials for logging in using selenium (e.g., { "username":"...", "password": "..." } )