import unittest

class ParametrizedApiTest(unittest.TestCase):
	def __init__(self, methodName="runTest", api=None, version=None, endpoint=None, config=None, num_requests=None, auth=None):
		super(ParametrizedTestCase, self).__init__(methodName)
		self.api = api
		self.config = config  
		self.num_requests = num_requests 
		self.auth = auth 
		
	@staticmethod
	def parametrize(testcase_class, api=None, version=None, endpoint=None, config=None, num_requests=None, auth=None):
		testloader = unittest.TestLoader()
		testnames = testloader.getTestCaseNames(testcase_class)
		suite = unittest.TestSuite()
		for name in testnames:
			suite.addTest(testcase_class(name, api=api, version=version, endpoint=endpoint, config=config, num_requests=num_requests, auth=auth))
		return suite
		
		# NOTES: 
		# name: the name of the testcase class
		# api: the name of the api to test (e.g., "cohort")
		# config: the test configuration type (one of "minimal", "missing_required_params", "optional_params", "undefined_param_names", "undefined_param_values", "incorrect_param_types", "incorrect_permissions")
		# num_requests: the number of concurrent requests to run -- to use for load testing; default=1
		# auth: a dict containing user credentials for logging in using selenium (e.g., { "username":"...", "password": "..." } )
