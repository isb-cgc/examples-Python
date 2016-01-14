import unittest

class ParametrizedApiTest(unittest.TestCase):
	def __init__(self, methodName="runTest", config=None, config_dir=None, num_requests=None, auth=None):
		super(ParametrizedTestCase, self).__init__(methodName)
		self.config = config # this is the "type" of configuration for the test (one of "minimal", "missing_required_params", "optional_params", "undefined_param_names", "undefined_param_values", "incorrect_param_types")
		self.config_dir = config_dir # this is the directory location of the requests (json files) which match the given configuration
		self.num_requests = num_requests # the number of requests to make simultaneously for the test
		self.auth = auth # will be a dictionary containing a username/password combo -- do I need to use a copy() method for this?
		
	@staticmethod
	def parametrize(testcase_class, config=None, config_dir=None, num_requests=None, auth=None):
		testloader = unittest.TestLoader()
		testnames = testloader.getTestCaseNames(testcase_class)
		suite = unittest.TestSuite()
		for name in testnames:
			suite.addTest(testcase_class(name, config=config, config_dir=config_dir, num_requests=num_requests, auth=auth))
		return suite
