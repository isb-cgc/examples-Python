import unittest

class ParametrizedApiTest(unittest.TestCase):
	def __init__(self, methodName="runTest", param=None):
		super(ParametrizedTestCase, self).__init__(methodName)
		self.param = param
		
	@staticmethod
	def parametrize(testcase_class, param=None):
		testloader = unittest.TestLoader()
		testnames = testloader.getTestCaseNames(testcase_class)
		suite = unittest.TestSuite()
		for name in testnames:
			suite.addTest(testcase_class(name, param=param))
		return suite