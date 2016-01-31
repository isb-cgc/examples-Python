'''
Copyright 2015, Institute for Systems Biology.

Licensed under the Apache License, Version 2.0 (the 'License');
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an 'AS IS' BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

'''
import argparse
import httplib2
import json
import multiprocessing
import requests 
import sys
import time
import unittest
from apiclient import discovery
from concurrent import futures
from datetime import datetime
from jsonspec.reference import resolve

import isb_auth
from ParametrizedApiTest import ParametrizedApiTest

# TODO: Write browser automation with selenium modules (user login)

cohort_ids = set()
# TODO: modify stress test to have a request per count, rather than repeat the same request
# TODO: run one save per six example saves, then for stress test, randomly pick one per count

# this work around resolves an issue with TextTestRunner where it expects a stream to have a writeln method
# from: http://svn.python.org/projects/python/trunk/Lib/unittest/runner.py
# issue: https://bugs.python.org/issue16739
class _WritelnDecorator(object):
    '''Used to decorate file-like objects with a handy 'writeln' method'''
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
    def cohort_patients_samples_list_test(self):
        return
        self.test_run()
        
    def datafilenamekey_list_test(self):
        return
        self.test_run()
        
    def sample_details_test(self):
        return
        self.test_run()
        
    def patient_details_test(self):
        return
        self.test_run()
        
    def list_test(self):
        responses = self.test_run()
        cohort_count = responses[0]['count']
        print '\tfound %s cohorts' % (cohort_count)
        global cohort_ids
        cohort_ids = set()
        for item in responses[0]['items']:
            if 'OWNER' == item['perm']:
                cohort_ids.add(item['id'])
        self.assertEqual(int(cohort_count), len(cohort_ids), '%s:%s:%s returned a count different than the number of items' % (self.resource, self.endpoint, self.type_test))
        
    def delete_test(self):
        count = 0
        try:
            for cohort_id in cohort_ids:
                count += 1
                self.test_config_dict['request']['cohort_id'] = cohort_id
                self.num_requests = 1
                time.sleep(3)
                self.test_run()
        finally:
            print 'deleted %s cohorts out of %s' % (count, len(cohort_ids))
        for cohort_id in cohort_ids:
            cohort_ids.remove(cohort_id)
        
    def save_test(self):
        self.test_run()
        
    def test_run(self):
        execution_time = 0
        try:
            # authenticate (or not, depending on whether the 'auth' dict contains an entry or is None)
            if self.auth is not None:
                # simulate user login with selenium
                pass
            
            print '\n%s: testing %s.%s as %s.  repeats: %s' % (datetime.now(), self.resource, self.endpoint, self.type_test, self.num_requests)
            self.assertTrue((self.test_config_dict['request'] and 0 < len(self.test_config_dict['request'])) or self.endpoint == 'list', 'no request is specified to submit for %s:%s:%s' % (self.resource, self.endpoint, self.type_test))
            # build an API service object for the testing
            credentials = isb_auth.get_credentials()
            http = httplib2.Http()
            http = credentials.authorize(http)
            
            if credentials.access_token_expired:
                credentials.refresh(http)
            
            print '%s:\tbuild(%s)' % (datetime.now(), self.discovery_url)
            self.service = discovery.build(
                self.api, self.version, discoveryServiceUrl=self.discovery_url, http=http)    
            print '%s:\tfinished build(%s)' % (datetime.now(), self.discovery_url)
    
            # set up and run the test
            method_to_call = getattr(getattr(getattr(self.service, '{api}_endpoints'.format(api=self.base_resource))(), '{resource}'.format(resource=self.resource))(), '{method}'.format(method=self.endpoint))
            requests = []
            count = 0
            while count < self.num_requests:
                requests.append(method_to_call(**self.test_config_dict['request']))
                count += 1
                
            responses, execution_time = self._query_api(requests)
            for r in responses:
                self._check_response(r)
            return responses
        finally:
            # log execution time
            print '%s: finished testing %s.%s as %s.  took %s' % (datetime.now(), self.resource, self.endpoint, self.type_test, execution_time)

    # helper methods
    def _query_api(self, requests):
        def wrap_method_call(*method_to_call):
            return method_to_call[0].execute()

        executor = futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count() * 4)

        start = time.time()
        responses = []
        future2request = {}
        count = 0
        for request in requests:
            future2request[executor.submit(wrap_method_call, request)] = [request, count, time.time()]
            count += 1
        responses = []
        future_keys = future2request.keys()
        while future_keys:
            future_done, future_keys = futures.wait(future_keys, return_when = futures.FIRST_COMPLETED)
            for future in future_done:
                run_info = future2request.pop(future)
                run_info[2] = time.time() - run_info[2]
                if future.exception() is not None:
                    print '%s:\trun %s(%s) generated an exception--%s' % (datetime.now(), run_info[1], run_info[2], future.exception())
                    responses += [{'ERROR': future.exception()}]
                else:
                    print '%s:\trun%s(%s) succeeded\n' % (datetime.now(), run_info[1], run_info[2])
                    responses += [future.result()]
        execution_time = time.time() - start
        return responses, execution_time
        

    def _check_expected_map_list(self, response, expected_response, key, indent):
        count = 0
        for nextmap in response:
            if 0 == count % 32:
                print '%schecking the %s map on the list for %s' % (indent, count, key)
            count += 1
            self._check_expected(nextmap, expected_response, indent + '\t')
        print '%schecked %s total maps for %s' % (indent, count, key)

    def _check_expected(self, response, expected_response, indent):
        for key, details in expected_response.iteritems():
            if 'value' in details.keys():
                self.assertEqual(response[key], details['value'], 'value in response isn\'t equal to expected value for %s:%s:%s: %s != %s' % 
                    (self.resource, self.endpoint, self.type_test, response[key], details['value']))
            elif 'type' in details.keys():
                if details['type'] == 'string':
                    if 'format' in details.keys():
                        if details['format'] == 'int64':
                            self.assertRegexpMatches(response[key], '^[+-]?[0-9]+$', 'value not in expected format(int64) for %s:%s:%s' % (self.resource, self.endpoint, self.type_test))
                        elif details['format'] == 'date':
                            self.assertRegexpMatches(response[key], '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$', 'value not in expected format(date) for %s:%s:%s' % 
                                (self.resource, self.endpoint, self.type_test))
                    else:
                        self.assertIsInstance(response[key], basestring, 'value not in expected format(str) for %s:%s:%s' % (self.resource, self.endpoint, self.type_test))
                elif details['type'] == 'map':
                    if 'key' in details.keys():
                        self._check_expected(response[key], self.test_config_dict[details['key']])
                elif details['type'] == 'from list':
                    values = details['values']
                    self.assertIn(response[key], values, 'value(%s) not in expected values(%s) for %s:%s:%s' % (response[key], ','.join(values), self.resource, self.endpoint, self.type_test))
                elif details['type'] == 'map_list':
                    self._check_expected_map_list(response[key], self.test_config_dict[details['key']], details['key'], indent)
                else:
                    self.assertTrue(False, 'unrecognized type(%s) for %s:%s:%s' % (details['type'], self.resource, self.endpoint, self.type_test))
            else:
                self.assertTrue(False, 'unrecognized details for key(%s) for %s:%s:%s' % (key, self.resource, self.endpoint, self.type_test))

    def _check_response(self, response):
# TODO, only have the json returned by the endpoint
#         self.assertEqual(response.status_code, self.expected_status_code)
        # TODOD: take into account the nested dict in response
        self.assertFalse('ERROR' in response, 'an error occurred for %s:%s:%s: %s' % (self.resource, self.endpoint, self.type_test, response['ERROR']) if 'ERROR' in response else 'no error???')
        self._check_expected(response, self.test_config_dict['expected_response'], '\t')

def run_suite(test_suite, stream, test_name):
    test_results = unittest.TextTestResult(stream = stream, descriptions = True, verbosity = 2)
    test_suite.run(test_results)
    if test_results.wasSuccessful():
        print '%s: SUCCESS' % (test_name)
    else:
        print '%s:' % (test_name)
        test_results.printErrors()
    if 0 < len(cohort_ids):
        print '\t\tWARNING: cohort_ids still exist(%s)' % (','.join(cohort_ids))

def main():
    # final report should include length of all responses and time taken for each test
    parser = argparse.ArgumentParser(description='Unit test module for ISB CGC endpoints')
    parser.add_argument('api_name', help='The name of the API to run the unit tests against')
    parser.add_argument('--test_user_credentials', help='A list of objects containing credentials for test users', nargs = '+')
    args = parser.parse_args()
    
    with open('endpoints_testing/config/{api_config}.json'.format(api_config=args.api_name)) as f:
        json_config = json.load(f)

    endpoints_url_base = json_config['endpoints_url_base']
    for api_name, api_config in json_config['apis'].iteritems():
#         api_config = json_config['apis'][args.api_name]
        discovery_url = endpoints_url_base + api_config['endpoint_uri']
        version = api_config['version']
        base_resource_name = api_config['base_resource_name']
        # get the endpoints test order
        endpoint_test_ordering = []
        
        for reference in api_config['test_ordering']:
            # TODO:  the tests may not be run in the order that they are set
            fields = reference.split('/')
            resource_name = fields[2]
            endpoint_name = fields[-1]
            test_config = resolve(api_config, reference)
            test_config['resource'] = resource_name
            test_config['endpoint'] = endpoint_name
            endpoint_test_ordering.append(test_config)
        
        test_unauthorized = unittest.TestSuite()
        test_authorized = unittest.TestSuite()
        load_test_authorized = unittest.TestSuite()
        
        for test_user_credentials in args.test_user_credentials:
            # add tests without authorization first
            for endpoints_test_config in endpoint_test_ordering:
                resource = endpoints_test_config['resource'] 
                endpoint_name = endpoints_test_config['endpoint']
                requires_auth = endpoints_test_config['requires_auth']
                
                for test_config_name, test_config_dict in endpoints_test_config['test_config'].iteritems():
                    test_config = {
                        'api': api_name, 
                        'version': version, 
                        'endpoint': endpoint_name, 
                        'base_resource': base_resource_name, 
                        'resource': resource, 
                        'discovery_url': discovery_url, 
                        'type_test': test_config_name, 
                        'deletes_resource': endpoints_test_config['deletes_resource'], 
                        'test_config_dict': test_config_dict, 
#                         'expected_response': test_config_dict['expected_response'] if 'expected_response' in test_config_dict else None, 
                        'expected_status_code': test_config_dict['expected_status_code'] if 'expected_status_code' in test_config_dict else None
                    }
                    test_name = endpoints_test_config['test_name'] if 'test_name' in endpoints_test_config else 'test_run'
                    if test_config_name == 'minimal':
                        test_unauthorized.addTest(ParametrizedApiTest.parametrize(IsbCgcApiTest, test_name, test_config, num_requests=1, auth=None))
#                         test_authorized.addTest(ParametrizedApiTest.parametrize(IsbCgcApiTest, test_name, test_config_dict, num_requests=1, auth=test_user_credentials))
#                         load_test_authorized.addTest(ParametrizedApiTest.parametrize(IsbCgcApiTest, test_name, test_config_dict, num_requests=10, auth=test_user_credentials))
#                         load_test_authorized.addTest(ParametrizedApiTest.parametrize(IsbCgcApiTest, test_name, test_config_dict, num_requests=50, auth=test_user_credentials))
#                         load_test_authorized.addTest(ParametrizedApiTest.parametrize(IsbCgcApiTest, test_name, test_config_dict, num_requests=100, auth=test_user_credentials))
#                         load_test_authorized.addTest(ParametrizedApiTest.parametrize(IsbCgcApiTest, test_name, test_config_dict, num_requests=500, auth=test_user_credentials))
#                         load_test_authorized.addTest(ParametrizedApiTest.parametrize(IsbCgcApiTest, test_name, test_config_dict, num_requests=1000, auth=test_user_credentials))
#                     else:
#                         test_authorized.addTest(ParametrizedApiTest.parametrize(IsbCgcApiTest, test_name, test_config_dict, num_requests=1, auth=test_user_credentials))

    stream = _WritelnDecorator(sys.stdout)
    run_suite(test_unauthorized, stream, 'unauthorized')
#     run_suite(test_authorized, stream, 'authorized')
#     cohort_ids = []
#     run_suite(load_test_authorized, stream, 'load_authorized')
#     cohort_ids = []
    
if __name__ == '__main__':
#     main(sys.argv[1:])
    main()
