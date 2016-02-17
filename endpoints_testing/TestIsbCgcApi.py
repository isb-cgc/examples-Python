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
import traceback
import unittest
from apiclient import discovery
from concurrent import futures
from datetime import datetime
from jsonspec.reference import resolve
from jsonspec.reference.providers import FilesystemProvider

import isb_auth
from TestIsbCgcApiCohort import IsbCgcApiTestCohort
from TestIsbCgcApiFeatureData import IsbCgcApiTestFeatureData
from TestIsbCgcApiFeatureType import IsbCgcApiTestFeatureType
from TestIsbCgcApiMeta import IsbCgcApiTestMeta
from TestIsbCgcApiPairwise import IsbCgcApiTestPairwise
from TestIsbCgcApiSeqpeek import IsbCgcApiTestSeqpeek
from TestIsbCgcApiUser import IsbCgcApiTestUser
from ParametrizedApiTest import ParametrizedApiTest

# TODO: Write browser automation with selenium modules (user login)

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

class IsbCgcApiTest(IsbCgcApiTestCohort, IsbCgcApiTestFeatureData, IsbCgcApiTestFeatureType, IsbCgcApiTestMeta, IsbCgcApiTestPairwise, IsbCgcApiTestSeqpeek, IsbCgcApiTestUser):
    def test_run(self, **kwargs):
        execution_time = 0
        try:
            # authenticate (or not, depending on whether the 'auth' dict contains an entry or is None)
            if self.auth is not None:
                # simulate user login with selenium
                pass
            
            print '\n%s: testing %s.%s as %s.  repeats: %s' % (datetime.now(), self.resource, self.endpoint, self.type_test, self.num_requests)
            all_responses = []
            for test_config_dict in self.test_config_list['tests']:
                self.assertTrue((test_config_dict['request'] and 0 < len(test_config_dict['request'])) or self.endpoint == 'list', 'no request is specified to submit for %s:%s:%s' % (self.resource, self.endpoint, self.type_test))
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
                print '%s:\tget the http request' % (datetime.now())
                # TODO: test adjust for pairwise 'missing' field
                if self.resource:
                    method_to_call = getattr(getattr(getattr(self.service, '{api}'.format(api=self.base_resource))(), '{resource}'.format(resource=self.resource))(), '{method}'.format(method=self.endpoint))
                else:
                    method_to_call = getattr(getattr(self.service, '{api}'.format(api=self.base_resource))(), '{method}'.format(method=self.endpoint))
                print '%s:\tgot the http request' % (datetime.now())
                requests = []
                count = 0
                bad_requests = []
                while count < self.num_requests:
                    try:
                        requests.append(method_to_call(**test_config_dict['request']))
                    except Exception as e:
                        bad_requests += [{'ERROR': e, 'pos': count}]
                    count += 1
                    
                print '%s:\texecute the requests' % (datetime.now())
                responses, execution_time = self._query_api(requests)
                print '%s:\tfinished executing the requests' % (datetime.now())
                for bad_request in bad_requests:
                    responses.insert(bad_request['pos'], bad_request)
                for r in responses:
                    self._check_response(r, test_config_dict, **kwargs)
                all_responses += responses
            return all_responses
        except AssertionError as ae:
            traceback.print_exc()
            raise ae
        except Exception as e:
            traceback.print_exc()
            self.assertTrue(False, 'exception %s raised for %s:%s:%s.  exception is type %s' % (e, self.resource, self.endpoint, self.type_test, type(e)))
        finally:
            # log execution time
            print '%s: finished testing %s.%s as %s.  took %s' % (datetime.now(), self.resource, self.endpoint, self.type_test, execution_time)

    # helper methods
    def _query_api(self, requests):
        def wrap_method_call(*method_to_call):
            return method_to_call[0].execute()

        executor = futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count() * 4)

        start = time.time()
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
        

    def _check_expected_map_list(self, response, expected_response, key, test_config_dict, matchup_key, indent, **kwargs):
        count = 0
        if matchup_key:
            name2maps = {}
            for listmap in response:
                cohorts = name2maps.setdefault(listmap[matchup_key], [])
                cohorts += [listmap]
            name2expected = dict([(expected_map_response['value'], [expected_map_response, expected_map_response[expected_map_response['response_key']]]) for expected_map_response in expected_response])
            for name, maps in name2maps.iteritems():
                self.assertIn(name, name2expected, 'name %s not found as key into the map list for %s:%s:%s' % (name, self.resource, self.endpoint, self.type_test))
                expected_nested_response = name2expected[name]
                for nestedmap in maps:
                    self._check_expected(nestedmap, expected_nested_response[1], expected_nested_response[0], indent + '\t', **kwargs)
        else:
            for nextmap in response:
                if 0 == count % 32:
                    print '%schecking the %s map on the list for %s' % (indent, count, key)
                count += 1
                self._check_expected(nextmap, expected_response, test_config_dict, indent + '\t', **kwargs)
            print '%schecked %s total maps for %s' % (indent, count, key)

    def _check_expected(self, response, expected_response, test_config_dict, indent, **kwargs):
        for key, details in expected_response.iteritems():
            if 'value' in details.keys():
                self.assertEqual(response[key], details['value'], 'value in response for %s isn\'t equal to expected value for %s:%s:%s: %s != %s' % 
                    (key, self.resource, self.endpoint, self.type_test, response[key], details['value']))
            elif 'type' in details.keys():
                if details['type'] == 'string':
                    if 'format' in details.keys():
                        if details['format'] == 'int64':
                            self.assertRegexpMatches(response[key], '^[+-]?[0-9]+$', 'value not in expected format(int64) for %s:%s:%s' % (self.resource, self.endpoint, self.type_test))
                        elif details['format'] == 'date':
                            self.assertRegexpMatches(response[key], '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{6})?$', 'value not in expected format(date) for %s:%s:%s' % 
                                (self.resource, self.endpoint, self.type_test))
                        elif details['format'] == 'email':
                            self.assertRegexpMatches(response[key], '^.+@.+$', 'value not in expected format(email) for %s:%s:%s' % (self.resource, self.endpoint, self.type_test))
                        else:
                            self.assertTrue(False, 'for %s found unexpected format for type string of %s for %s:%s:%s' % (key, details['format'], self.resource, self.endpoint, self.type_test))
                    else:
                        self.assertIsInstance(response[key], basestring, 'value not in expected format(str) for %s:%s:%s' % (self.resource, self.endpoint, self.type_test))
                elif details['type'] == 'map':
                    if 'key' in details.keys():
                        self._check_expected(response[key], test_config_dict[details['key']], test_config_dict, indent, **kwargs)
                    else:
                        self.assertTrue(False, 'for %s expected key for map for %s:%s:%s' % (key, details['type'], self.resource, self.endpoint, self.type_test))
                elif details['type'] == 'list':
                    values = set(response[key])
                    expected_values = set(test_config_dict[details['key']])
                    self.assertSetEqual(values, expected_values)
#                    self.assertSetEqual(values, expected_values, 'value(%s) not in expected values(%s) for %s:%s:%s' % (response[key], ','.join(values), self.resource, self.endpoint, self.type_test))
                elif details['type'] == 'from list':
                    values = details['values']
                    self.assertIn(response[key], values, 'for key %s value(%s) not in expected values(%s) for %s:%s:%s' % (key, response[key], ','.join(values), self.resource, self.endpoint, self.type_test))
                elif details['type'] == 'map_list':
                    if 'optional' not in details or not details['optional']:
                        self._check_expected_map_list(response[key], test_config_dict[details['key']], details['key'], test_config_dict, details['matchup_key'], indent, **kwargs)
                    elif key in response:
                        self._check_expected_map_list(response[key], test_config_dict[details['key']], details['key'], test_config_dict, details['matchup_key'], indent, **kwargs)
                else:
                    self.assertTrue(False, 'unrecognized type(%s) for %s:%s:%s' % (details['type'], self.resource, self.endpoint, self.type_test))
            elif 'value_replace' in details.keys():
                self.assertEqual(response[key], details['value_replace'].format(**kwargs), 'value in response isn\'t equal to expected value for %s:%s:%s: %s != %s' % 
                    (self.resource, self.endpoint, self.type_test, response[key], details['value_replace'].format(**kwargs)))
            else:
                self.assertTrue(False, 'unrecognized details for key(%s) for %s:%s:%s' % (key, self.resource, self.endpoint, self.type_test))

    def _check_response(self, response, test_config_dict, **kwargs):
# TODO, only have the json returned by the endpoint
#         self.assertEqual(response.status_code, self.expected_status_code)
        # TODOD: take into account the nested dict in response
        if 'ERROR' in response:
            assertmsg = 'an error occurred for %s:%s:%s: \'%s\'.' % (self.resource, self.endpoint, self.type_test, response['ERROR'])
            if 'expected_error_response' in test_config_dict:
                self.assertTrue(str(response['ERROR']) == test_config_dict['expected_error_response']['error_msg'], 
                    assertmsg + '  expected \'%s\'' % (test_config_dict['expected_error_response']['error_msg']))
            else:
                self.assertFalse(True, assertmsg)
        else:
            self._check_expected(response, test_config_dict['expected_response'], test_config_dict, '\t', **kwargs)

def print_other_results(test_results):
    if 0 < len(test_results.expectedFailures):
        failure_results = '\texpected failures:\n'
        for failure in test_results.expectedFailures:
            test_name = failure[0]._testMethodName
            except_info = failure[1][failure[1].index('Error: ') + 7:]
            failure_results += '\t\t%s: %s\n' % (test_name, except_info)
        
        print failure_results + test_results.separator1 + '\n'
    if 0 < len(test_results.unexpectedSuccesses):
        failure_results = ''
        for failure in test_results.unexpectedSuccesses:
            failure_results += (',' if 0 < len(failure_results) else '\t\t') + failure._testMethodName
        
        print '\tunexpected successes:\n%s\n%s\n' % (failure_results, test_results.separator1)

def _run_suite(test_suite, stream, test_name):
    test_results = unittest.TextTestResult(stream = stream, descriptions = True, verbosity = 2)
    print '\n%s' % (test_results.separator1)
    print '%s: running %s test' % (datetime.now(), test_name)
    print '%s' % (test_results.separator1)
    start = time.time()
    test_suite.run(test_results)
    total_time = time.time() - start
    print '\n%s' % (test_results.separator1)
    if test_results.wasSuccessful():
        print '%s: results from the %s test: SUCCESS' % (datetime.now(), test_name)
        print_other_results(test_results)
    else:
        print '%s: results from the %s test:' % (datetime.now(), test_name)
        test_results.printErrors()
        print_other_results(test_results)
    print 'test took %s secs' % (total_time)
    print '%s' % (test_results.separator1)

def main():
    # TODO: final report should include length of all responses and time taken for each test
    parser = argparse.ArgumentParser(description='Unit test module for ISB CGC endpoints')
    parser.add_argument('api_name', help='The name of the API to run the unit tests against')
    parser.add_argument('--test_user_credentials', help='A list of objects containing credentials for test users', nargs = '+')
    args = parser.parse_args()
    
    with open('endpoints_testing/config/{api_config}.json'.format(api_config=args.api_name)) as f:
        json_config = json.load(f)

    provider = FilesystemProvider(json_config['api_config_dir'], 'cur:config', aliases = json_config['aliases'])
    endpoints_url_base = json_config['endpoints_url_base']
    test_minimal = unittest.TestSuite()
    test_bad_requests = unittest.TestSuite()
    load_test_authorized = unittest.TestSuite()
    for api_name, api_config in json_config['apis'].iteritems():
        print 'start adding tests for %s' % (api_name)
#         api_config = json_config['apis'][args.api_name]
        discovery_url = endpoints_url_base + api_config['endpoint_uri']
        version = api_config['version']
        base_resource_name = api_config['base_resource_name']
        # get the endpoints test order
        endpoint_test_ordering = []
        for index, reference in enumerate(api_config['test_ordering']):
            ref = '#/test_ordering/%s' % index
            test_config = resolve(api_config, ref, provider)
            fields = reference['$ref'].split('/')
            # TODO: adjust for pairwise 'missing' field
            test_config['resource'] = fields[2]
            test_config['endpoint'] = fields[-1]
            endpoint_test_ordering.append(test_config)
        
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
                        'expected_status_code': test_config_dict['expected_status_code'] if 'expected_status_code' in test_config_dict else None
                    }
                    test_name = test_config_dict['test_name'] if 'test_name' in test_config_dict else 'test_run'
                    if 'tests' not in test_config_dict:
                        test_config_dict['tests'] = {}
                    if test_config_name == 'minimal':
                        test_minimal.addTest(ParametrizedApiTest.parametrize(IsbCgcApiTest, test_name, test_config, num_requests=1, auth=None))
                        load_test_authorized.addTest(ParametrizedApiTest.parametrize(IsbCgcApiTest, test_name, test_config, num_requests=10, auth=test_user_credentials))
                        load_test_authorized.addTest(ParametrizedApiTest.parametrize(IsbCgcApiTest, test_name, test_config, num_requests=50, auth=test_user_credentials))
                        load_test_authorized.addTest(ParametrizedApiTest.parametrize(IsbCgcApiTest, test_name, test_config, num_requests=100, auth=test_user_credentials))
                        load_test_authorized.addTest(ParametrizedApiTest.parametrize(IsbCgcApiTest, test_name, test_config, num_requests=500, auth=test_user_credentials))
                        load_test_authorized.addTest(ParametrizedApiTest.parametrize(IsbCgcApiTest, test_name, test_config, num_requests=1000, auth=test_user_credentials))
                    elif test_config_name == 'bad_requests':
                        test_bad_requests.addTest(ParametrizedApiTest.parametrize(IsbCgcApiTest, test_name, test_config, num_requests=1, auth=test_user_credentials))
        print 'finished adding tests for %s' % (api_name)

    stream = _WritelnDecorator(sys.stdout)
    _run_suite(test_minimal, stream, 'minimal')
    _run_suite(test_bad_requests, stream, 'bad requests')
#     _run_suite(load_test_authorized, stream, 'load authorized')
#     cohort_ids = []
    
if __name__ == '__main__':
#     main(sys.argv[1:])
    main()
