'''
Created on Mar 16, 2016

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

@author: michael
'''
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
from pickle import PicklingError
import random
import sys
import time
import traceback
import unittest

def run_parallel_test(test_name, test_config, count, result):
    from TestIsbCgcApi import IsbCgcApiTest
#     from TestIsbCgcApi import run_suite7
    from TestIsbCgcApi import _WritelnDecorator
    from ParametrizedApiTest import ParametrizedApiTest
    # when launching from a new p[rocess, the stream is closed, so reset the stream in the results
    stream = _WritelnDecorator(sys.stdout)
    result.stream = stream
    test_suite = ParametrizedApiTest.parametrize(IsbCgcApiTest, test_name, test_config, auth=None)
    try:
        test_suite.run(result)
    except Exception as e:
        traceback.print_exc(5)
        result.printErrors()
        return "%s:\tcompleted %s:%s(%s) and failed: %s" % (datetime.now(), test_name, test_config['test']['request'], count, e), count, False
    result.printErrors()
    if 0 < result.errors or 0 < result.failures:
        return "%s:\tcompleted %s:%s(%s) and failed" % (datetime.now(), test_name, test_config['test']['request'], count), count, False
    return "%s:\tcompleted %s:%s(%s)" % (datetime.now(), test_name, test_config['test']['request'], count), count, True
    
class ParallelizeTest(unittest.TestCase):
    '''
    a class to run a set of TestCases in parallel
    '''
    def runParallelTest(self, executor, parallel_count, call_count, wait_count, list_config = None):
        print '==============================================\n\t\trunning concurrency tests(%s:%s)\n==============================================\n' % (parallel_count, call_count)
        print '%s: starting %s pool' % (datetime.now(), 'process' if list_config else 'thread')
        executor = executor(parallel_count)
        max_return = 0
        count_fail = 0
        print '%s: %s pool started' % (datetime.now(), 'process' if list_config else 'thread')
        if list_config:
            # this is not perfect, some processes can still not have the list_test run in them.
            print '%s: start setting up cohort maps' % (datetime.now())
            list_names = ['list_test'] * parallel_count * 2
            list_test = [list_config] * parallel_count * 2
            list_results = [self.result] * parallel_count * 2
            returns = executor.map(run_parallel_test, list_names, list_test, range(parallel_count), list_results)
            for _ in returns:
                pass
            print '%s: finished setting up cohort maps' % (datetime.now())

        try:
#             future_map = {}
            start = time.time()
            test_names = []
            test_configs = []
            for count in range(call_count):
                testcase = random.choice(self.testcases[1])
                print "%s:\tadding %s:%s(%s)" % (datetime.now(), testcase[0], testcase[1]['test']['request'], count)
                test_names += [testcase[0]]
                test_configs += [testcase[1]]
            results = executor.map(run_parallel_test, test_names, test_configs, range(call_count), [self.result] * call_count, timeout = wait_count)
            for result in results:
                max_return = max(max_return, result[1])
                count_fail += 0 if result[2] else 1
                print result[0]
        except PicklingError as pe:
            raise pe
        finally:
            executor.shutdown()
            msg = 'completed %s calls in %s seconds for %s pool (%s:%s)\n\ttotal %s fails\n\t%s tests returned\n' % \
                (call_count, time.time() - start, 'process' if list_config else 'thread', parallel_count, call_count, count_fail, max_return + 1)
            print msg
        return msg

    def parallel_test(self):
        '''
        function that will be called by testsuite.  within this function, invoke the tests using 
        the base class run so that they are run individually and can succeed or fail appropriately
        '''
  
        print '\n==============================================\n\tin parallel_test\n==============================================\n'
        
        print '==============================================\n\t\tsetting up\n==============================================\n'
        timings = ''
        need_lists = True
        for test_name, test_config in self.testcases[0]:
            run_parallel_test(test_name, test_config, 0, self.result)
            if need_lists and 'list_test' == test_name:
                need_lists = False
                list_config = test_config
            
        try:
            timings = ''
            for parallel_count, call_count, wait_count in self.config['parallelize_test']['process_tests']:
                executor = ProcessPoolExecutor
                try:
                    timings += self.runParallelTest(executor, parallel_count, call_count, wait_count, list_config)
                except:
                    traceback.print_exc(5)
        finally:
            print '==============================================\n\t\tcleaning up\n==============================================\n'
            for test_name, test_config in self.testcases[2]:
                run_parallel_test(test_name, test_config, 0, self.result)
            print timings
    
    def run(self, result=None):
        '''
        override to capture the accumulating results
        '''
        self.result = result
        return unittest.TestCase.run(self, result=result)

    def __init__(self, testcases, config):
        '''
        testcases is a list of three lists of TestCases.  the first list are setup test cases, the last is cleanup.
        the middle list is of the test cases to run in parallel
        '''
        super(ParallelizeTest, self).__init__('parallel_test')
        self.testcases = testcases
        self.config = config
        