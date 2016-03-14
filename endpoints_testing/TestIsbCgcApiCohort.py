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
from ParametrizedApiTest import ParametrizedApiTest

# TODO: Write browser automation with selenium modules (user login)

cohort_id2cohort_name = {} 

# TODO: modify stress test to have a request per count, rather than repeat the same request
# TODO: run one save per six example saves, then for stress test, randomly pick one per count

class IsbCgcApiTestCohort(ParametrizedApiTest):
    def set_cohort_id(self, test_config_dict):
        found_count = 0
        if 'cohort_id' in test_config_dict['request'] and 'cohort_name_lookup' in test_config_dict:
            notfound = True
            for cohort_id, cohort_name in cohort_id2cohort_name.iteritems():
                if cohort_name == test_config_dict['cohort_name_lookup']:
                    notfound = False
                    found_count += 1
                    test_config_dict['request']['cohort_id'] = cohort_id
                    break
            
            if notfound:
                self.assertTrue(False, 'didn\'t find a cohort id for %s' % (test_config_dict['cohort_name_lookup']))
        return found_count

    def cohort_patients_samples_list_test(self):
        # based on the cohort name in the config file, need to get an id
        for test_config_dict in self.test_config_list['tests']:
            self.set_cohort_id(test_config_dict)
        self.test_run()

    def datafilenamekey_list_from_cohort_test(self):
        return
        # based on the cohort name in the config file, need to get an id
        for test_config_dict in self.test_config_list['tests']:
            self.set_cohort_id(test_config_dict)
        self.test_run()
        
    def datafilenamekey_list_from_sample_test(self):
        self.test_run()
        
    def datafilenamekey_list_from_sample_error_test(self):
        self.test_run()
        
    def sample_details_test(self):
        self.test_run()
        
    def sample_details_error_test(self):
        self.test_run()
    
    def patient_details_test(self):
        self.test_run()
    
    def patient_details_error_test(self):
        self.test_run()
    
    def list_test(self):
        found_count = 0
        for test_config_dict in self.test_config_list['tests']:
            found_count += self.set_cohort_id(test_config_dict)
        responses = self.test_run()
        if not responses:
            return
        cohort_count = int(responses[0]['count'])
        if 'items' in responses[0]:
            for item in responses[0]['items']:
                if item['perm'] != 'OWNER':
                    cohort_count -= 1
        print '\tfound %s cohorts' % (cohort_count)
        if 0 == found_count:
            global cohort_id2cohort_name
            cohort_id2cohort_name = {}
            if 'items' in responses[0]:
                for item in responses[0]['items']:
                    if 'OWNER' == item['perm']:
                        cohort_id2cohort_name[item['id']] = item['name']
            self.assertEqual(cohort_count, len(cohort_id2cohort_name), '%s:%s:%s returned a count(%s) different than the number of items(%s)' % 
                (self.resource, self.endpoint, self.type_test, cohort_count, len(cohort_id2cohort_name) if 'items' in responses[0] else 0))
        
    def delete_test(self):
        count = 0
        try:
            cohort_ids = []
            for cohort_id in cohort_id2cohort_name:
                count += 1
                cohort_ids += [cohort_id]
                # TODO: think about how to make this more general
                self.test_config_list['tests'][0]['request']['cohort_id'] = cohort_id
                self.num_requests = 1
                self.test_run(**{"cohort_id": cohort_id})
        finally:
            print 'deleted %s cohorts out of %s' % (count, len(cohort_id2cohort_name))
        for cohort_id in cohort_ids:
            cohort_id2cohort_name.pop(cohort_id)
    
    def preview_test(self):
        responses = self.test_run()
        self.assertTrue(8 == len(responses))
        
    def preview_error_test(self):
        self.test_run()
        
    def save_test(self):
        self.test_run()

    def google_genomics_from_cohort_test(self):
        # based on the cohort name in the config file, need to get an id
        for test_config_dict in self.test_config_list['tests']:
            self.set_cohort_id(test_config_dict)
        self.test_run()
    
    def google_genomics_from_sample_test(self):
        self.test_run()
    