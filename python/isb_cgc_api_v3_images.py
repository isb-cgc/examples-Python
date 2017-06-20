'''
Created on Jun 20, 2017

Copyright 2017, Institute for Systems Biology.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

@author: michael
'''
import pprint

from isb_cgc_api_v3_cohorts import cloud_storage_file_paths, list, get_authorized_service

def main():
    cohorts = list(get_authorized_service(''))
    pprint.pprint(cohorts)
    cohort_id = None
    for item in cohorts['items']:
        if 'All TCGA Data' == item['name']:
            cohort_id = item['id']
    if not cohort_id:
        raise ValueError('didn\'t find the ALL TCGA Data cohort')
    
    paths = cloud_storage_file_paths(get_authorized_service(''), cohort_id, data_type='Tissue slide image')
    if 0 < paths['count']:
        pprint.pprint(paths['cloud_storage_file_paths'][:10])
    else:
        pprint.pprint('no rows returned')

if __name__ == '__main__':
    main()
