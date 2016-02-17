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

# TODO: modify stress test to have a request per count, rather than repeat the same request
# TODO: run one save per six example saves, then for stress test, randomly pick one per count

class IsbCgcApiTestUser(ParametrizedApiTest):
    ''''''
    
