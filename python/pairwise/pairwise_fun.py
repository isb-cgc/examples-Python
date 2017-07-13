'''
Copyright 2015, Institute for Systems Biology

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

#here we're going to keep the actual pairwise functions.

def selectTest(q3, ffd1, ffd2):
    type1 = ffd1['valuetype']
    type2 = ffd2['valuetype']
    if type1 == 'FLOAT' and type2 == 'FLOAT':
        q4 = spearmans(q3, ffd1, ffd2)
    else:
        q4 = 'ERROR: you have failed at pairwise.'
    return(q4)


# spearman's correlation
# for float vs float

def spearmans(q3, ffd1, ffd2):
    # first rank the data
    thisq =  'ranktable AS (\nSELECT \n '
    thisq += ffd1['groupby2'] + ',\n'
    thisq += ffd2['groupby2'] + ',\n'
    thisq += '  DENSE_RANK() OVER (PARTITION BY ' + ffd1['groupby2'] + ' ORDER BY ' + ffd1['valuevar2'] + ' ASC) as rankvar1,  \n'
    thisq += '  DENSE_RANK() OVER (PARTITION BY ' + ffd2['groupby2'] + ' ORDER BY ' + ffd2['valuevar2'] + ' ASC) as rankvar2  \n'
    thisq += 'FROM\nmainjoin \n'
    thisq += ')\n'
    # then correlate the ranks
    thisq += 'SELECT \n'
    thisq += ffd1['groupby2'] + ',\n'
    thisq += ffd2['groupby2'] + ',\n'
    thisq += '  CORR( rankvar1, rankvar2 ) as Spearmans \n'
    thisq += 'FROM\n '
    thisq += '  ranktable \n'
    thisq += 'GROUP BY \n'
    thisq += ffd1['groupby2'] + ',\n'
    thisq += ffd2['groupby2'] + '\n'
    thisq += 'ORDER BY \n Spearmans DESC \n'
    return(q3 + thisq)


# t-test
# for float vs binary


# anova
# for float vs. more than one


 # Chi-sq or fisher's ?
 # for categorical vs categorical

