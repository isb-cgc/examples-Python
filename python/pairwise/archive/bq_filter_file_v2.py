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

******************************************************
Using python to generate bigqueries.

Here we use the 'filter file' to create subusets of data
to download.
******************************************************

First need to install the BigQuery API
pip3 install --upgrade google-cloud-bigquery

The first time I ran the installer, there was an error. But just running pip3
again seemed to work.

Also we need to get authenticated. At the command line we:
gcloud auth application-default login

table:isb-cgc.tcga_201607_beta.DNA_Methylation_chr11
tablevar:Probe_Id
tablevar:ParticipantBarcode
tablevar:Beta_Value
tablekey:Probe_Id
annot:isb-cgc.platform_reference.methylation_annotation
annotvar:IlmnID
annotvar:UCSC.RefGene_Name
annotkey:IlmnID
filter:SampleTypeLetterCode='TP'
filter:Study='BRCA'
filter:UCSC.RefGene_Name IN ('ACSM5','NAP1L4','SULF2')
limit:100



'''

from google.cloud import bigquery
import argparse
import sys

# main key order
mko = ['tablevar', 'table' ]
# annotation key order
ako = [ 'annotvar','annot','recordflatten']
# join key order
jko = ['bothvars', 'joinkey', 'filter', 'limit']


# Some queries must be annoated before running pairwise
## to this point, some annotation fields are nested
## so we need to check the schema first.
def checkSchemas(client,ffd):
    # have to use a client pointed to the table that you want to query
    ks = list(ffd.keys())
    for x in ['table', 'annot']:
        if x in ks:
            ts = ffd[x].split('.')
            d1 = client.dataset(ts[1])
            t1 = d1.table(ts[2])
            t1.reload()
            # then t1 contains a list of schema fields
            for i in range(0,len(t1.schema)):
                if t1.schema[i].field_type == 'RECORD':
                    ffd['recordflatten'] = t1.schema[i].name
                    for y in ks:
                        # then we need to edit that entry and remove the prefix.
                        if t1.schema[i].name in ffd[y] and (y not in ['filter','pivot']):
                            searchString = t1.schema[i].name + '.'
                            z = str(ffd[y])
                            print("search string: " + searchString)
                            print("type: " + str(type(z)))
                            print("remove prefix for " + z)
                            z = z.replace(searchString, '')
                            print(z)
                            ffd[y] = z
    return(ffd)


# check that dictionary names are
# in the allowed set.
def checkFilterFile(client, ffd):
    # check schemas for records
    ffd = checkSchemas(client, ffd)
    return(ffd)


def keyOrder(ffdict, mode):
    ks = list(ffdict.keys())
    if mode == 'maintable':
        kd = [x for x in mko if x in mko]
    elif mode == 'annottable':
        kd = [x for x in ako if x in ako]
    elif mode == 'jointable':
        kd = [x for x in jko if x in jko]
    else:
        kd = []
    return(kd)


def readFilterFile(filepath):
    # build a dictionary of query terms
    # the filter entries are concatenated
    fin = open(filepath, 'r')
    ffdict = {}
    for line in fin:
        strings = line.strip().split(':')
        k, v = [s.strip() for s in strings]
        if k not in ffdict:
            ffdict[k] = v
        elif k in ffdict and k in ['idvar', 'valvar', 'annotvar', 'tablevar']:
            ffdict[k] = ffdict[k] + ",\n" + v
        else:
            ffdict[k] = ffdict[k] + " AND " + v
    fin.close()
    return(ffdict)


def buildQuery(client, ffd, mode):
    query =  "SELECT \n"
    thisKeyOrder = keyOrder(ffd, mode)
    for key in thisKeyOrder:  # queries need to have a particular order
        if key in ['idvar', 'valvar', 'annotvar', 'tablevar']:
            query += ffd[key] + "\n"
        elif key == 'bothvars':
            query += ffd['tablevar'] + ',\n' + ffd['annotvar'] +'\n'
        elif key == 'joinkey':
            query += ' FROM T1 JOIN A1 ON T1.' + ffd['tablekey'] + '= A1.' +ffd['annotkey'] +'\n'
        elif key  == 'filter':
            query += "WHERE \n" + ffd[key] +'\n'
        elif (key  == 'table' or key == 'annot') and 'filter' not in thisKeyOrder:
            query += "FROM `" + ffd[key] + "`\n"
        elif key == 'limit':
            query += "LIMIT " + ffd[key] + " \n"
        elif key == 'recordflatten':
            query += ", UNNEST(" + ffd[key] +")\n"
        else:
            query += ffd[key] + " \n"
    return(query)


def buildAnnotQuery(q1,q2,q3):
    x = (
    "WITH\n" +
    "T1 AS (\n" +
    q1 +
    "),\n" +
    "A1 AS (\n" +
    q2 +
    ") \n" +
    q3
    )
    return(x)


def buildFilterQuery(args):
    client = bigquery.Client(project=args.prj)
    ffdict = readFilterFile(args.ff1)
    ffdict = checkFilterFile(client, ffdict)
    q1 = buildQuery(client, ffdict, "maintable")
    if 'annot' in ffdict.keys():
        # prepare the annotation table, and perform a join
        q2 = buildQuery(client, ffdict, "annottable")
        q3 = buildQuery(client, ffdict, "jointable")
        queryString = buildAnnotQuery(q1,q2,q3)
    else:
        # just query the main table with filters.
        q2 = '' # no annotation
        q3 = '' # no joins
        queryString = q1
    print("*****************************************")
    print(queryString)
    print("*****************************************")
    #query_results = client.run_sync_query(queryString)
    #query_results.use_legacy_sql = False
    #query_results.run()
    #print(query_results.total_rows)
    #for qi in query_results.rows:
    #    print(qi)
    print("done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BigQuery PairWise")
    parser.add_argument("prj", help="google project ID")
    parser.add_argument("ff1", help="filter file")
    args = parser.parse_args()
    buildFilterQuery(args)
