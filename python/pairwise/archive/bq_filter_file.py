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

#    table:isb-cgc.tcga_201510_alpha.DNA_Methylation_betas
#    tablevar:Probe_Id
#    annot:isb-cgc.platform_reference.methylation_annotation
#    annotvar:IlmnID
#    idvar:ParticipantBarcode
#    valvar:Beta_Value
#    pivot:UCSC.RefGene_Name  # after the annotation join
#    filter:SampleTypeLetterCode='TP'
#    filter:Study='BRCA'
#    filter:UCSC.RefGene_Name IN ('ACSM5','NAP1L4','SULF2')
#    limit:100

'''

from google.cloud import bigquery
import argparse
import sys

ko = ['idvar', 'valvar', 'pivot', 'table', 'annot', 'tablevar', 'annotvar', 'filter', 'limit']

# Some queries must be annoated before running pairwise
## to this point, some annotation fields are nested
## so we need to check the schema first.
def checkSchemas(client,ffd):
    # have to use a client pointed to the table that you want to query
    ts = ffd['table'].split('.')
    d1 = client.dataset(ts[1])
    t1 = d1.table(ts[2])
    t1.reload()
    # then t1 contains a list of schema fields
    print(t1.schema[0].description)
    print(t1.schema[0].name)
    print(t1.schema[0].field_type)
    print(t1.schema[0].mode)
    # will have to check if any of the fields are records
    # or structs or arrays.


# check that dictionary names are
# in the allowed set.
def checkQuery(client, ffd):
    # make sure the query contains only allowed keys in KO.
    ks = list(ffd.keys())
    if any([x not in ko for x in ks]):
        print("Removing items from the filter file:")
        print([x for x in ks if x not in ko])
    filtered_dict = {key: value for key, value in ffd.items() if key in ko}
    filtered_dict = checkSchemas(client, filtered_dict)
    return(filtered_dict)


def keyOrder(ffdict):
    ks = list(ffdict.keys())
    kd = [x for x in ko if x in ks]
    return(kd)


def readFilterFile(filepath):
    # build a dictionary of query terms
    fin = open(filepath, 'r')
    ffdict = {}
    for line in fin:
        strings = line.strip().split(':')
        k, v = [s.strip() for s in strings]
        if k not in ffdict:
            ffdict[k] = v
        else:
            ffdict[k] = ffdict[k] + " AND " + v
    fin.close()
    return(ffdict)


def buildQuery(client, filename):
    ffd   = readFilterFile(filename)
    ffd   = checkQuery(client, ffd)
    query =  "SELECT \n"
    for key in keyOrder(ffd):  # queries need to have a particular order
        if key in ['idvar', 'valvar']:
            query += ffd[key] + ",\n"
        elif key  == 'table':
            query += "FROM `" + ffd[key] + "`\n WHERE \n"
        elif key == 'limit':
            query += "LIMIT " + ffd[key] + " \n"
        else:
            query += ffd[key] + " \n"
    return(query)


def bq(args):
    client = bigquery.Client(project=args.proj)
    queryString = buildQuery(client, args.ff1)
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
    bq(args)
