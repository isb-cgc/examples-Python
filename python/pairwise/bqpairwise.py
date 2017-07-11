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

Here we use the 'filter file' to create SQL
******************************************************

First need to install the BigQuery API
pip3 install --upgrade google-cloud-bigquery

The first time I ran the installer, there was an error. But just running pip3
again seemed to work.

Also we need to get authenticated. At the command line we:
gcloud auth application-default login

<file1>
table:isb-cgc.TCGA_hg19_data_v0.DNA_Methylation_chr16
tablekey:probe_id
tablevar:project_short_name
joinkey:case_barcode
valuevar:beta_value
annot:isb-cgc.platform_reference.methylation_annotation
annotkey:IlmnID
groupby:UCSC.RefGene_Name
filter:project_short_name='TCGA-BRCA'
filter:RefGene_Name IN ('ACSM5','NAP1L4','SULF2')
limit:100

<file2>
table:isb-cgc.TCGA_hg19_data_v0.RNAseq_Gene_Expression_UNC_RSEM
tablejoin:case_barcode
valuevar:normalized_count
filter:project_short_name='TCGA-BRCA'
filter:HGNC_gene_symbol IN ('ACSM5','NAP1L4','SULF2')
limit:100


<run it like>
python3 bqpairwise.py isb-cgc filter_file_1.txt filter_file_2.txt
'''

import filter_and_annot as fa
import pairwise_fun as pf
from google.cloud import bigquery
import argparse
import sys


def mainJoin(ffd1, ffd2):
    # joins the two filter queries
    q =  'mainjoin AS ( \nSELECT '
    q += ffd1['valuevar'] + ',\n'
    q += ffd2['valuevar'] + ',\n'
    q += ffd1['groupby']  + ',\n'
    q += ffd2['groupby']  + ' \n'  # both need a groupby #
    q += 'FROM' + '\n'
    q += 'J1 JOIN J2 ON \n'
    q += 'J1.'+ffd1['joinkey'] + ' = ' + 'J2.' + ffd2['joinkey'] + ' AND \n'
    q += 'J1.'+ffd1['groupby'] + ' < ' + 'J2.' + ffd2['groupby'] + '\n),\n' # will be another two tables
    return(q)


def mainFun(args):
    # constructs each query, then joins the two queries,
    q1,ffd1 = fa.buildFilterQuery(args, "1")
    q2,ffd2 = fa.buildFilterQuery(args, "2")
    q3 = 'WITH\n' + q1 + ',\n' + q2 + ',\n' + mainJoin(ffd1,ffd2)
    q4 = pf.selectTest(q3, ffd1, ffd2)
    print(q4)
    # query_results = client.run_sync_query(queryString)
    # query_results.use_legacy_sql = False
    # query_results.run()
    # print(query_results.total_rows)
    # for qi in query_results.rows:
    # print(qi)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BigQuery PairWise")
    parser.add_argument("prj", help="google project ID")
    parser.add_argument("ff1", help="filter file")
    parser.add_argument("ff2", help="filter file")
    args = parser.parse_args()
    mainFun(args)
