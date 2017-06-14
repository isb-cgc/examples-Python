# Using python to generate bigqueries, as an application

# First need to install the BigQuery API
# pip3 install --upgrade google-cloud-bigquery

# The first time I ran the installer, there was an error. But just running pip3
# again seemed to work.

# Also we need to get authenticated. At the command line we:
# gcloud auth application-default login

#    table=isb-cgc.tcga_201510_alpha.DNA_Methylation_betas
#    tablevar=Probe_Id
#    annot=isb-cgc.platform_reference.methylation_annotation
#    annotvar=IlmnID
#    idvar=ParticipantBarcode
#    valvar=Beta_Value
#    pivot=UCSC.RefGene_Name  # after the annotation join
#    filter=SampleTypeLetterCode='TP'
#    filter=Study='BRCA'
#    filter=UCSC.RefGene_Name IN ('ACSM5','NAP1L4','SULF2')
#    limit=100


from google.cloud import bigquery
import argparse
import sys

ko = ['idvar', 'valvar', 'pivot', 'table', 'filter', 'limit']

# check that dictionary names are
# in the allowed set.
def checkQuery(ffd):
    ks = list(ffd.keys())
    if any([x not in ko for x in ks]):
        print("To Err is Human: please check your filter file.")
        print("Allowable keys include:")
        print(ko)
        sys.exit(2)
    return()


def keyOrder(ffdict):
    ks = list(ffdict.keys())
    kd = [x for x in ko if x in ks]
    return(kd)


def buildQuery(filename):
    ffd   =  readFilterFile(filename)
    checkQuery(ffd)
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


def readFilterFile(filepath):
    f = open(filepath, 'r')
    ffdict = {}
    for line in f:
        strings = line.strip().split(':')
        print(strings)
        k, v = [s.strip() for s in strings]
        if k not in ffdict:
            ffdict[k] = v
        else:
            ffdict[k] = ffdict[k] + " AND " + v
    f.close()
    return(ffdict)


def bq(args):
    print(args)
    client = bigquery.Client(project=args.proj)
    queryString = buildQuery(args.ff1)
    print(queryString)
    query_results = client.run_sync_query(queryString)
    query_results.use_legacy_sql = False
    query_results.run()
    print(query_results.total_rows)
    for qi in query_results.rows:
        print(qi)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BigQuery PairWise")
    parser.add_argument("proj", help="google project ID")
    parser.add_argument("ff1", help="filter file")
    args = parser.parse_args()
    bq(args)
