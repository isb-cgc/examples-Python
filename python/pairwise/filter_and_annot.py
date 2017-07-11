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

'''

from google.cloud import bigquery
import argparse
import sys

# main key order
mko = ['tablevar', 'table']
# annotation key order
ako = [ 'annotvar', 'annot','recordflatten']
# join key order
jko = ['bothvar', 'joinkey', 'filter', 'limit']
# join-with-no-annot key order
nko = ['renamevar', 't1key', 'filter', 'limit']


## Some queries must be annotated before running pairwise
## to this point, some annotation fields are nested
## so we need to check the schema first.
def checkSchemas(client,ffd):
    # have to use a client pointed to the table that you want to query
    ks = list(ffd.keys())
    for x in ['table', 'annot']:
        if x in ks:
            ts = ffd[x].split('.')     # get the table (or annot table) name
            d1 = client.dataset(ts[1]) # get the dataset
            t1 = d1.table(ts[2])
            t1.reload()                # get the schema in t1
            for i in range(0,len(t1.schema)): # for each item in the schema
                if t1.schema[i].name == ffd['valuevar']:
                    ffd['valuetype'] = t1.schema[i].field_type
                if t1.schema[i].field_type == 'RECORD':   # if it's a record then we need extra attention
                    ffd['recordflatten'] = t1.schema[i].name   # keep track of which one it is.
                    for y in ks:
                        # then we need to edit that entry and remove the prefix.
                        if t1.schema[i].name in ffd[y] and (y not in ['filter']):
                            searchString = t1.schema[i].name + '.'
                            z = str(ffd[y])
                            z = z.replace(searchString, '')
                            ffd[y] = z
    return(ffd)


def addItem(ffdict, mode, ki, qid):
    if mode == 'tablevar':
        if 'tablevar' not in ffdict.keys():
            ffdict['tablevar'] = ffdict[ki]
        else:
            ffdict['tablevar'] = ffdict['tablevar'] + ",\n" + ffdict[ki]

    if mode == 'tablevar2':
        if 'tablevar2' not in ffdict.keys():
            ffdict['tablevar2'] = ffdict[ki] + " AS " + ffdict[ki] + "_J" + qid
            ffdict['tablevar_rename'] = ffdict[ki] + "_J" + qid
        else:
            ffdict['tablevar2'] = ffdict['tablevar2'] + ",\n" + ffdict[ki] + " AS " + ffdict[ki] + "_J" + qid
            ffdict['tablevar_rename'] = ffdict['tablevar_rename'] + ",\n" + ffdict[ki] + "_J" + qid

    if mode == 'annotvar':
        if 'annotvar' not in ffdict.keys():
            ffdict['annotvar'] = ffdict[ki]
        else:
            ffdict['annotvar'] = ffdict['annotvar'] + ",\n" + ffdict[ki]

    if mode == 'annotvar2':
        if 'annotvar2' not in ffdict.keys():
            ffdict['annotvar2'] = ffdict[ki] + " AS " + ffdict[ki] + "_J" + qid
            ffdict['annotvar_rename'] = ffdict[ki] + "_J" + qid
        else:
            ffdict['annotvar2'] = ffdict['annotvar2'] + ",\n" + ffdict[ki] + " AS " + ffdict[ki] + "_J" + qid
            ffdict['annotvar_rename'] = ffdict['annotvar_rename'] + ",\n" + ffdict[ki] + "_J" + qid

    if mode == 'groupby':
        if 'groupby' not in ffdict.keys():
            ffdict['groupby'] = ffdict[ki]
        else:
            ffdict['groupby'] = ffdict['groupby'] + ",\n" + ffdict[ki]

    if mode == 'groupby2':
        if 'groupby2' not in ffdict.keys():
            ffdict['groupby2'] = ffdict[ki] + "_J" + qid
        else:
            ffdict['groupby2'] = ffdict['groupby2'] + ",\n" + ffdict[ki] + "_J" + qid

    return(ffdict)


def updateFFdict(ffdict, qid):
    ks = list(ffdict.keys())
    for ki in ks:
        if ki in ['tablekey','tablejoin','tablegroup', 'valuevar']:
            ffdict = addItem(ffdict, 'tablevar', ki, qid)
            ffdict = addItem(ffdict, 'tablevar2', ki, qid)
        if ki in ['annotkey', 'annotjoin', 'annotgroup']:
            ffdict = addItem(ffdict, 'annotvar', ki, qid)
            ffdict = addItem(ffdict, 'annotvar2', ki, qid)
        if ki in ['annotgroup','tablegroup']:
            ffdict = addItem(ffdict, 'groupby', ki, qid)
            ffdict = addItem(ffdict, 'groupby2', ki, qid)
        if ki in ['annotjoin', 'tablejoin']:
            ffdict['joinkey'] = ffdict[ki]
            ffdict['joinkey'] = ffdict[ki] + "_J" + qid
        if ki == 'valuevar':
            ffdict['valuevar2'] = ffdict['valuevar'] + "_J" + qid
    return(ffdict)



# check that dictionary names are
# in the allowed set.
def checkFilterFile(client, ffd, qid):
    # check schemas for records
    ffd = updateFFdict(ffd, qid)
    ffd = checkSchemas(client, ffd)
    return(ffd)


def keyOrder(ffdict, mode):
    ks = list(ffdict.keys())
    if mode == 'maintable':
        kd = [x for x in mko if x in ks]
    elif mode == 'annottable':
        kd = [x for x in ako if x in ks]
    elif mode == 'jointable':
        kd = [x for x in jko if x in ks]
    elif mode == 'noannotjoin':
        kd = [x for x in nko if x in ks]
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
        elif k in ffdict and k in ['idvar', 'valuevar', 'annotvar', 'tablevar', 'tablegroup']:
            ffdict[k] = ffdict[k] + ",\n" + v
        else:
            ffdict[k] = ffdict[k] + " AND " + v
    fin.close()
    return(ffdict)


def buildQuery(client, ffd, mode, qid):
    query = "SELECT \n"
    thisKeyOrder = keyOrder(ffd, mode)
    for key in thisKeyOrder:  # queries need to have a particular order as specified in above lists
        if key in ['idvar', 'valuevar', 'annotvar', 'tablevar']:
            query += ffd[key] + "\n"
        elif key == 'renamevar':
            query += ffd['tablevar2']
        elif key == 'bothvar':
            query += ffd['tablevar2'] + ",\n" + ffd['annotvar2'] +'\n'
        elif key == 'joinkey':
            query += ' FROM T'+ qid +' JOIN A'+ qid +' ON T'+ qid +'.' + ffd['tablekey'] + '= A'+ qid +'.' +ffd['annotkey'] +'\n'
        elif key == 't1key':
            query += ' FROM T'+ qid +'\n'
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


def buildAnnotQuery(q1,q2,q3,qid):
    x = (
    "T"+qid+" AS (\n" +
    q1 +
    "),\n" +

    "A"+qid+" AS (\n" +
    q2 +
    "), \n" +

    "J"+qid+" AS (\n" +
    q3 +
    ") \n"
    )
    return(x)


def buildNoAnnotQuery(q1,q3,qid):
    x = (
    "T"+qid+" AS (\n" +
    q1 +
    "),\n" +

    "J"+qid+" AS (\n" +
    q3 +
    ") \n"
    )
    return(x)


def buildFilterQuery(args, qid):
    if qid == "1":
        ffdict = readFilterFile(args.ff1)
    else:
        ffdict = readFilterFile(args.ff2)
    thisproject = (ffdict['table'].split('.'))[0]
    client = bigquery.Client(project=thisproject)
    ffdict = checkFilterFile(client, ffdict, qid)
    q1 = buildQuery(client, ffdict, "maintable", qid)
    if 'annot' in ffdict.keys():
        # prepare the annotation table, and perform a join
        q2 = buildQuery(client, ffdict, "annottable", qid)
        ffdict['bothvar'] = True # we are going to use both sets of vars (table and annot)
        ffdict['renamevar'] = True
        q3 = buildQuery(client, ffdict, "jointable", qid)
        queryString = buildAnnotQuery(q1,q2,q3,qid)
    else:
        # just query the main table with filters.
        q2 = '' # no annotation
        ffdict['t1key'] = True # not using annotvars
        ffdict['renamevar'] = True
        q3 = buildQuery(client, ffdict, "noannotjoin", qid)
        queryString = buildNoAnnotQuery(q1,q3,qid)
    return(queryString, ffdict)

