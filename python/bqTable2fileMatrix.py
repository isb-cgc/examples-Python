import argparse
import fnmatch
import json
import os
import pandas as pd
import sys
import time
import uuid

from apiclient import discovery
from apiclient import errors
from gcloud    import storage
from oauth2client.client import GoogleCredentials

#------------------------------------------------------------------------------
# extract a list of the tables referred to in the input SQL query

def getTableRefs ( queryString ):

    tableList = []

    ii = 0
    done = 0
    while not done:
        ii = queryString.find("FROM",ii)
        if ( ii > 0 ):
            ## print " "
            ## print " "
            ## print ii, queryString[ii:ii+60]
            tString = queryString[ii+5:]
            tString.strip()
            tList = tString.split(' ')
            ## print len(tList), tList
            tableName = tList[0]
            if ( tableName[0] == '[' ): tableName = tableName[1:]
            if ( tableName[-1] == ']' ): tableName = tableName[:-1]
            tableList += [ tableName ]
            ii += 5
        else:
            done = 1

    return ( tableList )

#------------------------------------------------------------------------------
# read a multi-line SQL query from the input file and return as a string

def getBQueryFromFile ( aFilename, verboseFlag ):

    fh = file ( aFilename, 'r' )

    queryString = ''
    for aLine in fh:
        aLine = aLine.strip()
        if ( aLine[-1] == '\n' ): aLine = aLine[:-1]
        queryString += aLine + " "

    fh.close()

    if ( verboseFlag ):
        print " "
        print " input SQL query : "
        print " %s " % queryString 
        print " "

    tableList = getTableRefs ( queryString )

    print " "
    print " list of required input BigQuery tables : ", tableList
    print " "

    return ( queryString, tableList )

#------------------------------------------------------------------------------
# parse an input BQ table name
#     tableName should be of the form <gcp-project>:<dataset-name>.<table-name>
#                                  or <dataset-name>.<table-name>
# if the tableName does not 

def parseTableName ( tableName, projID ):

    # start by looking for ':' separator to see if the tableName
    # includes the project-id
    ii = tableName.find(':')
    if ( ii > 0 ):
        currProjID = tableName[:ii]
        if ( currProjID != projID ):
            print " NOTE that the specified table will be owned by a different GCP project "
            print projID, currProjID
    else:
        currProjID = projID
        ii = -1

    # next, look for the '.' separator to split the dataset-name from the table-name
    jj = tableName.find('.',ii+1)
    if ( jj > 0 ):
        currDatasetID = tableName[ii+1:jj]
        tableNameID = tableName[jj+1:]
    else:
        print " ERROR ??? the specified table name does not look correct "
        print " should look like, eg: isb-cgc:test.scratchTable "
        print " ( ie <project-name>:<dataset-name>.<table-name> "
        print " instead of: %s " % tableName
        sys.exit(-1)

    ## print " "
    ## print " --> returning : ", currProjID, currDatasetID, tableNameID
    return ( currProjID, currDatasetID, tableNameID )

#------------------------------------------------------------------------------
# submit the query using the 'dryRun' mode in order to estimate the cost
# of this query

def submitDryRunQueryJob ( BQsvc, queryString, billingProjID, \
                           BQdestProjID, BQdestDatasetID, BQdestTableID ):

    jobInfo = {
        'jobReference': {
            'projectId': billingProjID,
            'jobId': str(uuid.uuid4())
        },
        'configuration': {
            'query': {
                'query': queryString,
                'writeDisposition': 'WRITE_TRUNCATE',
                'destinationTable': {
                    'projectId': BQdestProjID,
                    'datasetId': BQdestDatasetID,
                    'tableId':   BQdestTableID
                },
                'allowLargeResults': True,
                'useQueryCache': True,
                'priority': 'INTERACTIVE'
            },
            'dryRun': True,
        }
    }

    numRetries = 3
    print " "
    print " submitting query to BigQuery in DRY-RUN mode "
    response = BQsvc.jobs().insert ( projectId=billingProjID, body=jobInfo ).execute(num_retries=numRetries)
    ## print json.dumps ( response, indent=4 )

    try:
        if ( response['status']['state'] == "DONE" ):
            totalBytes = int(str(response['statistics']['totalBytesProcessed']))
            print " amount of data to be scanned : %.1f MB " % ( float(totalBytes) / 1000000000. )
            costEstimate = 5. * float(totalBytes) / 1000000000000.
            print "      estimated cost of query : $ %.2f " % costEstimate
            print " "
            print " "
            return ( costEstimate )
    except:
        print " ERROR getting information from dry-run attempt ??? "
        sys.exit(-1)

#------------------------------------------------------------------------------
# now run the 'real' job

def submitRealQueryJob ( BQsvc, queryString, billingProjID, \
                         BQdestProjID, BQdestDatasetID, BQdestTableID ):

    jobInfo = {
        'jobReference': {
            'projectId': billingProjID,
            'jobId': str(uuid.uuid4())
        },
        'configuration': {
            'query': {
                'query': queryString,
                'writeDisposition': 'WRITE_TRUNCATE',
                'destinationTable': {
                    'projectId': BQdestProjID,
                    'datasetId': BQdestDatasetID,
                    'tableId':   BQdestTableID
                },
                'allowLargeResults': True,
                'useQueryCache': True,
                'priority': 'INTERACTIVE'
            }
        }
    }

    numRetries = 3
    print " submitting query to BigQuery job-queue now "
    return ( BQsvc.jobs().insert ( projectId=billingProjID, body=jobInfo ).execute(num_retries=numRetries) )

#------------------------------------------------------------------------------

def deleteBQtable ( BQsvc, ProjID, DatasetID, TableID ):

    try:
        print " deleting temporary table %s:%s.%s " % ( ProjID, DatasetID, TableID )
        BQsvc.tables().delete ( projectId=ProjID, datasetId=DatasetID, tableId=TableID ).execute()
    except:
        print " ERROR in deleteBQtable ... failed to delete temporary table %s:%s.%s " % ( ProjID, DatasetID, TableID )

#------------------------------------------------------------------------------
# export the BQ table that was created two the specified filename(s) in 
# Google Cloud Storage (GCS)

def exportBQtable2GCS ( BQsvc, GCSpath, billingProjID,
                   srcProjID, srcDatasetID, srcTableID,
                   exportFormat="CSV",
                   numRetries=5 ):

    print " "
    print " exporting to ", GCSpath

    if ( GCSpath.find(".csv") < 0 ):
        print " ERROR ... GCS wildcard path should include .csv "
        sys.exit(-1)

    if ( GCSpath.endswith(".gz") ):
        compression = "GZIP"
        print " output will be one or more GZIP compressed CSV files "
    else:
        compression = "NONE"
        print " output will be un-compressed CSV files "

    jobInfo = {
        'jobReference': {
            'projectId': billingProjID,
            'jobId': str(uuid.uuid4())
        },
        'configuration': {
            'extract': {
                'sourceTable': {
                    'projectId': srcProjID,
                    'datasetId': srcDatasetID,
                    'tableId':   srcTableID,
                },
                'destinationUris': [GCSpath],
                'destinationFormat': exportFormat,
                'compression': compression,
                'priority': 'INTERACTIVE'
            }
        }
    }

    # insert and execute the job within the return
    return ( BQsvc.jobs().insert(projectId=billingProjID, body=jobInfo).execute(num_retries=numRetries) )

#------------------------------------------------------------------------------

def pollJob ( BQsvc, jobInfo ):

    ## print " building request to ask about jobId ", jobInfo['jobReference']['jobId']
    request = BQsvc.jobs().get(
        projectId=jobInfo['jobReference']['projectId'], 
        jobId=jobInfo['jobReference']['jobId'])

    while True:
        if ( 0 ):
            print "     checking job status ... "
        response = request.execute(num_retries=2)
        if ( 0 ):
            print " >>>> "
            print json.dumps ( response, indent=4 )
            print " <<<< "
        if ( 1 ):
            try:
                sys.stdout.write('.')
                sys.stdout.flush()
                ## print response['status']['state']
            except:
                continue

        if response['status']['state'] == 'DONE':
            if 'errorResult' in response['status']:
                raise RuntimeError(response['status']['errorResult'])
            print('Job complete.')
            return

        time.sleep(2)

    print " "

#------------------------------------------------------------------------------

def splitGCSpath ( gcsPath ):

    if ( gcsPath.startswith("gs://") ):
        ii = gcsPath.find("/",5)
        if ( ii > 0 ): 
            return ( gcsPath[5:ii], gcsPath[ii+1:] )

    print " ERROR ??? unexpected gcsPath ??? <%s> " % gcsPath

#------------------------------------------------------------------------------

def matchesWildCardPath ( aName, bNamePattern ):

    ## print " want to compare <%s> to <%s> " % ( aName, bNamePattern )

    ii = bNamePattern.find('*')
    if ( ii < 0 ):
        print " ERROR ??? name pattern has no wildcard ??? <%s> " % bNamePattern
        sys.exit(-1)

    partA = bNamePattern[:ii]
    partB = bNamePattern[ii+1:]
    ## print " <%s> <%s> " % ( partA, partB )

    if ( aName.startswith(partA) and aName.endswith(partB) ):
        ## print " matches !!! ", aName, bNamePattern
        return ( True )
    else:
        ## print " NO match "
        return ( False )

#------------------------------------------------------------------------------

def makeLocalFilename ( localDir, gsName ):

    ## print " cloud blob name <%s> " % gsName
    if ( gsName.find('/') >= 0 ):
        ii = len(gsName) - 1
        while ( gsName[ii] != '/' ): ii -= 1
        localName = gsName[ii+1:]
    else:
        localName = gsName

    ## print " local name <%s> " % localName
    if ( localDir[-1] != "/" ): localDir += "/"
    localName = localDir + localName

    return ( localName )

#------------------------------------------------------------------------------

def deleteGCSobj ( gcsPath ):

    storageClient = storage.Client()
    ( bucketName, oName ) = splitGCSpath ( gcsPath )
    ## print " --> ", bucketName, oName
    bucket = storageClient.get_bucket ( bucketName )
    blob = bucket.blob ( oName )
    ## print " calling delete "
    blob.delete()

#------------------------------------------------------------------------------

def makeDFfromShards ( gcsPath, localDir, verboseFlag ):

    if ( verboseFlag ):
        print " input gcsPath : <%s> " % gcsPath
        print " calling storage.Client ... "

    storageClient = storage.Client()

    ( bucketName, bNamePattern ) = splitGCSpath ( gcsPath )
    if ( verboseFlag ):
        print " bucketName : <%s> " % bucketName
        print " bNamePattern : <%s> " % bNamePattern

    bucket = storageClient.get_bucket ( bucketName )
    blobs = bucket.list_blobs()

    nFiles = 0
    for b in blobs:

        if ( not matchesWildCardPath ( b.name, bNamePattern ) ):
            continue

        localName = makeLocalFilename ( localDir, b.name )
        if ( verboseFlag ): print "     calling download_to_filename ... ", b.name
        b.download_to_filename ( localName )

        aFile = localName

        ## print " calling read_csv ... "
        shard_df = pd.read_csv ( aFile, delimiter=',', compression='infer' )

        ## make sure that this shard has only TWO dimensions
        if ( shard_df.ndim != 2 ):
            print " ERROR: number of dimensions should be TWO "
            print "        error from file <%s> " % aFile
            sys.exit(-1)

        ## we also assume that each input file has 3 columns (row,col,val)
        if ( shard_df.shape[1] != 3 ):
            print " ERROR: second dimension should be THREE "
            print "        error from file <%s> " % aFile
            sys.exit(-1)

        ## print "     --> %d rows read ... " % shard_df.shape[0]

        if ( nFiles == 0 ):
            cum_df = shard_df
            fieldNames = shard_df.axes[1]
            ## print fieldNames
            rowName = fieldNames[0]
            colName = fieldNames[1]
            datName = fieldNames[2]

            print " "
            print " the output file will contain a 2D matrix in which: "
            print "       the <%s> field will be the output matrix ROW " % rowName
            print "       the <%s> field will be the output matrix COLUMN " % colName
            print "       the <%s> field will be the output matrix CELL values " % datName
            print " "

        else:

            ## make sure that the new shard is consistent with 
            ## the first one ...
            fieldNames = shard_df.axes[1]
            if ( rowName != fieldNames[0] ):
                print " ERROR: inconsistent rowName ... <%s> <%s> " % ( rowName, fieldNames[0] )
                print "        error from file <%s> " % aFile
                sys.exit(-1)
            if ( colName != fieldNames[1] ):
                print " ERROR: inconsistent colName ... <%s> <%s> " % ( colName, fieldNames[1] )
                print "        error from file <%s> " % aFile
                sys.exit(-1)
            if ( datName != fieldNames[2] ):
                print " ERROR: inconsistent datName ... <%s> <%s> " % ( datName, fieldNames[2] )
                print "        error from file <%s> " % aFile
                sys.exit(-1)

            ## print " concatenating with existing dataframe ... "
            cum_df = pd.concat ( [cum_df, shard_df] )
            ## print " --> total # of rows is now: %d " % cum_df.shape[0]

        nFiles += 1

        ## now that we don't need this file anymore, we can delete it 
        ## both in GCS and locally
        deleteShardFile ( gcsPath, localDir, aFile, verboseFlag )

    return ( cum_df )

#------------------------------------------------------------------------------

def readShards_write2dTSV ( gcsPath, localDir, outFile, verboseFlag ):

    print " "
    print " "

    if ( verboseFlag ):
        print " "
        print " getting file shards from %s " % gcsPath

    ## now we're going to get each of the shard files and create
    ## a dataframe from all of the shards ...
    df = makeDFfromShards ( gcsPath, localDir, verboseFlag )

    if ( verboseFlag ):
        print " "
        print " "
        print " info about our data frame : "
        df.info()

        print " dtypes ... "
        print ( df.dtypes )

        print " ftypes ... "
        print ( df.ftypes )

    fieldNames = df.axes[1]
    rowName = fieldNames[0]
    colName = fieldNames[1]
    datName = fieldNames[2]

    if ( verboseFlag ): print " getting all rowName values ... "
    allRowNames = pd.Series(df[rowName])
    unqRowNames = allRowNames.unique()
    unqRowNames.sort()

    if ( verboseFlag ):
        print "     --> in %d rows, we have %d unique row labels " % \
                ( len(allRowNames), len(unqRowNames) )
        print " "
        print " "

    if ( verboseFlag ): print " getting all colName values ... "
    allColNames = pd.Series(df[colName])
    unqColNames = allColNames.unique()
    unqColNames.sort()
    if ( verboseFlag ):
        print "     --> in %d rows, we have %d unique col labels " % \
                ( len(allColNames), len(unqColNames) )
        print " "
        print " "

    if ( verboseFlag ): print " now calling pivot : "
    new_df = df.pivot ( index=rowName, columns=colName, values=datName )
    if ( verboseFlag ): print " done with pivot operation "

    if ( verboseFlag ):
        print " " 
        print " new ndim : ", ( new_df.ndim )

    if ( new_df.ndim !=2 ):
        print " ERROR ??? the new number of dimensions should be exactly 2 "
        sys.exit(-1)

    if ( verboseFlag ):
        print " new shape : ", ( new_df.shape )
        print " new axes : ", ( new_df.axes )
        print " number of missing values : ", new_df.isnull().values.ravel().sum()
        ## print ( new_df.describe() )

    print " "
    if ( outFile.endswith(".gz") ): outFile = outFile[:-3]
    if ( not outFile.endswith(".tsv") ): outFile += ".tsv"
    print " writing new matrix to output file name <%s> " % outFile
    new_df.to_csv ( path_or_buf=outFile, sep='\t', na_rep='NA', float_format='%.3f' )

    return ( shardFiles )

#------------------------------------------------------------------------------

def deleteShardFile ( gcsPath, localDir, aShard, verboseFlag ):

    if ( verboseFlag ):
        print " "
        print " in deleteShardFile ... "
        print "        GCS path  : ", gcsPath
        print "        local dir : ", localDir

    ii = len(gcsPath) - 1
    while ( gcsPath[ii] != '/' ): ii -= 1
    gcsPrefix = gcsPath[:ii]
    ## print " gcsPrefix : ", gcsPrefix

    gcsFile = gcsPrefix + "/" + aShard[len(localDir):]
    if ( verboseFlag ): print "             deleting GCS object ", gcsFile
    deleteGCSobj ( gcsFile )
    if ( verboseFlag ): print "             deleting local file ", aShard
    os.remove ( aShard )

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------

def main ( sqlFilename, billingProjID, tmpDestTable, delTableFlag, 
           maxCost, gcsPath, localDir, outFile, verboseFlag ):

    ## let's read in the SQL query first
    ( queryString, BQtableList ) = getBQueryFromFile ( sqlFilename, verboseFlag )

    ## first we need to get credentials and build the service
    if ( verboseFlag ):
        print " "
        print " getting credentials to access BigQuery ... "
    credentials = GoogleCredentials.get_application_default()
    BQsvc = discovery.build('bigquery', 'v2', credentials=credentials)

    ( destProjID, destDatasetID, destTableID ) = parseTableName ( tmpDestTable, billingProjID )

    if ( verboseFlag ):
        print " "
        print " "

    ## submit the SQL query as a dry-run to validate and verify cost
    estCost = submitDryRunQueryJob ( BQsvc, queryString, billingProjID, \
                      destProjID, destDatasetID, destTableID )
    if ( estCost > maxCost ):
        print " "
        print " The estimated cost exceeds the maximum specified cost. "
        print " If you want this to run, rerun with a higher maxCost parameter. "
        print " "
        sys.exit(-1)

    ## now submit the SQL query for real
    jobInfo = submitRealQueryJob ( BQsvc, queryString, billingProjID, \
                    destProjID, destDatasetID, destTableID )

    ## and poll for job completion
    pollJob ( BQsvc, jobInfo )

    ## the 'destination' table from the previous query is now the 'source' table
    srcProjID = destProjID
    srcDatasetID = destDatasetID
    srcTableID   = destTableID

    ## once we've saved the results of the query, the next step is to
    ## export the contents of the table to GCS -- depending on the
    ## size of the table, there may be multiple output files ("shards")
    jobInfo = exportBQtable2GCS ( BQsvc, gcsPath, billingProjID, 
                        srcProjID, srcDatasetID, srcTableID )

    ## and poll this job too ...
    pollJob ( BQsvc, jobInfo )

    ## at this point we can delete that table if this has been requested
    if ( delTableFlag ):
        deleteBQtable ( BQsvc, srcProjID, srcDatasetID, srcTableID )

    ## now starts the the second part of this ... we need to download the files/shards
    ## that have been written to GCS by the 'export' request, and then parse them
    ## and 'un-melt' the input 'tidy-formatted' data ...

    shardFiles = readShards_write2dTSV ( gcsPath, localDir, outFile, verboseFlag )

    print " "
    print " DONE! "
    print " "

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------

if __name__ == '__main__':

    parser = argparse.ArgumentParser ( description=
    " Extract data from BigQuery and write it to a local flat-file as a 2D data matrix. \n\n This is a multi-step process and requires several parameters: \n      a) submit your SQL query to BigQuery to extract 3 columns of information \n         (eg sample-id, gene-id, gene-expression); \n      b) save the results of the SQL query to a temporary table; \n      c) export this temporary table to a file in Google Cloud Storage \n         (if the table is large, multiple file shards will be written); \n      d) copy the file(s) from GCS to your local directory; \n      e) read the file(s), and transform them into a single 2D data matrix; \n      f) write this 2D data matrix to a file in your local directory  \n\n\n Note: ignore the line below that says 'optional arguments'. \n There are five REQUIRED arguments: --sqlFile, --gcpProj, --destTable, --gcsPath, and --outFile \n\n Here is a sample SQL query, note that that it has 3 fields in the first SELECT statement: \n\n       SELECT AliquotBarcode, HGNC_gene_symbol, MAX(normalized_count) AS expCount \n       FROM [isb-cgc:tcga_201607_beta.mRNA_UNC_HiSeq_RSEM] \n       WHERE  \n           ( SampleBarcode IN ( SELECT SampleBarcode FROM [isb-cgc:tcga_cohorts.BRCA] ) ) \n           AND ( HGNC_gene_symbol IS NOT NULL ) \n       GROUP BY AliquotBarcode, HGNC_gene_symbol \n\n ", formatter_class=argparse.RawTextHelpFormatter )

    parser.add_argument ( "-q",  "--sqlFile", required=True, 
        help="name of file containing your SQL query" )
    parser.add_argument ( "-p",  "--gcpProj", required=True, 
        help="your GCP project id (required for BigQuery billing)" )
    parser.add_argument ( "-t",  "--destTable", required=True, 
        help="temporary destination BigQuery table (in your project), \nof the form <project-name>:<dataset-name>.<table-name> or <dataset-name>.<table-name>" )
    parser.add_argument ( "-s",  "--gcsPath", required=True, 
        help="cloud storage path including wildcard for output file(s), \nof the form gs://<bucket>/<path>/<to>/tmp*.csv.gz (where the subfolder structure is optional)" )
    parser.add_argument ( "-o",  "--outFile", required=True, 
        help="output file name" )
    parser.add_argument ( "-l",  "--localDir", required=False, 
        help="local path for output and temp files (default=./)", default="./" )
    parser.add_argument ( "-c",  "--maxCost", action='store', default='1', 
        help="maximum allowed query cost in dollars (default=$1)" )
    parser.add_argument ( "-dt", "--delTable", action='store_true', 
        help="delete temporary BigQuery table when finished" )
    parser.add_argument ( "-v",  "--verbose", action='store_true', 
        help="verbose mode" )

    args = parser.parse_args()

    if ( args.verbose ):
        print " "
        print " RUNNING %s " % sys.argv[0]
        print " input arguments : ", args
        print " "

    ## sanity check the arguments ...
    ## first, verify that we can open the SQL file
    try:
        fh = file ( args.sqlFile, 'r' )
        fh.close()
    except:
        print " ERROR ??? failed to open the SQL file <%s> ??? " % args.sqlFile 
        sys.exit(-1)

    ## next, verify that the destination BigQuery table has the correct format
    ( destProjID, destDatasetID, destTableID ) = parseTableName ( args.destTable, args.gcpProj )

    ## check that the maxCost value seems reasonable
    try:
        args.maxCost = float ( args.maxCost )
        if ( args.maxCost < 0.1 ):
            print " WARNING: resetting maxCost parameter from $%.2f to $0.10 " % args.maxCost
            args.maxCost = 0.1
        elif ( args.maxCost > 10. ):
            print " WARNING: that is a very high maxCost value !!! $%.2f " % args.maxCost
    except:
        print " ERROR ??? maxCost should be a number, instead got %s " % args.maxCost

    ## make sure that the gcsPath format looks correct
    if ( not args.gcsPath.startswith("gs://") ): 
        print " ERROR the cloud storage path must start with gs:// "
        print " eg: gs://<bucket-name>/tmp*.csv.gz "
        sys.exit(-1)
    if ( args.gcsPath.find('*') < 0 ):
        print " ERROR the cloud storage path should include a wild-card "
        print " eg: gs://<bucket-name>/tmp*.csv.gz "
        sys.exit(-1)

    ## and finally, test that the output file can be opened for writing
    try:
        if ( args.localDir[-1] != "/" ): args.localDir += "/"
        tFile = args.localDir + args.outFile
        ## print " trying to open this file: <%s> " % tFile
        fh = file ( tFile, 'w' )
        fh.close()
    except:
        print " ERROR ??? failed to open the output file %s in %s " % ( args.outFile, args.localDir )
        sys.exit(-1)
    
    t0 = time.time() 

    main ( args.sqlFile, args.gcpProj, args.destTable, 
           args.delTable, args.maxCost,
           args.gcsPath, args.localDir, args.outFile,
           args.verbose )

    t1 = time.time()

    print " "
    print " =============================================================== "
    print " "
    print " time taken in seconds : ", (t1-t0)
    print " "
    print " =============================================================== "

#------------------------------------------------------------------------------
