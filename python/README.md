# examples-Python/python
The **python** subdirectory of this repository contains examples to help you get started working with the data and tools hosted by the ISB-CGC on the Google Cloud Platform.  There are three endpoints based on the three programs in the Genomics Data Commons:
  * **isb_cgc_ccle_api**
  * **isb_cgc_target_api**
  * **isb_cgc_tcga_api**
and one endpoint for the cohort apis that can be cross-program:
  * **isb_cgc_api**

### Programmatic API Examples
The ISB-CGC programmatic API is implemented using Google Cloud Endpoints.  These services run on App Engine and can be accessed in several different ways.  You can use the [APIs Explorer](https://apis-explorer.appspot.com/apis-explorer/?base=https://api-dot-isb-cgc.appspot.com/_ah/api#p/) to try them out directly in your web browser.  
  *  **isb_auth.py** is a help script that takes care of the auth required for the cohort endpoint APIs
  *  **isb_cgc_api_v3_cases** shows you how to build a service object and run the isb_cgc_tcga_api cases get API.  This cases get API is also part of the isb_cgc_ccle_api and isb_cgc_target_api endpoints.
  *  **isb_cgc_api_v3_cohorts** shows you how to build a service object and run the isb_cgc_tcga_api cohorts create and preview APIs.  These cohorts APIs are also part of the isb_cgc_ccle_api and isb_cgc_target_api endpoints.  It also shows how to build a service object and run the isb_cgc_api cohorts get, list, cloud_storage_file_paths, and delete APIs
  *  **isb_cgc_api_v3_samples** shows you how to build a service object and run the isb_cgc_tcga_api samples get and cloud_storage_file_paths APIs.  These samples APIs are also part of the isb_cgc_ccle_api and isb_cgc_target_api endpoints.
  *  **isb_cgc_api_v3_users** shows you how to build a service object and run the isb_cgc_tcga_api users get API.  This users get API is also part of the isb_cgc_ccle_api and isb_cgc_target_api endpoints.
These are some helper scripts for other aspects of the Google Cloud
  *  **query_ccle_reads.py** script illustrates the usage of the GA4GH API for open-access CCLE reads
  *  **createSchema.py** script generates a JSON schema for an input data file. This is useful when the data file has a large number of columns, so you can avoid manual creation of its schema. This can be used with the 'bq load' command line tool to load data to BigQuery (https://cloud.google.com/bigquery/quickstart-command-line).
