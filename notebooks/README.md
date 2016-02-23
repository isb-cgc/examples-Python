# examples-Python/notebooks
The **notebooks** subdirectory of this repository contains a series of IPython notebooks that are intended to help you get started working with the ISB-CGC hosted [TCGA](http://cancergenome.nih.gov/) [data](https://tcga-data.nci.nih.gov/tcga/tcgaHome2.jsp) in BigQuery.

### Where to start?
You can find an overview of the BigQuery tables in this [notebook](https://github.com/isb-cgc/examples-Python/blob/master/notebooks/The%20ISB-CGC%20open-access%20TCGA%20tables%20in%20BigQuery.ipynb) and from there, we suggest that you look at the two "Creating TCGA cohorts" notebooks ([part 1](https://github.com/isb-cgc/examples-Python/blob/master/notebooks/Creating%20TCGA%20cohorts%20--%20part%201.ipynb) and [part 2](https://github.com/isb-cgc/examples-Python/blob/master/notebooks/Creating%20TCGA%20cohorts%20--%20part%202.ipynb)) which describe and make use of the Clinical and Biospecimen tables.  From there you can delve into the various molecular data tables as well as the Annotations table.  For now these sample notebooks are intentionally relatively simple and do not do any analysis that integrates data from multiple tables but once you have a grasp of how to use the data, developing your own more complex analyses should not be difficult.  You could even contribute an example back to our github repository!  You are also welcome to submit bug reports, comments, and feature-requests as [github issues](https://github.com/isb-cgc/examples-Python/issues).

### How to run the notebooks

1. Launch your own Cloud Datalab instance [in the cloud](https://cloud.google.com/datalab/getting-started) or [run it locally](https://github.com/GoogleCloudPlatform/datalab#using-datalab-and-getting-started).
2. Work through the introductory notebooks that are pre-installed on Cloud Datalab.
3. Run `git clone https://github.com/isb-cgc/examples-Python.git` on your local file system to download the notebooks.
4. Import the ISB-CGC notebooks into your Cloud Datalab instance by navigating to the notebook list page and uploading them.

### Important Note
If you are running in the cloud, a typical Google Cloud Datalab VM will cost your project approximately **$1/day**, so be sure to **shut down** Cloud Datalab when you are no longer using it. Shut down instructions and other tips are [here](https://cloud.google.com/datalab/getting-started) -- look for the section called **Stopping/Starting VM Instances**.
