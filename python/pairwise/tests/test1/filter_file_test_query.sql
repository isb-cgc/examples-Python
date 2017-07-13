WITH
T1 AS (
SELECT
project_short_name,
normalized_count,
case_barcode,
HGNC_gene_symbol
FROM `isb-cgc.TCGA_hg19_data_v0.RNAseq_Gene_Expression_UNC_RSEM`
),
J1 AS (
SELECT
normalized_count AS normalized_count_J1,
case_barcode AS case_barcode_J1,
HGNC_gene_symbol AS HGNC_gene_symbol_J1 FROM T1
WHERE
project_short_name='TCGA-BRCA' AND HGNC_gene_symbol='LARP1'
)
,
T2 AS (
SELECT
project_short_name,
beta_value,
case_barcode,
probe_id
FROM `isb-cgc.TCGA_hg19_data_v0.DNA_Methylation_chr16`
),
A2 AS (
SELECT
IlmnID,
RefGene_Name
FROM `isb-cgc.platform_reference.methylation_annotation`
, UNNEST(UCSC)
),
J2 AS (
SELECT
beta_value AS beta_value_J2,
case_barcode AS case_barcode_J2,
probe_id AS probe_id_J2,
IlmnID AS IlmnID_J2,
RefGene_Name AS RefGene_Name_J2
 FROM T2 JOIN A2 ON T2.probe_id= A2.IlmnID
WHERE
project_short_name='TCGA-BRCA' AND RefGene_Name = 'GSG1L'
)
,
mainjoin AS (
SELECT normalized_count_J1,
beta_value_J2,
HGNC_gene_symbol_J1,
RefGene_Name_J2
FROM
J1 JOIN J2 ON
J1.case_barcode_J1 = J2.case_barcode_J2 AND
J1.HGNC_gene_symbol_J1 > J2.RefGene_Name_J2
),
ranktable AS (
SELECT
 HGNC_gene_symbol_J1,
RefGene_Name_J2,
  DENSE_RANK() OVER (PARTITION BY HGNC_gene_symbol_J1 ORDER BY normalized_count_J1 ASC) as rankvar1,
  DENSE_RANK() OVER (PARTITION BY RefGene_Name_J2 ORDER BY beta_value_J2 ASC) as rankvar2
FROM
mainjoin
)
SELECT
HGNC_gene_symbol_J1,
RefGene_Name_J2,
  CORR( rankvar1, rankvar2 ) as Spearmans
FROM
   ranktable
GROUP BY
HGNC_gene_symbol_J1,
RefGene_Name_J2
ORDER BY
 Spearmans DESC
