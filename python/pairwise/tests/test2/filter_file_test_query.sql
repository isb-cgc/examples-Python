WITH
T1 AS (
SELECT
project_short_name,
HGNC_gene_symbol,
normalized_count,
sample_barcode
FROM `isb-cgc.TCGA_hg19_data_v0.RNAseq_Gene_Expression_UNC_RSEM`
),
J1 AS (
SELECT
HGNC_gene_symbol AS HGNC_gene_symbol_J1,
normalized_count AS normalized_count_J1,
sample_barcode AS sample_barcode_J1 FROM T1
WHERE
project_short_name='TCGA-BRCA' AND HGNC_gene_symbol > 'GZ' AND HGNC_gene_symbol < 'HZ'
)
,
T2 AS (
SELECT
project_short_name,
HGNC_gene_symbol,
normalized_count,
sample_barcode
FROM `isb-cgc.TCGA_hg19_data_v0.RNAseq_Gene_Expression_UNC_RSEM`
),
J2 AS (
SELECT
HGNC_gene_symbol AS HGNC_gene_symbol_J2,
normalized_count AS normalized_count_J2,
sample_barcode AS sample_barcode_J2 FROM T2
WHERE
project_short_name='TCGA-BRCA' AND HGNC_gene_symbol > 'AZ' AND HGNC_gene_symbol < 'BZ'
)
,
mainjoin AS (
SELECT normalized_count_J1,
normalized_count_J2,
HGNC_gene_symbol_J1,
HGNC_gene_symbol_J2
FROM
J1 JOIN J2 ON
J1.sample_barcode_J1 = J2.sample_barcode_J2 AND
J1.HGNC_gene_symbol_J1 > J2.HGNC_gene_symbol_J2
),
ranktable AS (
SELECT
 HGNC_gene_symbol_J1,
HGNC_gene_symbol_J2,
  DENSE_RANK() OVER (PARTITION BY HGNC_gene_symbol_J1 ORDER BY normalized_count_J1 ASC) as rankvar1,
  DENSE_RANK() OVER (PARTITION BY HGNC_gene_symbol_J2 ORDER BY normalized_count_J2 ASC) as rankvar2
FROM
mainjoin
)
SELECT
HGNC_gene_symbol_J1,
HGNC_gene_symbol_J2,
  CORR( rankvar1, rankvar2 ) as Spearmans
FROM
   ranktable
GROUP BY
HGNC_gene_symbol_J1,
HGNC_gene_symbol_J2
ORDER BY
 Spearmans DESC

