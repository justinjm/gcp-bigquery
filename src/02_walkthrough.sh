project_id=demos-vertex-ai
dataset_id=tempdataset1
table_id=testtable1
table_id_ingestion_partitioned=testtable1_ingestion_partitioned
table_id_partitioned=testtable1_partitioned

echo "[ INFO ] List all BigQuery Datasets"
bq ls

echo "[ INFO ] Create a new BigQuery Dataset named tempdataset1"
bq mk --description "Dataset used for Testing" $dataset_id
bq ls

echo "[ INFO ] Create an empty table with a defined schema and expiring in 3600 seconds."
bq mk --table --expiration 3600 --description "Test Table" --label org:analytics $project_id:$dataset_id.$table_id id:STRING,dept:STRING,sales:FLOAT

echo "[ INFO ] List all BigQuery tables within $dataset_id"
bq ls $dataset_id

echo "[ INFO ] Create a Ingestion-Time Partitioned Table called table_id_ingestion_partitioned"
echo "[ INFO ] This table is partitioned based on ingestion (load) time by day"
bq query --location=US --destination_table $dataset_id.table_id_ingestion_partitioned --time_partitioning_type=DAY --use_legacy_sql=false 'SELECT name,number FROM `bigquery-public-data.usa_names.usa_1910_current` WHERE gender = "M" ORDER BY number DESC'


echo "[ INFO ] Create a Partitioned Table called table_id_partitioned"
echo "[ INFO ] This table is partitioned based on the  on ingestion (load) time by day"
bq query --location=US --destination_table $dataset_id.table_id_partitioned --nouse_legacy_sql 'SELECT TIMESTAMP("2018-02-01") as TS, 2 as a'
 

echo "[ INFO ] List all BigQuery tables within $dataset_id"
bq ls $dataset_id


echo "[ INFO ] Deleting BigQuery dataset names tempdataset1"
bq rm -rf tempdataset1


echo "[ INFO ] Displaying Schema"
bq show "$project_id:$dataset_id.$table_id"


echo "[ INFO ] Run BigQuery query"
bq query "SELECT count(*) as count FROM $project_id:$dataset_id.$table_id"
