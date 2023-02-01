# BigLake Demo

[Create and manage BigLake tables  |  BigQuery  |  Google Cloud](https://cloud.google.com/bigquery/docs/biglake-quickstart)

[direct link](https://console.cloud.google.com/bigquery?project=demos-vertex-ai&ws=!1m5!1m4!4m3!1sdemos-vertex-ai!2sbiglake_demo!3sairports&cloudshell=true&d=biglake_demo&p=demos-vertex-ai&t=airports&page=table)


0. Create BQ dataset 

```sh  
bq --location=US mk \
--dataset \
demos-vertex-ai:biglake_demo
```

1. upload sample data to GCS 

```sh
gsutil cp gs://cloud-training/CPB200/BQ/lab4/airports.csv gs://demos-vertex-ai/airports-test.csv
# gsutil cp ./data/airports.csv gs://demos-vertex-ai/airports.csv
```

2. create BigLake connection 

```sh
bq mk --connection --location=US --project_id=demos-vertex-ai --connection_type=CLOUD_RESOURCE connection-01
```

get service account 
```sh  
bq show --connection demos-vertex-ai.US.connection-01
```

add service account to GCS as viewer: 

```sh  
gcloud projects add-iam-policy-binding demos-vertex-ai \
    --member=serviceAccount:connection-746038361521-eeik@gcp-sa-bigquery-condel.iam.gserviceaccount.com --role=roles/storage.objectViewer 
```

Create biglake table

```sh  
bq mk \
    --external_table_definition=csv=gs://demos-vertex-ai/airports.csv@projects/demos-vertex-ai/locations/US/connections/connection-01 \
    demos-vertex-ai:biglake_demo.airports 
```

View result: [direct link](https://console.cloud.google.com/bigquery?project=demos-vertex-ai&ws=!1m5!1m4!4m3!1sdemos-vertex-ai!2sbiglake_demo!3sairports&cloudshell=true&d=biglake_demo&p=demos-vertex-ai&t=airports&page=table)