bq --location=US mk \
    --dataset \
    demos-vertex-ai:biglake_demo

gsutil cp gs://cloud-training/CPB200/BQ/lab4/airports.csv gs://demos-vertex-ai/airports.csv

bq mk --connection --location=US --project_id=demos-vertex-ai --connection_type=CLOUD_RESOURCE connection-01

bq show --connection demos-vertex-ai.US.connection-01

gcloud projects add-iam-policy-binding demos-vertex-ai \
    --member=serviceAccount:connection-746038361521-eeik@gcp-sa-bigquery-condel.iam.gserviceaccount.com --role=roles/storage.objectViewer 

bq mk \
    --external_table_definition=csv=gs://demos-vertex-ai/airports.csv@projects/demos-vertex-ai/locations/US/connections/connection-01 \
    demos-vertex-ai:biglake_demo.airports 

