# create gcs bucket 
# gsutil mb -l $REGION $BUCKET_NAME

# download data from source & upload to gcs 
curl https://raw.githubusercontent.com/sedeh/Datasets/main/loan_200k.csv | gsutil cp - gs://demos-vertex-ai-bq-staging/loan_200k.csv

# references 
# https: // www.linkedin.com/pulse/one-hot-vs-target-encoding-samuel-edeh/
# https: // github.com/sedeh/Datasets/blob/main/loan_200k.csv
# https://raw.githubusercontent.com/sedeh/Datasets/main/loan_200k.csv
# https://github.com/sedeh/onehot_encoding_vs_target_encoding/blob/main/encoding.py
