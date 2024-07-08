cd ~/GCP/dataflow/traffic

export PROJECT_ID=$(gcloud config get-value project)
export REGION="us-west1"
export BUCKET=gs://${PROJECT_ID}
export PIPELINE_FOLDER=${BUCKET}
export RUNNER=DataflowRunner
export INPUT_PATH=${PIPELINE_FOLDER}/events.json
export TABLE_NAME=${PROJECT_ID}:logs.minute_traffic

python3 src/minute-traffic-pipeline.py --project=${PROJECT_ID} --region=${REGION} --staging_location=${PIPELINE_FOLDER}/staging --temp_location=${PIPELINE_FOLDER}/temp --runner=${RUNNER} --input_path=${INPUT_PATH} --table_name=${TABLE_NAME}
