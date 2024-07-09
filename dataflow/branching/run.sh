cd ~/GCP/dataflow/branching

export PROJECT_ID=$(gcloud config get-value project)
export REGION="us-west1"
export BUCKET=gs://${PROJECT_ID}
export PIPELINE_FOLDER=${BUCKET}
export RUNNER=DataflowRunner
export INPUT_PATH=${PIPELINE_FOLDER}/events.json
export OUTPUT_PATH=${PIPELINE_FOLDER}/output/events.json
export TABLE_NAME=${PROJECT_ID}:logs.events

python3 src/pipeline.py --project=${PROJECT_ID} --region=${REGION} --staging_location=${PIPELINE_FOLDER}/staging --temp_location=${PIPELINE_FOLDER}/temp --runner=${RUNNER} --input_path=${INPUT_PATH} --output_path=${OUTPUT_PATH} --table_name=${TABLE_NAME}
