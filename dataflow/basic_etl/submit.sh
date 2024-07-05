  python pipeline.py --project <YOUR_PROJECT_NAME> \
  --region us-central1 \
  --staging_location gs://<YOUR_BUCKET_NAME>/staging \
  --temp_location gs://<YOUR_BUCKET_NAME>/temp \
  --job_name load_json_to_bq
  --runner DataflowRunner
