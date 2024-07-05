  python pipeline.py --project <PROJECT_ID> \
  --region us-central1 \
  --staging_location gs://<GCS_BUCKET_ID>/staging \
  --temp_location gs://<GCS_BUCKET_ID>/temp \
  --runner DataflowRunner
