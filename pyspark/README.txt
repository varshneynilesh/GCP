-- Cluster Provision
gcloud services enable dataproc.googleapis.com
gsutil mb -l us-central1 gs://$DEVSHELL_PROJECT_ID-dataproc
cloud dataproc clusters create bluesea --region=us-central1 --zone=us-central1-f --single-node --master-machine-type=n1-standard-2

-- run flight_airlines_dataload
gcloud dataproc jobs submit pyspark flight_airlines_dataload.py --cluster=bluesea --region=us-central1
