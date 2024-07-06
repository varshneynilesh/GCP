from pyspark.sql import SparkSession
from google.cloud import bigquery
from google.cloud.bigquery import table
from google.cloud import storage


BUCKET = "<GCS_BUCKET>"
PROJECT = "<PROJECT>"

def create_bq_normal_table(bq_project, bq_dataset, bq_table, source_gcs_path):
  client = bigquery.Client()
  job_config = bigquery.LoadJobConfig()
  job_config.create_disposition = 'CREATE_IF_NEEDED'
  job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
  job_config.source_format = bigquery.SourceFormat.PARQUET
  job_config.use_avro_logical_types=True
  table_ref = client.dataset(bq_dataset, project= bq_project)
  uri = source_gcs_path + "*.parquet"
  load_job = client.load_table_from_uri(
    uri, table_ref, job_config = job_config
  )
  print("Job starting {}".format(load_job.job_id))
  destination_table = client.get_table(table_ref)
  print("Loaded {} rows".format(destination_table.num_rows))
  print("Loading Job Finished")


airline_data = "gs://{}/data/airlines.csv".format(BUCKET)
print(airline_data)
flight_data = "gs://{}/data/flights.csv".format(BUCKET)

spark = SparkSession.builder.appName('airline_data_loads').getOrCreate()
airlines = spark.read.csv(airline_data, header=True, inferSchema=True)

airlines.printSchema()
airlines.show(5, truncate=False)

flights = spark.read.csv(flight_data, header=True, inferSchema=True)
flights.printSchema()
flights.show(5, truncate=False)


# Save flight data to GCS
v_target_bucket = "gs://{}/{}/".format(BUCKET_ID,"flights")
flights.write.mode("overwrite").format("parquet").save(v_target_bucket)
create_bq_normal_table(PROJECT,"airlines_db","flights", v_target_bucket) 

v_target_bucket = "gs://{}/{}/".format(BUCKET_ID,"airlines")
airlines.write.mode("overwrite").format("parquet").save(v_target_bucket)
create_bq_normal_table(PROJECT,"airlines_db","airlines", v_target_bucket) 
print(v_target_bucket)


spark.stop()
