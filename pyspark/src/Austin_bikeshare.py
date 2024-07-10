from pyspark.sql import SparkSession
from google.cloud import storage
from google.cloud import bigquery
from pyspark.sql import  functions as f
import datetime


PROJECT = "<PROJECT_ID>"
GCS_BUCKET = "<GCS_BUCKET>"


def create_query_cache(v_query):
    print(datetime.now())
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    job_config.use_query_cache = True
    query_job = client.query(query=v_query, job_config=job_config)
    query_job.result()
    query_cache = query_job.destination.project + '.' + query_job.destination.dataset_id + '.' + query_job.destination.table_id
    return_value = {"query_cache_id": query_cache, "processed_cnt" : query_job.total_bytes_processed}
    print(datetime.now())
    return return_value

spark = SparkSession.builder.appName("Window").getOrCreate()


# read data from bigquery table
query = """SELECT * FROM `{}.london.bikeshare_trips` LIMIT 100""".format(PROJECT)
query_cach = create_query_cache(query)
df_bikeshare_trips = spark.read.format("bigquery").option("table", query_cach['query_cache_id']).load()

print(df_bikeshare_trips.printSchema())
print(df_bikeshare_trips.show(5, False))
