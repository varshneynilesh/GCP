from google.cloud import bigquery
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql import functions as f

PROJECT = "<PROJECT_ID>"
GCS_DATA_BUCKET = "<BUCKET_ID>"

player_data = "gs://{}/data/player.csv".format(GCS_DATA_BUCKET)
player_attr_data = "gs://{}/data/player_attributes*.csv".format(GCS_DATA_BUCKET)

def read_csv(spark_session: SparkSession, source_location:str) -> None:
    df = spark_session.read.csv(path=source_location, header=True)
    print(df.printSchema())
    print(df.show(5, truncate=False))
    return df

def create_bq_normal_table(bq_project, bq_dataset, bq_table, source_gcs_path):
  client = bigquery.Client()
  job_config = bigquery.LoadJobConfig()
  job_config.create_disposition = 'CREATE_IF_NEEDED'
  job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
  job_config.source_format = bigquery.SourceFormat.PARQUET
  job_config.use_avro_logical_types=True
  table_ref = client.dataset(bq_dataset, project= bq_project).table(bq_table)
  uri = source_gcs_path + "*.parquet"
  load_job = client.load_table_from_uri(
    uri, table_ref, job_config = job_config
  )
  print("Job starting {}".format(load_job.job_id))
  destination_table = client.get_table(table_ref)
  print("Loaded {} rows".format(destination_table.num_rows))
  print("Loading Job Finished")


# main section
spark = SparkSession.builder.appName('soccer_data').getOrCreate()
players = read_csv(spark, player_data)
players_attr = read_csv(spark, player_attr_data)

# Load player attributes to biq query table
v_target_bucket = "gs://{}/{}/".format(GCS_DATA_BUCKET,"players_attr")
players_attr.write.mode("overwrite").format("parquet").save(v_target_bucket)
create_bq_normal_table(PROJECT,"players","players_attributes", v_target_bucket)

# Load Players data in biq Query table
v_target_bucket = "gs://{}/{}/".format(GCS_DATA_BUCKET,"players")
players.write.mode("overwrite").format("parquet").save(v_target_bucket)
create_bq_normal_table(PROJECT,"players","players", v_target_bucket)

# udf function to retrive the year from date
extract_year_udf = udf(lambda date: date.split('-')[0])

players_attr = (
    players_attr.select(
                    'player_api_id',
                    'date',
                    'shot_power',
                    'finishing',
                    'acceleration'
    )
    .withColumn("year", extract_year_udf(players_attr.date))
    .filter(f.col("year") == 2016)
    .drop(players_attr.date)
)

players_attr = (
    players_attr.groupBy('player_api_id')
    .agg({"shot_power":"avg", "finishing":"avg","acceleration":"avg"})
)

players_attr = (
    players_attr
    .withColumnRenamed("avg(finishing)","finishing")
    .withColumnRenamed("avg(shot_power)","shot_power")
    .withColumnRenamed("avg(acceleration)","acceleration")
)

finishing_weight = 2
acceleration_weight = 1
shot_power_weight = 1

total_weight = finishing_weight + acceleration_weight + shot_power_weight

players_attr = (
    players_attr
    .withColumn("weighted_score", 
                players_attr.finishing * finishing_weight +
                players_attr.shot_power * shot_power_weight +
                players_attr.acceleration * acceleration_weight 
                )
    .drop("finishing", "shot_power","acceleration")
    )

top_20_stricker = players_attr.orderBy("weighted_score", ascending=False).limit(20)

selected_strickers = players.join(top_20_stricker, ['player_api_id'])

# Load data to GCS bucket
v_target_bucket = "gs://{}/{}/".format(GCS_DATA_BUCKET,"selected_strickers")
selected_strickers.write.mode("overwrite").format("parquet").save(v_target_bucket)
create_bq_normal_table(PROJECT,"players","selected_strickers", v_target_bucket)

print("----------finish -------------------------")










