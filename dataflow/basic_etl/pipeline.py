import time
import json
import logging
import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import DataflowRunner, DirectRunner

def run():

    parser = argparse.ArgumentParser(description='Convert Json into BigQuery using Schemas')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--stagingLocation', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--tempLocation', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')

    opts = parser.parse_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions()
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.stagingLocation
    options.view_as(GoogleCloudOptions).temp_location = opts.tempLocation
    options.view_as(GoogleCloudOptions).job_name = '{0}-{1}'.format(opts.job_name,time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner

    #static input and output strings
    input = 'gs://{0}/events.json'.format(opts.project)
    output = '{0}:logs.logs'.format(opts.project)
    
    # Table schema for BigQuery
    # Table schema for BigQuery
    table_schema = {
        "fields": [
            {
                "name": "ip",
                "type": "STRING"
            },
            {
                "name": "user_id",
                "type": "STRING"
            },
            {
                "name": "lat",
                "type": "FLOAT"
            },
            {
                "name": "lng",
                "type": "FLOAT"
            },
            {
                "name": "timestamp",
                "type": "STRING"
            },
            {
                "name": "http_request",
                "type": "STRING"
            },
            {
                "name": "http_response",
                "type": "INTEGER"
            },
            {
                "name": "num_bytes",
                "type": "INTEGER"
            },
            {
                "name": "user_agent",
                "type": "STRING"
            }
        ]
    }

    # Create the pipeline
    p = beam.Pipeline(options=options)
    (p
     |'ReadFromGCS' >> beam.io.ReadFromText(input)
     |'ParseJson' >> beam.Map(lambda line: json.loads(line))
     |'WriteToBQ' >> beam.io.Write(
                       output,
                       schema = table_schema,
                       create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                       write.disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE
              )
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run()

if __name__ == '__main__':
  run()
