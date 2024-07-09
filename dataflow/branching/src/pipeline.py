import json
import argparse
import time
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions, StandardOptions
from apache_beam.runners import DataflowRunner, DirectRunner


def parse_json(element):
    return json.loads(element)

def drop_fields(element):
    element.pop('user_agent')
    return element


def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from Json into BigQuery')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')
    parser.add_argument('--staging_location', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--temp_location', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--input_path', required=True, help='Path to events.json')
    parser.add_argument('--output_path', required=True, help='Path to coldline storage bucket')
    parser.add_argument('--table_name', required=True, help='BigQuery table name')

    opts, pipeline_opts = parser.parse_known_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions(pipeline_opts)
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.staging_location
    options.view_as(GoogleCloudOptions).temp_location = opts.temp_location
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('branching-pipeline-',time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner

    input_path = opts.input_path
    output_path = opts.output_path
    table_name = opts.table_name

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
                "type": "FLOAT",
                "mode": "NULLABLE"
            },
            {
                "name": "lng",
                "type": "FLOAT",
                "mode": "NULLABLE"
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
            }
        ]
    }

    p = beam.Pipeline(options=options)
    lines = p|'ReadFromGCS' >> beam.io.ReadFromText(input_path)

    # write to GCS
    lines|'WriteToGCS' >> beam.io.WriteToText(output_path)

    # write to bigquery
    (lines
     |'ParseJson' >> beam.Map(parse_json)
     |'DropFields' >> beam.Map(drop_fields)
     |'FilterFn' >> beam.Filter(lambda row : row['num_bytes'] < 120)
     |'WriteToBQ' >> beam.io.WriteToBigQuery(
         table_name,
         schema=table_schema,
         create_disposition= beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
         write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
     )
    )
    
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run()


if __name__ == "__main__":
    run()
