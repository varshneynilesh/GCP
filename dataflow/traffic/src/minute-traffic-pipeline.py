import json
import time
import logging
import argparse
import typing
from datetime import datetime

import apache_beam as beam
from apache_beam import window
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import DataflowRunner, DirectRunner

class CommonLog(typing.NamedTuple):
    ip: str
    user_id: str
    lat: float
    lng: float
    timestamp: str
    http_request: str
    http_response: int
    num_bytes: int
    user_agent: str

beam.coders.registry.register_coder(CommonLog, beam.coders.RowCoder)

def parse_json(element):
    row = json.loads(element)
    return CommonLog(**row)

def add_timestamp(element):
    ts = datetime.strptime(element.timestamp[:-8], "%Y-%m-%dT%H:%M:%S").timestamp()
    return beam.window.TimestampedValue(element, ts)

class GetTimestampFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        window_start = window.start.to_utc_datetime().strftime("%Y-%m-%dT%H:%M:%S")
        output = {'page_views': element, 'timestamp': window_start}
        yield output

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from Json into BigQuery')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--staging_location', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--temp_location', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')
    parser.add_argument('--input_path', required=True, help='Path to events.json')
    parser.add_argument('--table_name', required=True, help='BigQuery table name')

    opts = parser.parse_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions(save_main_session=True)
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.staging_location
    options.view_as(GoogleCloudOptions).temp_location = opts.temp_location
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('minute-traffic-pipeline-',time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner

    input_path = opts.input_path
    table_name = opts.table_name

    table_schema = {
        "fields": [
            {
                "name": "page_views",
                "type": "INTEGER"
            },
            {
                "name": "timestamp",
                "type": "STRING"
            },

        ]
    }

    p = beam.Pipeline(options=options)
    (p
     |'ReadFromGCS' >> beam.io.ReadFromText(input_path)
     |'ParseJson' >> beam.Map(parse_json).with_output_types(CommonLog)
     |'AddEventTimestamp' >> beam.Map(add_timestamp)
     |'WindowByMinute' >> beam.WindowInto(window.FixedWindows(60))
     |'CountPerMinute' >> beam.CombineGlobally(CountCombineFn()).without_defaults()
     |'AddWindowTimestamp' >> beam.ParDo(GetTimestampFn())
     | 'WriteToBQ' >> beam.io.WriteToBigQuery(
            table_name,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
            )

    )


    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building Pipeline.......")
    p.run()


if __name__ == "__main__":
    run()
