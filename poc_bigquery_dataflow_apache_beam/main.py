import sys
import os
import apache_beam as beam
import apache_beam.io.avroio as avroio
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.pipeline import PipelineOptions, SetupOptions
from pipelines.avro_to_bq import pipe_avro_all_sessions_raw, pipe_avro_products


if __name__ == "__main__":
    #pipe_avro_all_sessions_raw.run()
    pipe_avro_products.run()