import sys
import os
import apache_beam as beam
import apache_beam.io.avroio as avroio
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.pipeline import PipelineOptions, SetupOptions



def run():

    try:
        from pipelines.Transformations.transforms import Transforms
        from pipelines.TableSchemas.products import Products
        
    except ModuleNotFoundError:
        
        sys.path.append('../..')
        print('*'*50)
        print(sys.path)
        print('*'*50)
        from pipelines.Transformations.transforms import Transforms
        from pipelines.TableSchemas.products import Products
        

    schema = Products.bq_table_schema
        
    print(sys.argv[1:])
    file_path = Transforms.avro_file_path_prefix_for_table('products') + '*'
    table = '.'.join([Transforms.get_bq_dataset(), 'products'])

    pipeline_options = PipelineOptions(flags=sys.argv[1:])
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        read_from_avro = (
            p
            | avroio.ReadFromAvro(file_pattern=file_path,
                                  use_fastavro=True)
            | WriteToBigQuery(table=table, project=Transforms.gcloud_project(),
                              schema=schema, create_disposition= BigQueryDisposition.CREATE_IF_NEEDED,
                              write_disposition= BigQueryDisposition.WRITE_TRUNCATE,
                              custom_gcs_temp_location= Transforms.gcs_bq_temp_loc(),
                              temp_file_format='AVRO'
                              )
        )