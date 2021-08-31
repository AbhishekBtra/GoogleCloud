import sys
import apache_beam.io.gcp.bigquery as bq
import apache_beam as beam
import apache_beam.io.avroio as avroio
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


def run():

    try:
        import pipelines.Transformations.transforms as t
        
    except ModuleNotFoundError:
        
        sys.path.append('../..')
        print('*'*50)
        print(sys.path)
        print('*'*50)
        import pipelines.Transformations.transforms as t

    transform = t.Transforms

    pipeline_options = PipelineOptions(flags=sys.argv[1:])
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline() as p:
        read_from_bigquery_write_to_avro_products = (
                p
                | 'ReadingTransDataFromBigQuery' >> bq.ReadFromBigQuery(
            query=transform.query('rev_transactions'),
            use_standard_sql=True,
            gcs_location=transform.gcs_bq_temp_loc(),
            project=transform.gcloud_project()
        )
                | 'WriteToAVRO' >> avroio.WriteToAvro(
            file_path_prefix=transform.avro_file_path_prefix_for_table('rev_transactions'),
            schema=transform.get_avro_schema('rev_transactions'),
            codec='deflate',
            use_fastavro=True,
            file_name_suffix='.avro'
        )
        )