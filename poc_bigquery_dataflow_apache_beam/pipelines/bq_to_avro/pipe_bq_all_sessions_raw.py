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

    with beam.Pipeline(options=pipeline_options) as p:
        read_from_bigquery_write_to_avro_session = (
                p
                | 'ReadingAllSessionDataFromBigQuery' >> bq.ReadFromBigQuery(
            query=transform.query('all_sessions_raw'),
            use_standard_sql=True,
            gcs_location=transform.gcs_bq_temp_loc(),
            project=transform.gcloud_project()
        )
                | 'FilterInOnlyPAGEType' >> beam.Filter(lambda x: x['type'] == 'PAGE')
                | 'FixProductCategory' >> beam.Map(transform.transform_product_category)
                | 'AddEcommerceActionType' >> beam.Map(transform.set_ecommerce_action_type)
                | 'AddUniqueSessionIdCol' >> beam.Map(transform.add_unique_session_id)
                | 'WriteToAVRO' >> avroio.WriteToAvro(
            file_path_prefix=transform.avro_file_path_prefix_for_table('all_sessions_raw'),
            schema=transform.get_avro_schema('all_sessions_raw'),
            codec='deflate',
            use_fastavro=True,
            file_name_suffix='.avro'
        )
        )