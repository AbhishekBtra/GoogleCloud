import apache_beam as beam
import sys
from apache_beam.transforms.ptransform import PTransform
try:
        from pipelines.TableSchemas import all_session_raw,products,rev_transactions
except ModuleNotFoundError as identifier:
        sys.path.append('../..')
        from pipelines.TableSchemas import all_session_raw,products,rev_transactions



class Transforms:
    __PROJECT = "gcp-big-data-292014"
    __DATASET = "ecommerce"
    __BQ_TEMP_LOC = "gs://big_data_buket/temporary/bq_temp/"
    __GCS_BUCKET = "gs://big_data_buket"

    @staticmethod
    def gcs_bq_temp_loc():
        return str(Transforms.__BQ_TEMP_LOC)

    @staticmethod
    def get_bq_dataset():
        return Transforms.__DATASET

    @staticmethod
    def gcloud_project():
        return str(Transforms.__PROJECT)

    @staticmethod
    def transform_product_category(data):
        if data['v2ProductCategory'] not in '(not set)':
            reversed_text_list = data['v2ProductCategory'].split('/')[::-1]
            for item in reversed_text_list:
                if item.strip() != '':
                    data['v2ProductCategory'] = item
                    break

        return data

    @staticmethod
    def set_ecommerce_action_type(data):

        dict_eccomerceaction = {
            "0": 'Unknown',
            "1": 'Click through of product lists',
            "2": 'Product detail views',
            "3": 'Add product(s) to cart',
            "4": 'Remove product(s) from cart',
            "5": 'Check out',
            "6": 'Completed purchase',
            "7": 'Refund of purchase',
            "8": 'Checkout options'
        }

        data['eCommerceAction_type'] = dict_eccomerceaction[data['eCommerceAction_type']]

        return data

    @staticmethod
    def add_unique_session_id(data):
        data['unique_session_id'] = "-".join([data['fullVisitorId'], str(data['visitId'])])
        return data

    @staticmethod
    def get_avro_schema(table):
        dict_of_table = {
            'all_sessions_raw': all_session_raw.AllSessionRaw.schema,
            'products': products.Products.schema,
            'rev_transactions': rev_transactions.Transactions.schema
        }
        return dict_of_table[table]

    @staticmethod
    def avro_file_path_prefix_for_table(table):
        dict_of_table = {
            'all_sessions_raw': Transforms.__GCS_BUCKET+"/ecommerce_data/all_sessions_raw",
            'products': Transforms.__GCS_BUCKET+"/ecommerce_data/products",
            'rev_transactions': Transforms.__GCS_BUCKET+"/ecommerce_data/rev_transactions"
        }
        return dict_of_table[table]

    @classmethod
    def query(cls, table=None):
        if table is not None:
            return Transforms.__bq_get_data_by_query(table)
        else:
            return None

    @staticmethod
    def __bq_get_data_by_query(table):
        return """
                SELECT *
                FROM `{project}.{dataset}.{table}` limit 5000000
            """.format(project=Transforms.__PROJECT, dataset=Transforms.__DATASET, table=table)


class LeftJoin(PTransform):

    def __init__(self, left_table_name, right_table_name, left_data, right_data, join_on_key):
        self.left_data = left_data
        self.right_data = right_data
        self.join_on_key = join_on_key
        self.left_table_name = left_table_name
        self.right_table_name = right_table_name

    def expand(self, pcoll):
        def _get_tuple_by_joinkey_and_values(data_dict, joinkey):
            try:
                var = data_dict[joinkey]
                return data_dict[joinkey], data_dict
            except KeyError:
                joinkey = 'productSKU'
                return data_dict[joinkey],data_dict
        return (
                {
                    table: data | 'Convert {0} to {1}'.format(table,data)
                           >> beam.Map(_get_tuple_by_joinkey_and_values,self.join_on_key)
                    for table, data in pcoll.items()
                }
                  | beam.CoGroupByKey()
                  | beam.ParDo(UnNestGroups(),self.left_table_name,self.right_table_name)
        )


class UnNestGroups(beam.DoFn):

    def process(self, element, left_table_name, right_table_name,ts = beam.DoFn.TimestampParam):
        join_key, data_dict = element
        left_data = data_dict[left_table_name]
        right_data = data_dict[right_table_name]

        print(ts)
        for data in left_data:
            try:
                data.update(right_data[0])
                yield data
            except IndexError:
                yield data




