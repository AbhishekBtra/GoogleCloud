import apache_beam as beam
import apache_beam.io.gcp.pubsub as pubsub
import apache_beam.io.gcp.bigquery as bq
import sys
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window
from apache_beam.utils import windowed_value

SUBSCRIPTION = "projects/oceanic-oxide-285410/subscriptions/checkout-sub"


def create_tuples(pubsub_message):
    _data = pubsub_message.data.decode('utf-8').split(',')
    _data.append(pubsub_message.attributes['ts'])
    key = _data[0]
    value = _data[1:]
    return key, value


def windowed_data(data):
    k, v = data
    print(v.timestamp)
    data_dict = {k: v}
    print(data_dict)


def run():
    with beam.Pipeline(options=pipeline_options) as p:
        p | \
        pubsub.ReadFromPubSub(subscription=SUBSCRIPTION,with_attributes=True) \
        | beam.WindowInto(window.FixedWindows(5)) \
        | beam.Map(create_tuples) \
        | beam.GroupByKey() \
        #| bq.WriteToBigQuery(table=,dataset=,project=)


if __name__ == "__main__":
    sys.argv.append('--streaming')
    pipeline_options = PipelineOptions(flags=sys.argv[1:])
    run()
