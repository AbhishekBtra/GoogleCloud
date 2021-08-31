import random
import time
from google.cloud import pubsub
import google.cloud.storage.client as storgeclient
from datetime import datetime as dt

#file path gs://data_ab/ecommerce_data/Checkouts.csv
COUNTER = 0
TOPIC = "projects/oceanic-oxide-285410/topics/checkouts"
PUBLISHER = pubsub.PublisherClient()


def go_to_sleep(sec):
    global COUNTER
    COUNTER = 0
    print('*' * 50 + 'Sleeping for {0} seconds'.format(sec) + '*' * 50)
    time.sleep(sec)


def publish_message(publisher,message, pk):
    timestamp = dt.utcnow().isoformat("T")+"Z"
    #publisher = pubsub.PublisherClient()
    message_future = publisher.publish(data=message, topic=TOPIC, ts=timestamp, PK=pk)
    message_future.add_done_callback(callback)


def callback(message_future):
    if message_future.exception(timeout=10):
        print(f"Exception Raised By Future {message_future.exception()}")
    else:
        print("Printing message id from callback")
        print(message_future.result())


def main(event, context):
    try:
        if event['name'] == "ecommerce_data/Checkouts.csv":
            storage_client = storgeclient.Client()
            bucket = storage_client.bucket("data_ab")
            blob = bucket.blob(event['name'])
            file_content = blob.download_as_string()
            lines = file_content.decode("utf-8").split('\n')
            print(f"filename {event['name']} reading started")
            for line in lines[1:]:
                global COUNTER
                COUNTER += 1
                if COUNTER > random.randrange(500, 1000):
                    go_to_sleep(2)
                pk = line[0]
                message = line.encode(encoding="utf-8")
                publish_message(PUBLISHER,message, pk)

    except Exception as e:
        print(e.__cause__)
