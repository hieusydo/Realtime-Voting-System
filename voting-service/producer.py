from kafka import KafkaProducer
from flask import Flask, render_template, request, session, url_for, redirect

import json

app = Flask(__name__)

#Define a route to hello function
@app.route('/')
def hello():
  return render_template('index.html')

@app.route('/castVote', methods = ['POST'])
def api_message():
    if request.headers['Content-Type'] == 'application/json':
        kafka_producer = connect_kafka_producer()
        publish_message(kafka_producer, 'all-votes', 'vote', json.dumps(request.json))
        if kafka_producer is not None:
            kafka_producer.close()
        return "JSON Message: " + json.dumps(request.json)
    else:
        return "415 Unsupported Media Type ;)"

def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        print(key_bytes, value_bytes)
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

if __name__ == '__main__':
    # vote = {
    #     'id': 1,
    #     'voter': 'Alice',
    #     'candidate': 'Bob'
    # }
    # kafka_producer = connect_kafka_producer()
    # publish_message(kafka_producer, 'all-votes', 'vote', json.dumps(vote))
    # if kafka_producer is not None:
    #     kafka_producer.close()

    app.run('127.0.0.1', 5000, debug = True)
