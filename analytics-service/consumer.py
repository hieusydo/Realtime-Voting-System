import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0 pyspark-shell'

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from flask import Flask, render_template, request, session, url_for, redirect
from kafka import KafkaConsumer

import json

app = Flask(__name__)

global VOTE
VOTE = 0

def init_stream():
    # Integrate with Spark Streaming
    sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 5)

    topic_name = 'unverified-votes'
    kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {topic_name:1})
    parsed = kafkaStream.map(lambda v: json.loads(v[1]))
    parsed.map(lambda x:'Vote in this batch: %s' % x).pprint()

    global VOTE
    VOTE += parsed.count()

    ssc.start()
    ssc.awaitTermination()

@app.route('/view')
def draw_vote():
    return render_template('index.html', vote=VOTE)

if __name__ == "__main__":
    init_stream()
    app.run('127.0.0.1', 3718, debug = True)
