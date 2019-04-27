import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0 pyspark-shell'

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

from kafka import KafkaConsumer


print('Running Consumer..')


'''
    Separate Kafka consumer
'''
# consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest',
#                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
# for msg in consumer:
#     v = msg.value
#     print(v)
# consumer.close()


'''
    Integrate with Spark Streaming
'''
sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, 5)

topic_name = 'all-votes'
kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {topic_name:1})
parsed = kafkaStream.map(lambda v: json.loads(v[1]))
parsed.map(lambda x:'Vote in this batch: %s' % x).pprint()

ssc.start()
ssc.awaitTermination()