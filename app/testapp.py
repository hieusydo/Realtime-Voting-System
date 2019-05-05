from flask import Flask, render_template, request, session, url_for, redirect
from kafka import KafkaConsumer

import json

topic_name = 'aggregate-votes'
consumer = KafkaConsumer(topic_name, bootstrap_servers=['localhost:9092'],
                        auto_offset_reset='earliest', group_id='group1', enable_auto_commit=True, consumer_timeout_ms=1000)
print('HELLO:', consumer)
for msg in consumer:
    print('check')
    dmsg = json.loads(msg.value)
    print(dmsg)


consumer.close()