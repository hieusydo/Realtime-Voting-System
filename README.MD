# Real-time Voting System

## Versions
- Kafka: 2.2.0
- Spark/PySpark: 2.4.0
- Python: 3.7

## Kafka
Start ZooKeeper and Kafka (from extracted Kafka folder downloaded from Apache website)
```sh
$ bin/zookeeper-server-start.sh config/zookeeper.properties
$ bin/kafka-server-start.sh config/server.properties
```

## Python Server
```sh
$ python voting-service/producer.py
```

## Reference
- https://kafka.apache.org/quickstart
- http://spark.apache.org/docs/2.4.0/api/python/pyspark.streaming.html