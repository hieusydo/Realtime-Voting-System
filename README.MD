# Real-time Voting System

## Versions
- Kafka: 2.2.0
- Spark/PySpark: 2.4.0
- Python: 3.7

## Kafka
Start ZooKeeper and Kafka (from extracted Kafka folder downloaded from Apache website)
```sh
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

Create topics
```sh
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic aggregate-votes
```

Delete
```sh
bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic aggregate-votes
```

## Python Server
```sh
$ python voting-service/producer.py
```

## MongoDB
```sh
$ mongod --config /usr/local/etc/mongod.conf
$ brew services start mongodb-community@4.0
```

## Reference
- https://kafka.apache.org/quickstart
- http://spark.apache.org/docs/2.4.0/api/python/pyspark.streaming.html
<<<<<<< HEAD
=======

weird bug
https://github.com/darkskyapp/forecast-ruby/issues/13
>>>>>>> upstream/master
