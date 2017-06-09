import json
import time

from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName="stream_direct")
ssc = StreamingContext(sc, 40)

topics = ["create-user"]
socket_format = "{}:{}"
first = ["ec2-34-225-192-104.compute-1.amazonaws.com",
         "ec2-34-197-55-50.compute-1.amazonaws.com",
         "ec2-52-3-102-216.compute-1.amazonaws.com"]

kafka_server_list = [socket_format.format(v, "9092") for v in first]
bootstrap_servers = ",".join(kafka_server_list)



create_user_stream = KafkaUtils.createDirectStream(ssc,
                                                   topics,
                                                   {"metadata.broker.list": bootstrap_servers})

create_user_stream.pprint()
#create_user_stream.saveAsTextFiles("s3a://insight-ks-test/create_user_stream/")

ssc.start()
ssc.awaitTermination()
