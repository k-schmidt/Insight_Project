import json
import time

from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName="stream_direct")
ssc = StreamingContext(sc, 40)

inputData = [[1, 2, 3],
             [0],
             [4, 4, 4],
             [0, 0, 0, 25],
             [1, -1, 10]]

rddQueue = []
for datum in inputData:
    rddQueue += [ssc.sparkContext.parallelize(datum)]

inputStream = ssc.queueStream(rddQueue)
inputStream.reduce(sum).pprint()

ssc.start()
time.sleep(5)
ssc.stop(stopSparkContext=True, stopGraceFully=True)

# topics = ["create-user"]
# socket_format = "{}:{}"
# first = ["ec2-52-44-117-247.compute-1.amazonaws.com",
#          "ec2-52-200-2-182.compute-1.amazonaws.com",
#          "ec2-34-198-26-209.compute-1.amazonaws.com"]

# kafka_server_list = [socket_format.format(v, "9092") for v in first]
# bootstrap_servers = ",".join(kafka_server_list)



# create_user_stream = KafkaUtils.createDirectStream(stream,
#                                                    topics,
#                                                    {"metadata.broker.list": bootstrap_servers})

# create_user_stream.pprint()
# #create_user_stream.saveAsTextFiles("s3a://insight-ks-test/create_user_stream/")

# stream.start()
# stream.awaitTermination()
