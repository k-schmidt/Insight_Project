from datetime import datetime
import json
import time

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext

from config_secure import ZkQuorum

sc = SparkContext(appName="stream_direct")
ssc = StreamingContext(sc, 40)
topics = {"create-user": 1}


def create_stream(spark_stream_context,
                  zk_quorum,
                  group_name,
                  topic_dict):
    return KafkaUtils.createStream(spark_stream_context,
                                   ",".join(zk_quorum),
                                   group_name,
                                   topic_dict)

def timestamp_to_datetime(user_dict):
    datetime_obj = datetime.strptime(user_dict["created_time"], ("%Y-%m-%d %H:%M:%S.%f"))
    user_dict["created_time"] = datetime_obj
    return user_dict


def main():
    create_user_stream = create_stream(ssc,
                                       ZkQuorum,
                                       "create-user-group",
                                       topics)
    create_user_stream.map(lambda tup: json.loads(tup[1]))\
                      .map(lambda json_dict: timestamp_to_datetime(json_dict))\
                      .map(lambda rdd: rdd.saveToCassandra("instabrand", "users"))
    #create_user_stream.saveAsTextFiles("s3a://insight-ks-test/create_user_stream/")


if __name__ == "__main__":
    main()
    ssc.start()
    ssc.awaitTermination()
