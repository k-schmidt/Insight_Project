from datetime import datetime
import json
import time
import uuid

from cassandra import ConsistencyLevel
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, SparkSession

sc = SparkContext(appName="stream_direct")
ssc = StreamingContext(sc, 20)
topics = {"create-user": 1}

ZkQuorum = ["ec2-34-225-192-104.compute-1.amazonaws.com:2181",
            "ec2-34-197-55-50.compute-1.amazonaws.com:2181",
            "ec2-52-3-102-216.compute-1.amazonaws.com:2181"]


def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def create_stream(spark_stream_context,
                  zk_quorum,
                  group_name,
                  topic_dict):
    return KafkaUtils.createStream(spark_stream_context,
                                   ",".join(zk_quorum),
                                   group_name,
                                   topic_dict)
    # return KafkaUtils.createDirectStream(ssc,
    #                                      ["create-user"],
    #                                      {"bootstrap.servers": ",".join(zk_quorum)})

def timestamp_to_datetime(user_dict):
    datetime_obj = datetime.strptime(user_dict["created_time"], ("%Y-%m-%d %H:%M:%S.%f"))
    user_dict["created_time"] = datetime_obj
    return user_dict


def process(rdd):
    if rdd.take(1) == 0:
        return
        
    spark = getSparkSessionInstance(rdd.context.getConf())
    df = spark.read.json(rdd)
    #df.withColumn("time_uuid", uuid.UUID(df.created_time))
    # df = df.select(col("created_time"), df.created_time.cast(uuid.UUID).alias("created_time"))
    df.show()
    df.write.format("org.apache.spark.sql.cassandra").mode("append").options(table="users2", keyspace="instabrand").save()


def main():
    create_user_stream = create_stream(ssc,
                                       ZkQuorum,
                                       "create-user-group",
                                       topics)
    new_users = create_user_stream.map(lambda tup: tup[1])
                                  # .map(lambda json_dict: timestamp_to_datetime(json_dict))\
                                  # .map(lambda rdd: rdd.saveToCassandra("instabrand", "users", ConsistencyLevel.ANY))
    new_users.pprint()
    new_users.foreachRDD(process)
    #create_user_stream.saveAsTextFiles("s3a://insight-ks-test/create_user_stream/")


if __name__ == "__main__":
    main()
    ssc.start()
    ssc.awaitTermination()
