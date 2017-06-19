from datetime import datetime
import json

from cassandra import ConsistencyLevel
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, SparkSession

from config_secure import ZkQuorum

sc = SparkContext(appName="stream_direct")
ssc = StreamingContext(sc, 40)
sql = SQLContext(sc)

def getSparkSessionInstance(sparkConf):
    sparkConf.set("spark.streaming.concurrentJobs", "7")
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


def process_users(rdd):
    if rdd.isEmpty():
        return
    spark = getSparkSessionInstance(rdd.context.getConf())
    df = spark.read.json(rdd)
    df.show()
    df.write.format("org.apache.spark.sql.cassandra")\
            .mode("append")\
            .options(table="users",
                     keyspace="instabrand")\
            .save()
    for row in df.collect():
        user_df = sql.createDataFrame([
            {"followed_username": row.username, "follower_username": row.username}])
        user_df.write.format("org.apache.spark.sql.cassandra")\
                     .mode("append")\
                     .options(table="user_inbound_follows",
                              keyspace="instabrand")\
                     .save()
        user_df.write.format("org.apache.spark.sql.cassandra")\
                     .mode("append")\
                     .options(table="user_outbound_follows",
                                             keyspace="instabrand")\
                     .save()


def main():
    create_user_stream = create_stream(ssc,
                                       ZkQuorum,
                                       "create-user-group",
                                       {"create-user": 1})
    new_users = create_user_stream.map(lambda tup: tup[1])
    new_users.foreachRDD(process_users)


if __name__ == "__main__":
    main()
    ssc.start()
    ssc.awaitTermination()
