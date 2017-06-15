from datetime import datetime
import json

from cassandra import ConsistencyLevel
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, SparkSession

from config_secure import ZkQuorum, CASSANDRA_CLUSTER

sConf = SparkConf().set("spark.streaming.concurrentJobs", "7")\
                   .set("spark.cassandra.connection.host", CASSANDRA_CLUSTER)
sc = SparkContext(appName="stream_direct", conf=sConf)
ssc = StreamingContext(sc, 40)


def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession.builder\
                                                                 .config(conf=sparkConf)\
                                                                 .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def create_stream(spark_stream_context,
                  zk_quorum,
                  group_name,
                  topic_dict):
    zookeeper_ips = ",".join(zk_quorum)
    
    return KafkaUtils.createStream(spark_stream_context,
                                   zookeeper_ips,
                                   group_name,
                                   topic_dict)


def process_follows(rdd):
    """
    Think about updating a person's timeline after following another user
    """
    spark = getSparkSessionInstance(rdd.context.getConf())
    df = spark.read.json(rdd)
    df = df.drop("created_time")
    df.show()
    df.write\
      .format("org.apache.spark.sql.cassandra")\
      .mode("append")\
      .options(table="user_outbound_follows", keyspace="instabrand")\
      .save()
    df.write\
      .format("org.apache.spark.sql.cassandra")\
      .mode("append")\
      .options(table="user_inbound_follows", keyspace="instabrand")\
      .save()


def main():
    follow_events_stream = create_stream(ssc,
                                         ZkQuorum,
                                         "follow-events-group",
                                         {"follow": 1})
    follow_event = follow_events_stream.map(lambda tup: tup[1])
    follow_event.foreachRDD(process_follows)


if __name__ == "__main__":
    main()
    ssc.start()
    ssc.awaitTermination()
