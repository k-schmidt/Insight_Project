from datetime import datetime
import json

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, SparkSession

from config_secure import ZkQuorum

sc = SparkContext(appName="stream_direct")
ssc = StreamingContext(sc, 40)
sql = SQLContext(sc)


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
    # sql.sql("DELETE FROM user_inbound_follows where followed_username = 'bradleyreyes639' and follower_username = 'sarahmorgan337';").collect()
    # rdd.map(lambda row: sql.sql("""DELETE FROM user_inbound_follows where followed_username = '{}' and follower_username = '{}'""".format(row.followed_username, row.follower_username)).collect())
    pass


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
