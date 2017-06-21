from datetime import datetime
import json

from cassandra import ConsistencyLevel
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *
import pyspark_cassandra

from config_secure import ZkQuorum

sConf = SparkConf().set("spark.streaming.concurrentJobs", "7")\
                   .set("spark.cassandra.connection.host",
                        CASSANDRA_CLUSTER)
sc = pyspark_cassandra.CassandraSparkContext(conf=sConf)
ssc = StreamingContext(sc, 50)
sql = SQLContext(sc)

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


def process_photos(rdd):
    if rdd.isEmpty():
        return

    spark = getSparkSessionInstance(rdd.context.getConf())
    df = sql.read.json(rdd)
    df2 = df.select(df.username,
                    df.tags,
                    df.photo_link,
                    df.latitude,
                    df.longitude,
                    df.created_time.alias("photo_id"))
    df2.write.format("org.apache.spark.sql.cassandra")\
             .mode("append")\
             .options(table="user_status_updates", keyspace="instabrand")\
             .save()

    photo_update_schema = StructType([
        StructField("photo_username", StringType(), False),
        StructField("photo_comments", ArrayType(MapType(StringType(), StringType())), True),
        StructField("photo_id", StringType(), False),
        StructField("photo_latitude", StringType(), True),
        StructField("photo_tags", ArrayType(StringType()), True),
        StructField("photo_longitude", StringType(), True),
        StructField("photo_likes", ArrayType(StringType()), True),
        StructField("photo_link", StringType(), True)])
    for row in df2.collect():
        photo_update_df = sql.createDataFrame([
            {"photo_username": row.username,
             "photo_id": row.photo_id,
             "photo_comments": getattr(row, "comments", None),
             "photo_tags": getattr(row, "tags", None),
             "photo_latitude": row.latitude,
             "photo_longitude": row.longitude,
             "photo_likes": getattr(row, "likes", None),
             "photo_link": row.photo_link}], photo_update_schema)
        rdd_followers = sc.cassandraTable("instabrand", "user_inbound_follows")\
                         .select("follower_username")\
                         .where("followed_username=?", row.username)
        if rdd_followers.isEmpty():
            continue
        df_followers = rdd_followers.toDF()
        df_followers.show()
        df_followers_renamed = df_followers.withColumnRenamed("follower_username", "timeline_username")
        df_followers_renamed.show()
        cart_join = df_followers_renamed.crossJoin(photo_update_df)
        cart_join.show()
        cart_join.write.format("org.apache.spark.sql.cassandra")\
                       .mode("append")\
                       .options(table="home_status_updates", keyspace="instabrand")\
                       .save()


def main():
    photo_upload_stream = create_stream(ssc,
                                        ZkQuorum,
                                        "photo-upload",
                                        {"photo-upload": 1})
    photo_upload = photo_upload_stream.map(lambda tup: tup[1])
    photo_upload.pprint()
    photo_upload.foreachRDD(process_photos)


if __name__ == "__main__":
    main()
    ssc.start()
    ssc.awaitTermination()
