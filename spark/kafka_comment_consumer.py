from datetime import datetime
import json

from cassandra import ConsistencyLevel
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import *
import pyspark_cassandra

from config_secure import ZkQuorum, CASSANDRA_CLUSTER

sConf = SparkConf().set("spark.streaming.concurrentJobs", "7")\
                   .set(CASSANDRA_CLUSTER)
sc = pyspark_cassandra.CassandraSparkContext(conf=sConf)
ssc = StreamingContext(sc, 20)
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


def process_comments(rdd):
    if rdd.isEmpty():
        return

    spark = getSparkSessionInstance(rdd.context.getConf())
    df = spark.read.json(rdd)
    df.show()
    # my_udf = udf(lambda x, y: {"username": x, "comment": y}, MapType(StringType(), StringType()))
    # df = df.withColumn('comments', my_udf(df.follower_username, df.text))\
    #        .drop("follower_username", "text", "created_time")\
    #        .withColumnRenamed("followed_username", "username")
    comments_schema = StructType([
        StructField("comments", ArrayType(MapType(StringType(), StringType())))])
    append_if = udf(lambda orig_comments, new_commenter, new_comment: orig_comments.append({"username": new_commenter,
                                                                                            "comment": new_comment})
                    if orig_comments else [{"username": new_commenter,
                                            "comment": new_comment}], ArrayType(MapType(StringType(), StringType())))

    for row in df.collect():
        followers = sc.cassandraTable("instabrand", "user_inbound_follows")\
                      .select("follower_username")\
                      .where("followed_username=?", row.followed_username).toDF()
        comments_df = sql.createDataFrame(sc.cassandraTable("instabrand", "user_status_updates")
                                          .select("comments")
                                          .where("username=?", row.followed_username)
                                          .where("photo_id=?", row.photo_id), comments_schema)
        comments_df.show()
        revised_comments = comments_df.withColumn("comments", append_if(comments_df.comments, row.follower_username, row.text))
        followers.show()
        revise_coments.show()
    # df.show()
    # df.write.format("org.apache.spark.sql.cassandra").mode("append").options(table="user_status_updates", keyspace="instabrand").save()

    # photo_update_schema = StructType([
    #     StructField("photo_username", StringType(), False),
    #     StructField("photo_comments", ArrayType(MapType(StringType(), StringType())), True),
    #     StructField("photo_id", StringType(), False),
    #     StructField("photo_latitude", StringType(), True),
    #     StructField("photo_tags", ArrayType(StringType()), True),
    #     StructField("photo_longitude", StringType(), True),
    #     StructField("photo_likes", ArrayType(StringType()), True),
    #     StructField("photo_link", StringType(), True)])
    # for row in df.collect():
    #     photo_update_df = sql.createDataFrame([
    #         {"photo_username": row.username,
    #          "photo_id": row.photo_id,
    #          "photo_comments": getattr(row, "comments", None)}], photo_update_schema)
    #     rdd_followers = sc.cassandraTable("instabrand", "user_inbound_follows")\
    #                      .select("follower_username")\
    #                      .where("followed_username=?", row.username)
    #     if rdd_followers.isEmpty():
    #         continue
    #     df_followers = rdd_followers.toDF()
    #     df_followers.show()
    #     df_followers_renamed = df_followers.withColumnRenamed("follower_username", "timeline_username")
    #     df_followers_renamed.show()
    #     cart_join = df_followers_renamed.crossJoin(photo_update_df)
    #     cart_join.show()
    #     cart_join.write.format("org.apache.spark.sql.cassandra").mode("append").options(table="home_status_updates", keyspace="instabrand").save()


def main():
    comment_stream = create_stream(ssc,
                                   ZkQuorum,
                                   "comment-group",
                                   {"comment": 1})
    comment = comment_stream.map(lambda tup: tup[1])
    comment.foreachRDD(process_comments)


if __name__ == "__main__":
    main()
    ssc.start()
    ssc.awaitTermination()
