package com.github.kschmidt

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.cassandra._

import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraJoinRDD
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.writer.SqlRowWriter


object ProcessStreams {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: CreateUser <zkQuorum>")
      System.exit(1)
    }

    val Array(zkQuorum) = args
    val sparkConf = new SparkConf(true).setAppName("CreateUser").set("spark.streaming.concurrentJobs", "7")
    val sparkSession = SparkSession.builder().config(conf = sparkConf).getOrCreate()
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(50))

    val keyspace = "instabrand"
    ssc.checkpoint("checkpoint")

    val topics = Map(
      "create-user" -> 1,
      "follow" -> 1,
      "unfollow" -> 1,
      "like" -> 1,
      "comment" -> 1,
      "photo-upload" -> 1)

    val eventsStream: DStream[String] = KafkaUtils.createStream(ssc, zkQuorum, "events", topics).map(_._2)
    eventsStream.foreachRDD(processTopLevelEvents(_, keyspace, sparkSession))

    ssc.start()
    ssc.awaitTermination()
  }

  def dateString(): String = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val today = Calendar.getInstance().getTime()
    return formatter.format(today)
  }

  def processTopLevelEvents(totalRDD: RDD[String], keyspace: String, sparksession: SparkSession) {
    val eventsDF: Dataset[Row] = sparksession.read.json(totalRDD)
    eventsDF.collect.foreach(println)
    eventsDF.createOrReplaceTempView("events")

    if (eventsDF.columns.contains("full_name")) {
      val dfCreateUsers: Dataset[Row] = sparksession.sql(
        """select username,
        |full_name,
        |created_time
        |from events
        |where event = 'create-user'""".stripMargin)
    val dfCreateUserFollow: Dataset[Row] = sparksession.sql(
      """select username as followed_username,
        |username as follower_username
        |from events
        |where event = 'create-user'""".stripMargin)
      saveNewUsers(dfCreateUsers, dfCreateUserFollow, keyspace)
    }
    val dfFollow: Dataset[Row] = sparksession.sql(
      """select follower_username,
        |followed_username
        |from events
        |where event = 'follow'""".stripMargin)
    val dfUnFollow: Dataset[Row] = sparksession.sql(
      """select follower_username,
        |followed_username
        |from events
        |where event = 'unfollow'""".stripMargin)
    if (eventsDF.columns.contains("photo_id")) {
      val dfLike: Dataset[Row] = sparksession.sql(
        """select distinct follower_username as liker,
          |followed_username as username,
          |photo_id
          |from events
          |where event = 'like'""".stripMargin)
      val dfComment: Dataset[Row] = sparksession.sql(
        """select distinct follower_username as commenter,
          |followed_username as username,
          |photo_id,
          |text
          |from events
          |where event = 'comment'
        """.stripMargin)
      saveLikes(dfLike, keyspace, sparksession)
      saveComments(dfComment, keyspace, sparksession)
    }
    val dfPhotoUpload: Dataset[Row] = sparksession.sql(
      """select username,
        |tags,
        |photo_link,
        |latitude,
        |longitude,
        |created_time as photo_id
        |from events
        |where event = 'photo-upload'""".stripMargin)

    saveFollows(dfFollow, keyspace)
    saveUnfollows(dfFollow, keyspace)
    savePhotoUploads(dfPhotoUpload, keyspace, sparksession)
  }

  def saveNewUsers(dfUser: Dataset[Row], dfFollow: Dataset[Row], keyspace: String) {
    implicit val rowWriter = SqlRowWriter.Factory
    val rddUser: RDD[Row] = dfUser.rdd
    val rddFollow: RDD[Row] = dfFollow.rdd
    rddUser.saveToCassandra(keyspace, "users")
    rddFollow.saveToCassandra(keyspace, "user_inbound_follows")
    rddFollow.saveToCassandra(keyspace, "user_outbound_follows")
    val todayDate = dateString()
    val s3_path = s"s3n://insight-hdfs/users/$todayDate/users.json"
    dfUser.coalesce(1).write.mode(SaveMode.Append).json(s3_path)
  }


  def saveFollows(df: Dataset[Row], keyspace: String) {
    implicit val rowWriter = SqlRowWriter.Factory
    val rdd = df.rdd
    val relationshipTables = List("user_inbound_follows", "user_outbound_follows")
    for (table <- relationshipTables) {
      rdd.saveToCassandra(keyspace, table)
     }
    val todayDate = dateString()
    val s3_path = s"s3n://insight-hdfs/follows/$todayDate/follows.json"
    df.coalesce(1).write.mode(SaveMode.Append).json(s3_path)
  }

  def saveUnfollows(df: Dataset[Row], keyspace: String) {
    implicit val rowWriter = SqlRowWriter.Factory
    val rdd = df.rdd
    val relationshipTables = List("user_inbound_follows", "user_outbound_follows")
    for (table <- relationshipTables) {
      rdd.deleteFromCassandra(
        keyspace,
        table)
    }
    val todayDate = dateString()
    val s3_path = s"s3n://insight-hdfs/unfollow/$todayDate/unfollow.json"
    df.coalesce(1).write.mode(SaveMode.Append).json(s3_path)
  }

  def saveUserLikes(df: Dataset[Row], keyspace: String, sparksession: SparkSession) {
    implicit val rowWriter = SqlRowWriter.Factory
    df.rdd.map(row => Row(row.get(1), row.get(2), row.get(0).asInstanceOf[String] :: Nil))
      .saveToCassandra(
        keyspace,
        "user_status_updates",
        SomeColumns("username", "photo_id" as "photo_id", "k_likes" as "liker" append))
    val todayDate = dateString()
    val s3_path = s"s3n://insight-hdfs/likes/$todayDate/likes.json"
    df.coalesce(1).write.mode(SaveMode.Append).json(s3_path)
  }

  def saveLikes(df: Dataset[Row], keyspace: String, sparksession: SparkSession) {
    implicit val rowWriter = SqlRowWriter.Factory
    df.createOrReplaceTempView("like")
    val rddFollowedUsers: RDD[Row] = sparksession.sql("""select username as followed_username from like""").rdd
    val rddFollowers: RDD[(Row, Row)] = rddFollowedUsers.joinWithCassandraTable(keyspace, "user_inbound_follows")
      .map(row => (row._1, Row.fromSeq(row._2.columnValues)))
    val rddEvent: RDD[(Row, Row)] = df.rdd.map(row => (Row(row.getAs("username")), Row.fromSeq(row.toSeq)))
    val rddJoinedEvent = rddEvent.join(rddFollowers)
    val rddResult = rddJoinedEvent.map(_._2)
      .map(row => Row(row._2.get(1), row._1.get(2), row._2.get(0), row._1.get(0).asInstanceOf[String] :: Nil))

    rddResult.saveToCassandra(
      keyspace,
      "home_status_updates",
      SomeColumns("timeline_username", "photo_id", "photo_username", "k_likes" append))
    saveUserLikes(df, keyspace, sparksession)
  }

  def saveUserComments(df: Dataset[Row], keyspace: String, sparksession: SparkSession){
    implicit val rowWriter = SqlRowWriter.Factory
    val rddUserUpdate = df.rdd.map(
      row => Row(
        row.get(1),
        row.get(2),
        Map("username" -> row.get(0).asInstanceOf[String], "comment" -> row.get(3).asInstanceOf[String]) :: Nil))
    rddUserUpdate
      .saveToCassandra(
        keyspace,
        "user_status_updates",
        SomeColumns("username", "photo_id", "comments" append))
    val todayDate = dateString()
    val s3_path = s"s3n://insight-hdfs/comments/$todayDate/comments.json"
    df.coalesce(1).write.mode(SaveMode.Append).json(s3_path)
  }

  def saveComments(df: Dataset[Row], keyspace: String, sparksession: SparkSession) {
    implicit val rowWriter = SqlRowWriter.Factory
    df.createOrReplaceTempView("comment")

    val rddFollowedUsers: RDD[Row] = sparksession.sql("""select username as followed_username from comment""").rdd
    val rddFollowers: RDD[(Row, Row)] = rddFollowedUsers.joinWithCassandraTable(keyspace, "user_inbound_follows")
      .map(row => (row._1, Row.fromSeq(row._2.columnValues)))
    val rddEvent: RDD[(Row, Row)] = df.rdd.map(row => (Row(row.getAs("username")), Row.fromSeq(row.toSeq)))
    val rddJoinedEvent = rddEvent.join(rddFollowers)

    // timeline_username, photo-id, followed_username, comment-map
    val rddTimelineOutput = rddJoinedEvent.map(_._2)
      .map(row => Row(row._2.get(1), row._1.get(2), row._2.get(0), Map("username" -> row._1.get(0), "comment" -> row._1.get(3)) :: Nil))

    rddTimelineOutput.saveToCassandra(keyspace,
      "home_status_updates",
      SomeColumns("timeline_username", "photo_id", "photo_username", "photo_comments" append))
    saveUserComments(df, keyspace, sparksession)
  }

  def saveUserPhotoUploads(df: Dataset[Row], keyspace: String, sparksession: SparkSession){
    implicit val rowWriter = SqlRowWriter.Factory
    // username, photo_id, comments, k_likes, k_tags, latitude, longitude, photo_link
    val rddUserUpdate = df.rdd.map(
      row => Row(
        row.get(0),
        row.get(5),
        row.get(1),
        row.get(3),
        row.get(4),
        row.get(2)))

    rddUserUpdate.collect.foreach(println)
    rddUserUpdate
      .saveToCassandra(
        keyspace,
        "user_status_updates",
        SomeColumns("username", "photo_id", "k_tags", "latitude", "longitude", "photo_link"))
    val todayDate = dateString()
    val s3_path = s"s3n://insight-hdfs/photo_upload/$todayDate/photo_upload.json"
    df.coalesce(1).write.mode(SaveMode.Append).json(s3_path)
  }

  def savePhotoUploads(df: Dataset[Row], keyspace: String, sparksession: SparkSession) {
    implicit val rowWriter = SqlRowWriter.Factory
    df.createOrReplaceTempView("photo_upload")

    val rddFollowedUsers: RDD[Row] = sparksession.sql("""select username as followed_username from photo_upload""").rdd
    val rddFollowers: RDD[(Row, Row)] = rddFollowedUsers.joinWithCassandraTable(keyspace, "user_inbound_follows")
      .map(row => (row._1, Row.fromSeq(row._2.columnValues)))
    val rddEvent: RDD[(Row, Row)] = df.rdd.map(row => (Row(row.getAs("username")), Row.fromSeq(row.toSeq)))
    val rddJoinedEvent = rddEvent.join(rddFollowers)


    rddJoinedEvent.collect.foreach(println)
    // timeline_username, photo-id, followed_username, k_tags, photo_latitude, photo_link, photo_longitude
    val rddTimelineOutput = rddJoinedEvent.map(_._2)
      .map(row => Row(row._2.get(1), row._1.get(5), row._2.get(0), row._1.get(1), row._1.get(3), row._1.get(2), row._1.get(4)))
    rddTimelineOutput.saveToCassandra(keyspace, "home_status_updates",
      SomeColumns("timeline_username", "photo_id", "photo_username", "k_tags", "photo_latitude", "photo_link", "photo_longitude"))

    saveUserPhotoUploads(df, keyspace, sparksession)
  }
}