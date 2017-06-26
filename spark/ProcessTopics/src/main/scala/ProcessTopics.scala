package com.github.kschmidt
/* Provides methods to handle all streaming actions and metrics. */

import java.sql.DriverManager
import java.util.Properties

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
import com.mysql.jdbc.ConnectionProperties
import org.apache.ivy.plugins.repository.BasicResource

/* Main entrypoint to our streaming application. */
object ProcessStreams {

  /** Main function to handle application events and streaming aggregations.
    *
    * Creates a streaming context consuming six Kafka topics
    * - create-user
    * - follow
    * - unfollow
    * - like
    * - comment
    * - photo-upload
    *
    * The streaming context is further separated into influencerStream to process trending influencers,
    * brandStream to process trending brands, and reachStream to calculate how many people a brand's photos are reaching.
    *
    * The events are processed and saved to Cassandra while the aggregations are stored in MySQL.
    *
    * @param args zkQuorum jdbcUrl mysqlUser mysqlPassword.
    */
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: CreateUser <zkQuorum>")
      System.exit(1)
    }

    val Array(zkQuorum, jdbcUrl, mysqlUser, mysqlPassword) = args
    val sparkConf = new SparkConf(true).setAppName("CreateUser").set("spark.streaming.concurrentJobs", "7")
    val sparkSession = SparkSession.builder().config(conf = sparkConf).getOrCreate()
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(90))
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

    val influencerStream: DStream[String] = eventsStream.window(Seconds(360), Seconds(90))
    influencerStream.foreachRDD(processAggregates(_, keyspace, sparkSession, jdbcUrl, mysqlUser, mysqlPassword))

    val brandStream: DStream[String] = eventsStream.window(Seconds(360), Seconds(90))
    brandStream.foreachRDD(processTopBrands(_, keyspace, sparkSession, jdbcUrl, mysqlUser, mysqlPassword))

    val reachStream: DStream[String] = eventsStream.window(Seconds(360), Seconds(90))
    reachStream.foreachRDD(processReach(_, keyspace, sparkSession, jdbcUrl, mysqlUser, mysqlPassword))

    ssc.start()
    ssc.awaitTermination()
  }

  /** Calculates reach of brands in social network using the reachStream.
    *
    * Find all followers of the uesr who posted the photo with tag.
    * Find all followers of the user who commented or liked a photo.
    * Aggregate the uniques of this pool of people for each brand.
    * Save results to MySQL in table brand_reach.
    *
    * @param rdd Each rdd to process in the stream.
    * @param keyspace Cassandra keyspace to query for relationships.
    * @param sparksession Session used to convert rdd of json to Dataframe.
    * @param jdbcUrl String of MySQL URL.
    * @param mysqlUser String MySQL user.
    * @param mysqlPassword String MySQL password.
    */
  def processReach(rdd: RDD[String], keyspace: String, sparksession: SparkSession, jdbcUrl: String, mysqlUser: String, mysqlPassword: String): Unit = {
    implicit val rowWriter = SqlRowWriter.Factory
    import sparksession.sqlContext.implicits.StringToColumn
    val eventsDF: Dataset[Row] = sparksession.read.json(rdd)
    eventsDF.createOrReplaceTempView("events")

    val photoEvent: Dataset[Row] = sparksession.sql(
      """select
        |username,
        |created_time as photo_id,
        |tags
        |from events
        |where event = 'photo-upload'
      """.stripMargin
    )

    val reactionEvent: Dataset[Row] = sparksession.sql(
      """select
        |followed_username as username,
        |photo_id,
        |follower_username as follower
        |from events
        |where event in ('comment', 'like')
      """.stripMargin
    )

    val followees: Dataset[Row] = sparksession.sql(
      """select
        |username
        |from events
        |where username is not NULL
        |and event = 'photo-upload'
      """.stripMargin
    )

    val followers: Dataset[Row] = sparksession.sql(
      """select
          follower_username as followed_username
          from events
          where event in ('comment', 'like')
      """.stripMargin
    )

    val countLikes: DataFrame = sparksession.sql(
      """select
        |a.tags as tag,
        |count(*) as like_frequency
        |from events as a
        |inner join
        |(select *
        |from events
        |where event = 'like') as b
        |on a.username = b.followed_username
        |and a.created_time = b.photo_id
        |group by a.tags
      """.stripMargin
    )

    val countComments: DataFrame = sparksession.sql(
      """select
        |a.tags as tag,
        |count(*) as comment_frequency
        |from events as a
        |inner join (
        |select *
        |from events
        |where event = 'comment') as b
        |on a.username = b.followed_username
        |and a.created_time = b.photo_id
        |group by a.tags
      """.stripMargin
    )

    val firstDegreeFollowersReach: CassandraJoinRDD[Row, CassandraRow] =
      followees
        .rdd
        .joinWithCassandraTable("instabrand", "user_inbound_follows")

    val secondDegreeEventFollowers: CassandraJoinRDD[Row, CassandraRow] =
      followers
        .rdd
        .joinWithCassandraTable("instabrand", "user_inbound_follows")

    val firstJoin: RDD[(Row, Row)] =
      photoEvent
        .join(reactionEvent, Seq("username", "photo_id"))
        .rdd
        .map(row => (Row(row.getAs("username")), Row.fromSeq(row.toSeq)))

    val firstDegreeReach: RDD[(Row, Row)] =
      firstJoin
        .join(firstDegreeFollowersReach)
        .map(row => (Row(row._2._1.get(2)), Row(row._2._2.getString("follower_username"))))

    val secondDegreeReach: RDD[(Row, Row)] =
      firstJoin
        .keyBy(row => Row(row._2.get(3)))
        .join(secondDegreeEventFollowers)
        .map(row => (Row(row._2._1._2.get(2)), Row(row._2._2.getString("follower_username"))))

    val finalAggReach: RDD[(Row, Row)] = firstDegreeReach.union(secondDegreeReach)

    val finalGroupBy: RDD[Row] = finalAggReach.countApproxDistinctByKey(0.05, 15).map(row => Row(row._1.get(0), row._2))

    val finalGroupBySchema =
      StructType(
        StructField("tag", StringType) ::
          StructField("reach", LongType) :: Nil)

    val finalGroupByDF = sparksession.sqlContext.createDataFrame(finalGroupBy, finalGroupBySchema)

    val finalDF: RDD[Row] = finalGroupByDF.join(countComments, "tag").join(countLikes, "tag").rdd

    finalDF.foreachPartition { partitionOfRecords =>
      val connection = DriverManager.getConnection(jdbcUrl,
        mysqlUser,
        mysqlPassword)
      val prep = connection.prepareStatement("REPLACE INTO integration.brand_reach (tag, reach, comment_frequency, like_frequency) values (?, ?, ?, ?);")
      for (tuple <- partitionOfRecords) {
        prep.setString(1, tuple.get(0).asInstanceOf[String])
        prep.setLong(2, tuple.get(1).asInstanceOf[Long])
        prep.setLong(3, tuple.get(2).asInstanceOf[Long])
        prep.setLong(4, tuple.get(3).asInstanceOf[Long])
        prep.executeUpdate()
      }
      connection.close()
    }
  }

  /** Aggregation of the number of each brand tagged in photos being uploaded to the network.
    *
    * Groupby the tag names in each photo and the count of how often that tag shows up.
    *
    * @param rdd Each rdd to process in the stream.
    * @param keyspace Cassandra keyspace to query for relationships.
    * @param sparksession Session used to convert rdd of json to Dataframe.
    * @param jdbcUrl String of MySQL URL.
    * @param mysqlUser String MySQL user.
    * @param mysqlPassword String MySQL password.
    */
  def processTopBrands(rdd: RDD[String], keyspace: String, sparksession: SparkSession, jdbcUrl: String, mysqlUser: String, mysqlPassword: String): Unit = {
    val eventsDF: Dataset[Row] = sparksession.read.json(rdd)
    eventsDF.createOrReplaceTempView("events")

    val topBrands: RDD[Row] =
      sparksession.sql(
        """select
          |tags as tags,
          |count(*) as cnt
          |from events
          |where event = 'photo-upload'
          |group by tags
        """.stripMargin
      ).rdd

    topBrands.foreachPartition { partitionOfRecords =>
      val connection = DriverManager.getConnection(
        jdbcUrl,
        mysqlUser,
        mysqlPassword
      )
      val prep = connection.prepareStatement("REPLACE INTO integration.top_brands (tag, frequency) values (?, ?);")
      for (tuple <- partitionOfRecords) {
        prep.setString(1, tuple.get(0).asInstanceOf[String])
        prep.setLong(2, tuple.get(1).asInstanceOf[Long])
        prep.executeUpdate()
      }
      connection.close()
    }
  }

  /** Aggregation of the number of times a person has uploaded or whose photos have been commented or liked.
    *
    * Groupby the usernames in each photo upload and the followed_username of those like and comment events.
    * Followed_usernames are the original owner of the photo while follower_username are those who made the comment or like.
    *
    * @param rdd Each rdd to process in the stream.
    * @param keyspace Cassandra keyspace to query for relationships.
    * @param sparksession Session used to convert rdd of json to Dataframe.
    * @param jdbcUrl String of MySQL URL.
    * @param mysqlUser String MySQL user.
    * @param mysqlPassword String MySQL password.
    */
  def processAggregates(rdd: RDD[String], keyspace: String, sparksession: SparkSession, jdbcUrl: String, mysqlUser: String, mysqlPassword: String) {
    val eventsDF: Dataset[Row] = sparksession.read.json(rdd)
    eventsDF.createOrReplaceTempView("events")

    val recentInfluencers: RDD[Row] =
      sparksession.sql(
        """select
          |username,
          |sum(lower_count) as frequency
          |from (
          |select username,
          |count(*) as lower_count
          |from events
          |where event = 'photo-upload'
          |group by username
          |union all
          |select followed_username as username,
          |count(*) as lower_count
          |from events
          |where event in ("comment", "like")
          |group by followed_username)
          |group by username
          |order by frequency desc
        """.stripMargin
      ).rdd

    recentInfluencers.foreachPartition { partitionOfRecords =>
      val connection = DriverManager.getConnection(
        jdbcUrl,
        mysqlUser,
        mysqlPassword
      )
      val prep = connection.prepareStatement("REPLACE INTO integration.recent_influencers (username, frequency) values (?, ?);")
      for (tuple <- partitionOfRecords) {
        prep.setString(1, tuple.get(0).asInstanceOf[String])
        prep.setLong(2, tuple.get(1).asInstanceOf[Long])
        prep.executeUpdate()
      }
      connection.close()
    }
  }

  /** Process all actions that someone can take on the website.
    *
    * Actions are defined as:
    * - photo-upload
    * - create-user
    * - follow
    * = unfollow
    * - comment
    * - like
    *
    * Here we further split our RDDs by event-type so that we can do individual processing on each.
    *
    * @param totalRDD Each rdd to process in the stream.
    * @param keyspace Cassandra keyspace to query for relationships.
    * @param sparksession Session used to convert rdd of json to Dataframe.
    */
  def processTopLevelEvents(totalRDD: RDD[String], keyspace: String, sparksession: SparkSession) {
    val eventsDF: Dataset[Row] = sparksession.read.json(totalRDD)
    eventsDF.createOrReplaceTempView("events")

    if (eventsDF.columns.contains("full_name")) {
      val dfCreateUsers: Dataset[Row] =
        sparksession.sql(
          """select username,
            |full_name,
            |created_time
            |from events
            |where event = 'create-user'""".stripMargin)
      val dfCreateUserFollow: Dataset[Row] =
        sparksession.sql(
          """select username as followed_username,
            |username as follower_username
            |from events
            |where event = 'create-user'""".stripMargin)
      saveNewUsers(dfCreateUsers, dfCreateUserFollow, keyspace)
    }
    val dfFollow: Dataset[Row] =
      sparksession.sql(
        """select follower_username,
          |followed_username
          |from events
          |where event = 'follow'""".stripMargin)
    val dfUnFollow: Dataset[Row] =
      sparksession.sql(
        """select follower_username,
          |followed_username
          |from events
          |where event = 'unfollow'""".stripMargin)
    val dfPhotoUpload: Dataset[Row] =
      sparksession.sql(
        """select username,
          |tags,
          |photo_link,
          |latitude,
          |longitude,
          |created_time as photo_id
          |from events
          |where event = 'photo-upload'""".stripMargin)

    saveFollows(dfFollow, keyspace)
    saveUnfollows(dfUnFollow, keyspace)
    savePhotoUploads(dfPhotoUpload, keyspace, sparksession)
    if (eventsDF.columns.contains("photo_id")) {
      val dfLike: Dataset[Row] =
        sparksession.sql(
          """select distinct follower_username as liker,
            |followed_username as username,
            |photo_id
            |from events
            |where event = 'like'""".stripMargin)
      val dfComment: Dataset[Row] =
        sparksession.sql(
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
  }

  /** Handles the creation of new users.
    *
    * New users need to be added to two tables:
    * - user_inbound_follows
    * - user_outbond_follows
    *
    * This is because a user needs to follow themselves in order to see their updates to their own timeline.
    *
    * @param dfUser Dataframe of create-user events.
    * @param dfFollow Datafram of follow events.
    * @param keyspace Cassandra keyspace.
    */
  def saveNewUsers(dfUser: Dataset[Row], dfFollow: Dataset[Row], keyspace: String): Unit = {
    implicit val rowWriter = SqlRowWriter.Factory
    val rddUser: RDD[Row] = dfUser.rdd
    val rddFollow: RDD[Row] = dfFollow.rdd
    rddUser.saveToCassandra(keyspace, "users")
    rddFollow.saveToCassandra(keyspace, "user_inbound_follows")
    rddFollow.saveToCassandra(keyspace, "user_outbound_follows")
  }

  /** Handles the creation of new follower relationships.
    *
    * @param df Dataset of follow events.
    * @param keyspace Cassandra keyspace.
    */
  def saveFollows(df: Dataset[Row], keyspace: String): Unit = {
    implicit val rowWriter = SqlRowWriter.Factory
    val rdd = df.rdd
    val relationshipTables = List("user_inbound_follows", "user_outbound_follows")
    for (table <- relationshipTables) {
      rdd.saveToCassandra(keyspace, table)
    }
  }

  /** Handles the deletion of follower relationships.
    *
    * @param df Dataset of unfollow events
    * @param keyspace Cassandra keyspace
    */
  def saveUnfollows(df: Dataset[Row], keyspace: String): Unit = {
    implicit val rowWriter = SqlRowWriter.Factory
    val rdd = df.rdd
    val relationshipTables = List("user_inbound_follows", "user_outbound_follows")
    for (table <- relationshipTables) {
      rdd.deleteFromCassandra(
        keyspace,
        table)
    }
  }

  /** Handles all like events for updating the followed person's timeline.
    *
    * We need to look up the photo that has been liked in the user_status_updates table and update its likes.
    *
    * @param df Dataset of unfollow events.
    * @param keyspace Cassandra keyspace.
    * @param sparksession Spark Session
    */
  def saveUserLikes(df: Dataset[Row], keyspace: String, sparksession: SparkSession): Unit = {
    implicit val rowWriter = SqlRowWriter.Factory

    df
      .rdd
      .map(row => Row(row.get(1), row.get(2), row.get(0).asInstanceOf[String] :: Nil))
      .saveToCassandra(
        keyspace,
        "user_status_updates",
        SomeColumns("username", "photo_id" as "photo_id", "g_likes" as "liker" append))
  }

  /** Handles all like events for updating the followed person's timeline.
    *
    * We need to look up the photo that has been liked in the user_status_updates table and update its likes.
    * We also need to retrieve all followers of the user so that the new likes show up on their timeline.
    * The effect needs to update all followers timelines by writing to the home_status_updates table.
    *
    * @param df Dataset of unfollow events.
    * @param keyspace Cassandra keyspace.
    * @param sparksession Spark Session
    */
  def saveLikes(df: Dataset[Row], keyspace: String, sparksession: SparkSession): Unit = {
    implicit val rowWriter = SqlRowWriter.Factory
    df.createOrReplaceTempView("like")
    val rddFollowedUsers: RDD[Row] = sparksession.sql("""select username as followed_username from like""").rdd
    val rddFollowers: RDD[(Row, Row)] =
      rddFollowedUsers
        .joinWithCassandraTable(keyspace, "user_inbound_follows")
        .map(row => (row._1, Row.fromSeq(row._2.columnValues)))
    val rddEvent: RDD[(Row, Row)] = df.rdd.map(row => (Row(row.getAs("username")), Row.fromSeq(row.toSeq)))
    val rddJoinedEvent = rddEvent.join(rddFollowers)
    val rddResult =
      rddJoinedEvent
        .map(_._2)
        .map(row => Row(row._2.get(1), row._1.get(2), row._2.get(0), row._1.get(0).asInstanceOf[String] :: Nil))

    rddResult.saveToCassandra(
      keyspace,
      "home_status_updates",
      SomeColumns("timeline_username", "photo_id", "photo_username", "g_likes" append))
    saveUserLikes(df, keyspace, sparksession)
  }

  /** Handles all comment events for updating the followed person's timeline.
    *
    * We need to look up the photo that has been commented in the user_status_updates table and update its comments.
    *
    * @param df Dataset of unfollow events.
    * @param keyspace Cassandra keyspace.
    * @param sparksession Spark Session
    */
  def saveUserComments(df: Dataset[Row], keyspace: String, sparksession: SparkSession): Unit = {
    implicit val rowWriter = SqlRowWriter.Factory
    val rddUserUpdate = df.rdd.map(
      row => Row(
        row.get(1),
        row.get(2),
        (row.get(0).asInstanceOf[String], row.get(3).asInstanceOf[String]) :: Nil))
    rddUserUpdate
      .saveToCassandra(
        keyspace,
        "user_status_updates",
        SomeColumns("username", "photo_id", "g_comments" append))
  }

  /** Handles all comment events for updating the followed person's timeline.
    *
    * We need to look up the photo that has been commented in the user_status_updates table and update its comments.
    * We also need to retrieve all followers of the user so that the new comments show up on their timeline.
    * The effect needs to update all followers timelines by writing to the home_status_updates table.
    *
    * @param df Dataset of unfollow events.
    * @param keyspace Cassandra keyspace.
    * @param sparksession Spark Session
    */
  def saveComments(df: Dataset[Row], keyspace: String, sparksession: SparkSession): Unit = {
    implicit val rowWriter = SqlRowWriter.Factory
    df.createOrReplaceTempView("comment")

    val rddFollowedUsers: RDD[Row] = sparksession.sql("""select username as followed_username from comment""").rdd
    val rddFollowers: RDD[(Row, Row)] =
      rddFollowedUsers
        .joinWithCassandraTable(keyspace, "user_inbound_follows")
        .map(row => (row._1, Row.fromSeq(row._2.columnValues)))
    val rddEvent: RDD[(Row, Row)] = df.rdd.map(row => (Row(row.getAs("username")), Row.fromSeq(row.toSeq)))
    val rddJoinedEvent = rddEvent.join(rddFollowers)

    // timeline_username, photo-id, followed_username, comment-map
    val rddTimelineOutput =
      rddJoinedEvent
        .map(_._2)
        .map(row => Row(row._2.get(1), row._1.get(2), row._2.get(0), (row._1.get(0), row._1.get(3)) :: Nil))

    rddTimelineOutput.saveToCassandra(keyspace,
      "home_status_updates",
      SomeColumns("timeline_username", "photo_id", "photo_username", "g_comments" append))
    saveUserComments(df, keyspace, sparksession)
  }

  /** Handles all photo-upload events for updating the followed person's timeline.
    *
    * We need to add the created photo to the user_status_updates table for that user.
    *
    * @param df Dataset of unfollow events.
    * @param keyspace Cassandra keyspace.
    * @param sparksession Spark Session
    */
  def saveUserPhotoUploads(df: Dataset[Row], keyspace: String, sparksession: SparkSession): Unit = {
    implicit val rowWriter = SqlRowWriter.Factory
    // username, photo_id, g_tags, latitude, longitude, photo_link
    val rddUserUpdate = df.rdd.map(
      row => Row(
        row.get(0),
        row.get(5),
        row.get(1),
        row.get(3),
        row.get(4),
        row.get(2)))

    rddUserUpdate
      .saveToCassandra(
        keyspace,
        "user_status_updates",
        SomeColumns("username", "photo_id", "g_tags", "latitude", "longitude", "photo_link"))
  }

  /** Handles all photo-upload events for updating the followed person's timeline.
    *
    * We need to add the created photo to the user_status_updates table for that user.
    * We also need to retrieve all followers of the user so that the new photos show up on their timeline.
    * The effect needs to update all followers timelines by writing to the home_status_updates table.
    *
    * @param df Dataset of unfollow events.
    * @param keyspace Cassandra keyspace.
    * @param sparksession Spark Session
    */
  def savePhotoUploads(df: Dataset[Row], keyspace: String, sparksession: SparkSession): Unit = {
    implicit val rowWriter = SqlRowWriter.Factory
    df.createOrReplaceTempView("photo_upload")

    val rddFollowedUsers: RDD[Row] = sparksession.sql("""select username as followed_username from photo_upload""").rdd
    val rddFollowers: RDD[(Row, Row)] =
      rddFollowedUsers
        .joinWithCassandraTable(keyspace, "user_inbound_follows")
        .map(row => (row._1, Row.fromSeq(row._2.columnValues)))
    val rddEvent: RDD[(Row, Row)] = df.rdd.map(row => (Row(row.getAs("username")), Row.fromSeq(row.toSeq)))
    val rddJoinedEvent = rddEvent.join(rddFollowers)

    // timeline_username, photo-id, followed_username, g_tags, photo_latitude, photo_link, photo_longitude
    val rddTimelineOutput =
      rddJoinedEvent
        .map(_._2)
        .map(row => Row(row._2.get(1), row._1.get(5), row._2.get(0), row._1.get(1), row._1.get(3), row._1.get(2), row._1.get(4)))
    rddTimelineOutput
      .saveToCassandra(keyspace,
        "home_status_updates",
        SomeColumns("timeline_username", "photo_id", "photo_username", "g_tags", "photo_latitude", "photo_link", "photo_longitude"))
    saveUserPhotoUploads(df, keyspace, sparksession)
  }
}