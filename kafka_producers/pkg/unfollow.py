"""
Insight Data Engineering
Kyle Schmidt

Unfollow Kafka Producer
"""
from datetime import datetime
import json
import random

from kafka.client import SimpleClient
from kafka.producer import KeyedProducer


def get_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


def get_unfollower(mysql_session):
    sql_string = "SELECT * from users order by rand() limit 1;"
    mysql_session.execute(sql_string)
    followee = mysql_session.fetchone()
    mysql_session.execute(sql_string)
    follower = mysql_session.fetchone()
    if followee is None or follower is None:
        return None, None
    while followee == follower:
        mysql_session.execute(sql_string)
        follower = mysql_session.fetchone()
        
    return followee[1], follower[1]  # username


def unfollow_producer(servers, mysql_session, cassandra_session):
    with mysql_session.cursor() as cursor:
        simple_client = SimpleClient(servers)
        producer = KeyedProducer(simple_client)
        followee, follower = get_unfollower(cursor)
        if not all([followee, follower]): return
        record = {
            "follower_username": follower,
            "followed_username": followee,
            "created_time": get_datetime()
        }
        producer.send_messages("unfollow-event",
                               bytes(follower, 'utf-8'),
                               json.dumps(record).encode('utf-8'))
    return record
