"""
Insight Data Engineering
Kyle Schmidt

Follow Kafka Producer
"""
from datetime import datetime
import json
import time
import uuid

from kafka.client import SimpleClient
from kafka.producer import KeyedProducer
from sqlalchemy.sql.expression import func


def get_datetime():
    return str(uuid.uuid1())


def get_new_follower(mysql_session):
    sql_string = "SELECT * from users order by rand() limit 1;"
    followee = list(mysql_session.execute(sql_string))
    follower = list(mysql_session.execute(sql_string))
    if followee is None or follower is None:
        return None, None
    while followee == follower:
        follower = list(mysql_session.execute(sql_string))

    return followee[0], follower[0]


def follow_producer(servers, mysql_session, cassandra_session):
    simple_client = SimpleClient(servers)
    producer = KeyedProducer(simple_client)
    followee, follower = get_new_follower(mysql_session)
    if not all([followee, follower]): return
    record = {
        "follower_username": follower.username,
        "followed_username": followee.username,
        "created_time": get_datetime(),
    }
    producer.send_messages("follow",
                           bytes(follower.username, 'ascii'),
                           json.dumps(record).encode('ascii'))
    return record
