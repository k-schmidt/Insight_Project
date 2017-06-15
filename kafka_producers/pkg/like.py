"""
Insight Data Engineering
Kyle Schmidt

Like Kafka Producer
"""
from datetime import datetime
import json
import random
import time

from kafka.client import SimpleClient
from kafka.producer import KeyedProducer


def get_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


def query_for_user(mysql_session):
    sql_string = "SELECT * from users order by rand() limit 1;"
    mysql_session.execute(sql_string)
    user = mysql_session.fetchone()
    return user[1] if user else None


def query_follower(user, cassandra_session):
    cql_string = "SELECT * from user_inbound_follows where followed_username = '{}';".format(user)
    result = list(cassandra_session.execute(cql_string))
    follower = random.choice(result) if result else None
    return follower


def query_photos(user, cassandra_session):
    cql_string = "SELECT username, photo_id from user_status_updates where username = '{}'"\
        .format(user)
    result = list(cassandra_session.execute(cql_string))
    photo = random.choice(result) if result else None
    return photo


def like_producer(servers, mysql_session, cassandra_session):
    with mysql_session.cursor() as cursor:
        simple_client = SimpleClient(servers)
        producer = KeyedProducer(simple_client)
        user = query_for_user(cursor)
        photo = query_photos(user, cassandra_session)
        follower = query_follower(user, cassandra_session)
        if not all([follower, photo, user]): return
        record = {
            "follower_username": follower,
            "followed_username": user,
            "photo_id": photo.photo_id,
            "created_time": get_datetime(),
        }
        producer.send_messages("like",
                               bytes(follower, 'utf-8'),
                               json.dumps(record).encode('utf-8'))
    return record
