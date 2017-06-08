"""
Insight Data Engineering
Kyle Schmidt

Unfollow Kafka Producer
"""
from datetime import datetime
import json
import random
import time

from kafka.client import SimpleClient
from kafka.producer import KeyedProducer
from sqlalchemy.sql.expression import func


def get_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_unfollower(Session):
    session = Session()
    sql_string = "SELECT * from users order by rand();"
    user = session.execute(sql_string).first()
    follower_string = "SELECT * from users left join followers on users.id = followers.followee where users.id = {} order by rand()".format(user.id)
    follower = session.execute(sql_string).first()
    session.close()
    return user, follower


def unfollow_producer(servers, Session):
    # simple_client = SimpleClient(servers)
    # producer = KeyedProducer(simple_client)
    followee, follower = get_unfollower(Session)
    if not follower: return
    record = {
        "followee": {
            "id": followee.id,
            "full_name": followee.full_name,
            "username": followee.username,
            "last_login": followee.last_login,
            "created_time": followee.created_time,
            "updated_time": followee.updated_time
        },
        "follower": {
            "id": follower.id,
            "full_name": follower.full_name,
            "username": follower.username,
            "last_login": follower.last_login,
            "created_time": follower.created_time,
            "updated_time": follower.updated_time
        },
        "created_time": get_datetime(),
        "updated_time": get_datetime()
    }
    if not record: return
    # producer.send_messages("unfollow",
    #                        bytes(follower.username, 'utf-8'),
    #                        json.dumps(record).encode('ascii'))
    return record
