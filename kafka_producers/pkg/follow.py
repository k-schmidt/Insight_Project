"""
Insight Data Engineering
Kyle Schmidt

Follow Kafka Producer
"""
from datetime import datetime
import json
import time

from kafka.client import SimpleClient
from kafka.producer import KeyedProducer
from sqlalchemy.sql.expression import func


def get_datetime():
    return datetime.now().isoformat()


def get_new_follower(Session):
    session = Session()
    sql_string = "SELECT * from users order by rand();"
    followee = session.execute(sql_string).first()

    follower_string = "SELECT * from users left join followers on users.id = followers.followee where users.id != {} order by rand()".format(followee.id)
    follower = session.execute(follower_string).first()
    session.close()
    return followee, follower


def follow_producer(servers, Session):
    simple_client = SimpleClient(servers)
    producer = KeyedProducer(simple_client)
    followee, follower = get_new_follower(Session)
    if not follower or not follower.created_time: return
    record = {
        "followee": {
            "id": followee.id,
            "full_name": followee.full_name,
            "username": followee.username,
            "last_login": followee.last_login.isoformat(),
            "created_time": followee.created_time.isoformat(),
            "updated_time": followee.updated_time.isoformat()
        },
        "follower": {
            "id": follower.id,
            "full_name": follower.full_name,
            "username": follower.username,
            "last_login": follower.last_login.isoformat(),
            "created_time": follower.created_time.isoformat(),
            "updated_time": follower.updated_time.isoformat()
        },
        "created_time": get_datetime(),
        "updated_time": get_datetime()
    }
    if not record: return
    producer.send_messages("follow",
                           bytes(follower.username, 'utf-8'),
                           json.dumps(record).encode('ascii'))
    return record
