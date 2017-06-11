"""
Insight Data Engineering
Kyle Schmidt

Photo Upload Kafka Producer
"""
from datetime import datetime
import json
import random
import string
import time

from kafka.client import SimpleClient
from kafka.producer import KeyedProducer
from sqlalchemy.sql.expression import func


def query_for_user(mysql_session):
    sql_string = "SELECT * from users order by rand() limit 1;"
    sql_user = list(mysql_session.execute(sql_string))
    return sql_user[0] if sql_user else None


def create_tags(mysql_session):
    sql_string = "SELECT * from tags order by rand() limit 1;"
    brand = list(mysql_session.execute(sql_string))
    return brand[0] if brand else None


def generate_location(mysql_session):
    sql_string = "SELECT latitude, longitude from locations order by rand() limit 1;"
    location = list(mysql_session.execute(sql_string))
    return location[0] if location else None


def get_link():
    return "link"


def get_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def create_photo_producer(servers, mysql_session, cassandra_session):
    simple_client = SimpleClient(servers)
    producer = KeyedProducer(simple_client)
    user = query_for_user(mysql_session)
    tags = create_tags(mysql_session)
    location = generate_location(mysql_session)
    created_time = get_datetime()
    link = get_link()
    if not user: return
    record = {
        "username": user.username,
        "tags": [tag for tag in tags],
        "photo_link": link,
        "created_time": created_time,
        "latitude": location.latitude,
        "longitude": location.longitude
    }
    producer.send_messages('photo-upload',
                           bytes(user.username, 'utf-8'),
                           json.dumps(record).encode('utf-8'))
    return record
