"""
Insight Data Engineering
Kyle Schmidt

Photo Upload Kafka Producer
"""
from datetime import datetime
import json
import random
import string

from kafka.client import SimpleClient
from kafka.producer import KeyedProducer


def query_for_user(mysql_session):
    sql_string = "SELECT * from users order by rand() limit 1;"
    mysql_session.execute(sql_string)
    sql_user = mysql_session.fetchone()
    return sql_user[1] if sql_user else None


def create_tags(mysql_session):
    sql_string = "SELECT * from tags order by rand() limit 1;"
    mysql_session.execute(sql_string)
    brand = mysql_session.fetchone()
    return brand[0] if brand else None


def generate_location(mysql_session):
    sql_string = "SELECT latitude, longitude from locations order by rand() limit 1;"
    mysql_session.execute(sql_string)
    latitude, longitude = mysql_session.fetchone()
    return latitude, longitude


def get_link():
    return "link"


def get_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


def create_photo_producer(servers, mysql_session, cassandra_session):
    with mysql_session.cursor() as cursor:
        simple_client = SimpleClient(servers)
        producer = KeyedProducer(simple_client)
        user = query_for_user(cursor)
        tag = create_tags(cursor)
        latitude, longitude = generate_location(cursor)
        created_time = get_datetime()
        link = get_link()
        if not user: return
        record = {
            "username": user,
            "tags": [tag],
            "photo_link": link,
            "created_time": created_time,
            "latitude": latitude,
            "longitude": longitude
        }
        producer.send_messages('photo-upload',
                               bytes(user, 'utf-8'),
                               json.dumps(record).encode('utf-8'))
    return record
