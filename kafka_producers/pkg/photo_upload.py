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


def query_for_user(Session):
    session = Session()
    sql_string = "SELECT * from users order by rand();"
    user = session.execute(sql_string).first()
    session.close()
    return user


def create_tags():
    return ["".join([random.choice(string.ascii_letters)
                     for i in range(random.randrange(15))])]


def generate_location():
    range_x = (0, 2500)
    range_y = (0, 2500)
    x = random.randrange(*range_x)
    y = random.randrange(*range_y)
    return "({x},{y})".format(x=x, y=y)


def get_link():
    return "link"


def get_datetime():
    return datetime.now().isoformat()


def create_photo_producer(servers, Session):
    simple_client = SimpleClient(servers)
    producer = KeyedProducer(simple_client)
    user = query_for_user(Session)
    tags = create_tags()
    location = generate_location()
    created_time = get_datetime()
    updated_time = get_datetime()
    print(created_time)
    link = get_link()
    record = {
        "user": {
            "id": user.id,
            "full_name": user.full_name,
            "username": user.username,
            "last_login": user.last_login.isoformat(),
            "created_time": user.created_time.isoformat(),
            "updated_time": user.updated_time.isoformat()
        },
        "tags": [{"id": 22, "tag": tag}
                 for tag in tags],
        "link": link,
        "created_time": created_time,
        "updated_time": updated_time,
        "location": location
    }
    if not record: return
    producer.send_messages('photo-upload',
                           bytes(user.username, 'utf-8'),
                           json.dumps(record).encode('ascii'))
    return record
