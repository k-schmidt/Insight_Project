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


def get_link():
    return "link"


def get_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


def create_photo_producer(servers, users, photos, tags, locations):
    simple_client = SimpleClient(servers)
    producer = KeyedProducer(simple_client)
    user = random.choice(users)[0]
    tag = random.choice(tags)[0]
    latitude, longitude = random.choice(locations)
    created_time = get_datetime()
    link = get_link()
    if not user: return
    record = {
        "username": user,
        "tags": [tag],
        "photo_link": link,
        "created_time": created_time,
        "latitude": latitude,
        "longitude": longitude,
        "event": "photo-upload"
    }
    producer.send_messages('photo-upload',
                           bytes(user, 'utf-8'),
                           json.dumps(record).encode('utf-8'))
    photos.append(created_time)
    return record
