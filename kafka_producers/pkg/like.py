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


def like_producer(servers, users, photos, tags, locations):
    if not photos:
        return

    simple_client = SimpleClient(servers)
    producer = KeyedProducer(simple_client)
    followee, follower = random.choice(users)[0], random.choice(users)[0]
    photo = random.choice(photos)
    if not all([follower, photo, followee]): return
    record = {
        "follower_username": follower,
        "followed_username": followee,
        "photo_id": photo,
        "created_time": get_datetime(),
        "event": "like"
    }
    producer.send_messages("like",
                           bytes(followee, 'utf-8'),
                           json.dumps(record).encode('utf-8'))
    return record
