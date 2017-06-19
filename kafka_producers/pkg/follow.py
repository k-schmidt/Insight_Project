"""
Insight Data Engineering
Kyle Schmidt

Follow Kafka Producer
"""
from datetime import datetime
import json
import random

from kafka.client import SimpleClient
from kafka.producer import KeyedProducer


def get_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


def follow_producer(servers, users, photos, tags, locations):
    simple_client = SimpleClient(servers)
    producer = KeyedProducer(simple_client)
    followee, follower = random.choice(users)[0], random.choice(users)[0]
    if not all([followee, follower]): return
    record = {
        "follower_username": follower,
        "followed_username": followee,
        "created_time": get_datetime(),
        "event": "follow"
    }
    producer.send_messages("follow",
                           bytes(followee, 'utf-8'),
                           json.dumps(record).encode('utf-8'))
    return record
