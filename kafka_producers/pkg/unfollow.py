"""
Insight Data Engineering
Kyle Schmidt

Unfollow Kafka Producer
"""
from datetime import datetime
import json
import random


def get_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def unfollow_producer(servers, users, photos, tags, locations, producer):
    followee, follower = random.choice(users)[0], random.choice(users)[0]
    if not all([followee, follower]): return
    record = {
        "follower_username": follower,
        "followed_username": followee,
        "created_time": get_datetime(),
        "event": "unfollow"
    }
    producer.send_messages("unfollow",
                           bytes(follower, 'utf-8'),
                           json.dumps(record).encode('utf-8'))
    return record
