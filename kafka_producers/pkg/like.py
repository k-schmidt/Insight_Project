"""
Insight Data Engineering
Kyle Schmidt

Like Kafka Producer
"""
from datetime import datetime
import json
import random
import time


def get_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def like_producer(servers, users, photos, tags, locations, producer):
    if not photos:
        return
    follower = random.choice(users)[0]
    photo, followee = random.choice(photos)
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
