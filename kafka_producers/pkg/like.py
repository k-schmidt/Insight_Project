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
    datetime_obj = datetime.now()
    return datetime_obj.strftime("%Y-%m-%d %H:%M:%S"), datetime_obj.strftime("%Y-%m-%d")


def like_producer(servers, users, photos, tags, locations, producer):
    if not photos:
        return
    follower = random.choice(users)[0]
    photo, followee = random.choice(photos)
    if not all([follower, photo, followee]): return
    created_time, partition_date = get_datetime()
    record = {
        "follower_username": follower,
        "followed_username": followee,
        "photo_id": photo,
        "created_time": created_time,
        "partition_date": partition_date,
        "event": "like"
    }
    producer.send_messages("like",
                           bytes(followee, 'utf-8'),
                           json.dumps(record).encode('utf-8'))
    return record
