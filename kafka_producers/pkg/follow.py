"""
Insight Data Engineering
Kyle Schmidt

Follow Kafka Producer
"""
from datetime import datetime
import json
import random


def get_datetime():
    datetime_obj = datetime.now()
    return datetime_obj.strftime("%Y-%m-%d %H:%M:%S"), datetime_obj.strftime("%Y-%m-%d")


def follow_producer(servers, users, photos, tags, locations, producer):
    followee, follower = random.choice(users)[0], random.choice(users)[0]
    if not all([followee, follower]): return
    created_time, parition_date = get_datetime()
    record = {
        "follower_username": follower,
        "followed_username": followee,
        "created_time": created_time,
        "partition_date": parition_date,
        "event": "follow"
    }
    producer.send_messages("follow",
                           bytes(followee, 'utf-8'),
                           json.dumps(record).encode('utf-8'))
    return record
