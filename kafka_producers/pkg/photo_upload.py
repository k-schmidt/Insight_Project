"""
Insight Data Engineering
Kyle Schmidt

Photo Upload Kafka Producer
"""
from datetime import datetime
import json
import random
import string


def get_datetime():
    datetime_obj = datetime.now()
    return datetime_obj.strftime("%Y-%m-%d %H:%M:%S"), datetime_obj.strftime("%Y-%m-%d")


def create_photo_producer(servers, users, photos, tags, locations, producer):
    user = random.choice(users)[0]
    tag, link = random.choice(tags)
    latitude, longitude = random.choice(locations)
    created_time, partition_date = get_datetime()
    if not user: return
    record = {
        "username": user,
        "tags": tag,
        "photo_link": link,
        "created_time": created_time,
        "partition_date": partition_date,
        "latitude": latitude,
        "longitude": longitude,
        "event": "photo-upload"
    }
    producer.send_messages('photo-upload',
                           bytes(user, 'utf-8'),
                           json.dumps(record).encode('utf-8'))
    photos.append((created_time, user))
    return record
