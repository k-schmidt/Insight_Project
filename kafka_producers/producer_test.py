"""
Insight Data Engineering
Kyle Schmidt

Kafka Producer Test Script
"""
import random
import time
from typing import List

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from pkg.config_secure import SERVERS, MYSQL_CONN
from pkg.comment import comment_producer
from pkg.create_user import create_user_producer
from pkg.follow import follow_producer
from pkg.like import like_producer
from pkg.photo_upload import create_photo_producer
from pkg.unfollow import unfollow_producer

engine = create_engine(MYSQL_CONN)


def generate_random_events(events):
    return random.choice(events)


def main(servers: List[str]) -> None:
    """
    Main Method
    """
    Session = sessionmaker(bind=engine)

    events = [
        comment_producer,
        create_user_producer,
        follow_producer,
        like_producer,
        create_photo_producer,
        unfollow_producer
    ]

    while True:
        event = generate_random_events(events)
        print(event(servers, Session))
        time.sleep(1)

if __name__ == '__main__':
    main(SERVERS)
