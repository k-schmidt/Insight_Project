"""
Insight Data Engineering
Kyle Schmidt

Kafka Producer Test Script
"""
import random
import time
from typing import List

from cassandra.cluster import Cluster
import pymysql

from config_secure import SERVERS, MYSQL_CONF, CASSANDRA_CLUSTER
from pkg.comment import comment_producer
from pkg.create_user import create_user_producer
from pkg.follow import follow_producer
from pkg.like import like_producer
from pkg.photo_upload import create_photo_producer
from pkg.unfollow import unfollow_producer


def generate_random_events(events):
    return random.choice(events)


def main(servers: List[str]) -> None:
    """
    Main Method
    """
    cluster = Cluster(CASSANDRA_CLUSTER)
    mysql_session = pymysql.connect(**MYSQL_CONF)
    cassandra_session = cluster.connect("instabrand")

    events = [
        comment_producer,
        create_user_producer,
        follow_producer,
        #like_producer,
        create_photo_producer,
        #unfollow_producer
    ]

    while True:
        event = generate_random_events(events)
        print(event(servers, mysql_session, cassandra_session))
        time.sleep(0.02)

if __name__ == '__main__':
    main(SERVERS)
