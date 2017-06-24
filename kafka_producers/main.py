"""
Insight Data Engineering
Kyle Schmidt

Kafka Producer Script
"""
from collections import deque
from multiprocessing import Process
import random
import time
from typing import List, Tuple

from kafka.client import SimpleClient
from kafka.producer import KeyedProducer
import pymysql

from pkg.comment import comment_producer
from pkg.config_secure import SERVERS, MYSQL_CONF
from pkg.create_user import create_user_producer
from pkg.follow import follow_producer
from pkg.like import like_producer
from pkg.photo_upload import create_photo_producer
from pkg.unfollow import unfollow_producer


def generate_random_events(events):
    """
    Randomly choose an event from a list of events

    Arguments:
        events: List of possible function calls

    Returns:
        Function object
    """
    return random.choice(events)


def query_for_users(mysql_session: pymysql.Connection) -> List[Tuple[str]]:
    """
    Query users from MySQL

    Arguments:
        mysql_session: MySQL Session object

    Returns:
        List of users
    """
    sql_string = "SELECT username from users;"
    with mysql_session.cursor() as cursor:
        cursor.execute(sql_string)
        users = cursor.fetchall()
    return users


def query_for_tags(mysql_session: pymysql.Connection) -> List[Tuple[str, str]]:
    """
    Query business names from MySQL

    Arguments;
        mysql_session: MySQL Session object

    Returns:
        List of tags
    """
    sql_string = "SELECT tag, link from tags;"
    with mysql_session.cursor() as cursor:
        cursor.execute(sql_string)
        tags = cursor.fetchall()
    return tags


def query_for_locations(mysql_session: pymysql.Connection) -> List[Tuple[str, str]]:
    """
    Query latitude/longitude locations from MySQL

    Arguments:
        mysql_session: MySQL Session object

    Returns:
        List of locations
    """
    sql_string = "SELECT latitude, longitude from locations;"
    with mysql_session.cursor() as cursor:
        cursor.execute(sql_string)
        locations = cursor.fetchall()
    return locations


def main(servers: List[str]) -> None:
    """
    Main Method

    Arguments:
        servers: List of Zookeeper Kafka Host IPs
    """
    mysql_session = pymysql.connect(**MYSQL_CONF)

    users = query_for_users(mysql_session)
    photos = deque([], maxlen=100)
    tags = query_for_tags(mysql_session)
    locations = query_for_locations(mysql_session)

    simple_client = SimpleClient(servers)
    producer = KeyedProducer(simple_client)

    events = [
        comment_producer,
        #create_user_producer,
        follow_producer,
        like_producer,
        create_photo_producer,
        unfollow_producer
    ]

    while True:
        event = generate_random_events(events)
        print(event(users, photos, tags, locations, producer))
        time.sleep(0.02)

if __name__ == '__main__':
    p1 = Process(target=main, args=(SERVERS,))
    p1.start()
    p2 = Process(target=main, args=(SERVERS,))
    p2.start()
