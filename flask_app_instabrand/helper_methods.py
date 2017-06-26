"""
Insight Data Engineering
Kyle Schmidt

Endpoint helpers
"""
from cassandra.cluster import Cluster  # pylint: disable=no-name-in-module
from sqlalchemy.engine.interfaces import Dialect  # type: ignore


def get_user_timeline(user: str, db_session: Cluster):
    """
    Retrieve the 20 most recent photos from a user's timeline

    User timelines are stored in table home_status_updates

    Arguments:
        user: Username to retrieve
        db_session: Cassandra instance

    Returns:
        20 most recent database records
    """
    sql_query = f"""
    SELECT *
    FROM home_status_updates
    WHERE timeline_username = '{user}' limit 20;"""
    timeline = db_session.execute(sql_query)
    return timeline


def get_top_brands(mysql_engine: Dialect):
    """
    Retrieve 20 top brands from database

    Top brands are stored in top_brands table

    Arguments:
        mysql_engine: MySQL connection used to execute sql string

    Returns:
        List of database tuple records
    """
    sql_string = """
    SELECT tag, frequency
    from top_brands
    where tag is not NULL
    order by frequency desc, tag limit 20"""
    brand_result = mysql_engine.execute(sql_string).fetchall()
    return brand_result


def get_top_influencers(mysql_engine: Dialect):
    """
    Retrieve top 20 recent influencers

    Top influencers are stored in recent_influencers table

    Arguments:
        mysql_engine: MySQL connection used to execute sql string

    Returns:
        List of database tuple records
    """
    influencer_string = """
    SELECT username, frequency
    from recent_influencers
    order by frequency desc, username
    limit 20"""
    influencer_result = mysql_engine.execute(influencer_string).fetchall()
    return influencer_result

def get_brand_metrics(mysql_engine: Dialect):
    """
    Retrieve brand engagement metrics ordered by engagement rate

    Arguments:
        mysql_engine: MySQL connection to execute sql string

    Returns:
        List of database tuple records
            - tag
            - reach
            - comment_frequency
            - like_frequency
            - comment_rate
            - like_rate
            - engagement_rate
    """
    brand_metrics = """
    SELECT tag, reach, comment_frequency,
    like_frequency, (comment_frequency / reach) * 100 as comment_rate,
    (like_frequency / reach) * 100 as like_rate,
    ((comment_frequency + like_frequency) / reach) * 100 as engagement_rate
    from brand_reach
    order by engagement_rate desc limit 20"""
    brand_metrics_result = mysql_engine.execute(brand_metrics).fetchall()
    return brand_metrics_result
