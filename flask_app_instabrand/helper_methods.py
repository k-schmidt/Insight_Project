"""
Insight Data Engineering
Kyle Schmidt

Endpoint helpers
"""


def get_user_timeline(user, db_session):
    sql_query = f"""
    SELECT *
    FROM home_status_updates
    WHERE timeline_username = '{user}' limit 20;"""
    timeline = db_session.execute(sql_query)
    return timeline


def get_top_brands(mysql_engine):
    sql_string = """
    SELECT tags, cnt
    from top_brands
    where tags is not NULL
    order by cnt desc, tags limit 20"""
    brand_result = mysql_engine.execute(sql_string).fetchall()
    return brand_result


def get_top_influencers(mysql_engine):
    influencer_string = """
    SELECT username, frequency
    from recent_influencers
    order by frequency desc, username
    limit 20"""
    influencer_result = mysql_engine.execute(influencer_string).fetchall()
    return influencer_result

def get_brand_metrics(mysql_engine):
    brand_metrics = """
    SELECT tag, reach, comment_frequency,
    like_frequency, (comment_frequency / reach) * 100 as comment_rate,
    (like_frequency / reach) * 100 as like_rate,
    ((comment_frequency + like_frequency) / reach) * 100 as engagement_rate
    from brand_reach
    order by engagement_rate desc limit 20"""
    brand_metrics_result = mysql_engine.execute(brand_metrics).fetchall()
    return brand_metrics_result
