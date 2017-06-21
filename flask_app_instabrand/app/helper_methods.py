"""
Insight Data Engineering
Kyle Schmidt

Endpoint helpers
"""


def get_user_timeline(user, db_session):
    sql_query = f"SELECT * FROM home_status_updates WHERE timeline_username = '{user}' limit 20;"
    timeline = db_session.execute(sql_query)
    return timeline
