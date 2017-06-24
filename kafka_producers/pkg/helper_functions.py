"""
Insight Data Engineering
Kyle Schmidt

Helper functions for all Kafka topics
"""
from datetime import datetime
from typing import Tuple


def get_datetime() -> Tuple[str, str]:
    """
    Create timestamp at the second level and date

    Returns:
        Tuple datetime string %Y-%m-%d %H:%M:%S and date %Y-%m-%d
    """
    datetime_obj = datetime.now()
    return datetime_obj.strftime("%Y-%m-%d %H:%M:%S"), datetime_obj.strftime("%Y-%m-%d")
