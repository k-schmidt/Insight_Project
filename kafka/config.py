"""
Insight Data Engineering
Kyle Schmidt

Configurations to generate random events for Kafka Producer
"""
from datetime import datetime


###############################################################################
# Event Generation
###############################################################################
datetime_format = "%Y-%m-%d %H:%M:%S"  # pylint: disable=invalid-name
date_format = "%Y-%m-%d"  # pylint: disable=invalid-name
users = [  # pylint: disable=invalid-name
    {
        "username": "kyleschmidt",
        "full_name": "Kyle Schmidt",
        "created_time": datetime.now().strftime(datetime_format)
    },
    {
        "username": "ericsloan",
        "full_name": "Eric Sloan",
        "created_time": datetime.now().strftime(datetime_format)
    },
    {
        "username": "joycewu",
        "full_name": "Joyce Wu",
        "created_time": datetime.now().strftime(datetime_format)
    },
    {
        "username": "rosemaryschmidt",
        "full_name": "Rosemary Schmidt",
        "created_time": datetime.now().strftime(datetime_format)
    }
]

tags = {  # pylint: disable=invalid-name
    "Coca-cola",
    "Microsoft",
    "Google",
    "Nike",
    "beach",
    "New York City",
    "Paris",
    "London"
}

locations = {  # pylint: disable=invalid-name
    "Hells Kitchen",
    "Palo Alto",
    "Menlo Park",
    "Seattle",
    "Sarasota",
    "Tampa"
}
