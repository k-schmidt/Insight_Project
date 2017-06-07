"""
Insight Data Engineering
Kyle Schmidt

Users API
"""
from . import api
from ..decorators.json import json
from ..models import Users


@api.route("/users")
@json
def get_users():
    """
    Get all users from the database
    """
    return {"data": [record for record in Users.query.all()]}
