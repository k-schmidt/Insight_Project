"""
Insight Data Engineering
Kyle Schmidt

Users API
"""
from . import api
from .. import ma
from ..decorators.json import json
from ..models import Users


class UserSchema(ma.ModelSchema):
    class Meta:
        model = Users


@api.route("/users")
@json
def get_users():
    """
    Get all users from the database
    """
    user_schema = UserSchema()
    return {"data": [user_schema.dump(user).data for user in Users.query.all()]}
