"""
Insight Data Engineering
Kyle Schmidt

Application Initialization
"""
import os

from flask import Flask  # type: ignore
from flask_sqlalchemy import SQLAlchemy  # type: ignore

from .config import config, DevelopmentConfig

GITBRANCH = os.environ.get("GITBRANCH")
app = Flask(__name__)
app.config.from_object(config.get(GITBRANCH, DevelopmentConfig))
db = SQLAlchemy(app)
import pkg.models.users
from pkg.api import api
app.register_blueprint(api, url_prefix="/api")
