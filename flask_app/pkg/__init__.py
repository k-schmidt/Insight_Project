"""
Insight Data Engineering
Kyle Schmidt

Application Initialization
"""
import os

import boto3
from flask import Flask  # type: ignore
from flask_sqlalchemy import SQLAlchemy  # type: ignore


def format_database_uri(username,
                        password,
                        host,
                        database,
                        protocol: str = "mysql+pymysql"):
    return f"{protocol}://{username}:{password}@{host}/{database}"


class ConfigBase:

    PATH_APP = os.path.abspath(os.path.dirname(__file__))

    S3_BUCKET = "instabrand-user-photos"
    S3_RESOURCE = boto3.resource('s3')
    S3_CLIENT = boto3.client('s3')

    RDS_HOST = os.environ.get("RDS_HOST")
    RDS_USERNAME = os.environ.get("RDS_USERNAME")
    RDS_PASSWORD = os.environ.get("RDS_PASSWORD")

    SQLALCHEMY_DATABASE_URI = format_database_uri(RDS_USERNAME,
                                                  RDS_PASSWORD,
                                                  RDS_HOST,
                                                  RDS_DATABASE)

class DevelopmentConfig(ConfigBase):
    DEBUG = True
    IGNORE_AUTH = True
    RDS_DATABASE = "development"

class IntegrationConfig(ConfigBase):
    DEBUG = True
    IGNORE_AUTH = True
    RDS_DATABASE = "integration"

class MasterConfig(ConfigBase):
    DEBUG = False
    IGNORE_AUTH = True
    RDS_DATABASE = "production"

config = {
    "development": DevelopmentConfig,
    "integration": IntegrationConfig,
    "master": MasterConfig
}

GITBRANCH = os.environ.get("GITBRANCH")

db = SQLAlchemy()
app = Flask(__name__)
app.config.from_object(config.get(GITBRANCH, DevelopmentConfig))
db.init_app(app)


def create_app(application_context: Flask) -> Flask:
    """
    Configure applicaiton by registering api endpoints
    """
    from api import flask_api
    application_context.register_blueprint(flask_api, url_prefix="/api/")
    return application_contex
