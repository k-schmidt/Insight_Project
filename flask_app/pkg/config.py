"""
Insight Data Engineering
Kyle Schmidt

Application Configuration
"""
import abc
import os

import boto3


class ConfigBase(abc.ABCMeta):

    PATH_APP = os.path.abspath(os.path.dirname(__file__))

    S3_BUCKET = "instabrand-user-photos"
    S3_RESOURCE = boto3.resource('s3')
    S3_CLIENT = boto3.client('s3')

    RDS_HOST = os.environ.get("RDS_HOST")
    RDS_USERNAME = os.environ.get("RDS_USERNAME")
    RDS_PASSWORD = os.environ.get("RDS_PASSWORD")

    SQLALCHEMY_TRACK_MODIFICATIONS = False

    @classmethod
    def format_database_uri(cls,
                            database,
                            protocol: str = "mysql+pymysql"):
        return "{protocol}://{username}:{password}@{host}/{database}".format(
            protocol=protocol,
            username=cls.RDS_USERNAME,
            password=cls.RDS_PASSWORD,
            host=cls.RDS_HOST,
            database=database)

class DevelopmentConfig(ConfigBase):
    DEBUG = True
    IGNORE_AUTH = True
    RDS_DATABASE = "development"
    SQLALCHEMY_DATABASE_URI = ConfigBase.format_database_uri(RDS_DATABASE)

class IntegrationConfig(ConfigBase):
    DEBUG = True
    IGNORE_AUTH = True
    RDS_DATABASE = "integration"
    SQLALCHEMY_DATABASE_URI = ConfigBase.format_database_uri(RDS_DATABASE)

class MasterConfig(ConfigBase):
    DEBUG = False
    IGNORE_AUTH = True
    RDS_DATABASE = "production"
    SQLALCHEMY_DATABASE_URI = ConfigBase.format_database_uri(RDS_DATABASE)

config = {
    "development": DevelopmentConfig,
    "integration": IntegrationConfig,
    "master": MasterConfig
}
