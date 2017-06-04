"""
Insight Data Engineering
Kyle Schmidt

Configuration file for global vars
"""
import os

import boto3  # type: ignore


###############################################################################
# PATHS
###############################################################################
PATH_SRC: str = os.path.dirname(os.path.abspath(__file__))
PATH_INSTANCE: str = os.path.join(PATH_SRC, "instance")

###############################################################################
# AWS
###############################################################################
s3_client = boto3.resource('s3')  # pylint: disable=invalid-name
