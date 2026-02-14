import re

valid_imports = [
    'import dlt',
    'import logging',
    'import polars as pl',
    'import pandas as pd',
    'from dlt.sources.credentials import ConnectionStringCredentials',
    'from pathlib import Path',
    'from sys import path',
    'from src.services.workspace.Workspace import Workspace',
    'from src.services.workspace.SecretManager import SecretManager',
    'from src.services.workspace.SecretManager import referencedSecrets',
    'from src.utils import SQLServerUtil',
    'from dlt.sources.sql_database import sql_database, sql_table',
    'from os import getenv as env',
    'from src.utils.SQLDatabase import normalize_table_names, converts_field_type',
    'from sqlalchemy import create_engine',
    'from kafka import KafkaConsumer',
    'from certifi import where',
    'from json import loads',
    'from json import loads, JSONDecodeError',
    'from pymongo import MongoClient',
    'from src.services.workspace.SecretManager import referencedSecrets,SecretManager',
    'from dlt.sources.filesystem import filesystem, read_csv',
    'from dlt.sources import TDataItems',
    'from typing import Iterator, Any',
    'import requests',
    'from src.utils.APIClientUtil import PaginateParam',
    'import boto3',
    'from botocore import UNSIGNED',
    'from botocore.client import Config',
    'import fnmatch',
    'from dlt.sources.filesystem import filesystem, FileItemDict',
    'from src.utils.pipeline_logger_config import setup_dlt_logging, PipelineLogger',
    'from src.utils.logging.pipeline_logger_config import PipelineLogger',
    'from os import getenv as env, environ',
    'from dlt.sources.helpers.rest_client import RESTClient',
    'from src.utils.BucketConnector import get_bucket_credentials',
]

FORBIDDEN_CALLS = {"eval", "exec", "compile", "open"}

FORBIDDEN_CALLS_REGEX = re.compile(
(
    r'(?<![.\w])\b(exec|eval|compile|open|__import__|input|getattr|setattr|delattr|'
    r'globals|locals|vars|dir|help)\s*\((?!\s*[a-zA-Z_][\w]*\s*,)'
    r'|'
    r'(?<![.\w])\b(__builtins__|builtins)\s*\.\s*(exec|eval|compile|open|__import__|'
    r'input|getattr|setattr|delattr|globals|locals|vars|dir|help)\b'
    r'|'
    r'(?<![.\w])\b(exec|eval|compile|open|__import__|input|getattr|setattr|delattr|'
    r'globals|locals|vars|dir|help)\s*='
)
)

FORBIDDEN_DUNDER_REGEX = re.compile(
    r'\b(getattr|setattr|delattr)\s*\([^,)]+,\s*[\'\"]\s*__\w+__\s*[\'\"]\s*\)'
)