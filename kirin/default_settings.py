# encoding: utf-8

from __future__ import absolute_import, print_function, unicode_literals, division
import os
from flask_restful.inputs import boolean
import json
from datetime import timedelta
from celery import schedules
from kirin.helper import IdFilter

# URI for postgresql
# postgresql://<user>:<password>@<host>:<port>/<dbname>
# http://docs.sqlalchemy.org/en/rel_0_9/dialects/postgresql.html#psycopg2
SQLALCHEMY_DATABASE_URI = os.getenv(
    "KIRIN_SQLALCHEMY_DATABASE_URI", "postgresql://navitia:navitia@localhost/kirin"
)

NAVITIA_URL = os.getenv("KIRIN_NAVITIA_URL", None)

NAVITIA_TIMEOUT = int(os.getenv("KIRIN_NAVITIA_TIMEOUT", 5))

NAVITIA_INSTANCE = os.getenv("KIRIN_NAVITIA_INSTANCE", None)

NAVITIA_TOKEN = os.getenv("KIRIN_NAVITIA_TOKEN", None)

CELERY_BROKER_URL = os.getenv("KIRIN_CELERY_BROKER_URL", "pyamqp://guest:guest@localhost:5672//?heartbeat=60")


# COTS configuration
# * external configuration
COTS_CONTRIBUTOR = os.getenv("KIRIN_COTS_CONTRIBUTOR", None)
COTS_PAR_IV_API_KEY = os.getenv("KIRIN_COTS_PAR_IV_API_KEY", None)
COTS_PAR_IV_MOTIF_RESOURCE_SERVER = os.getenv("KIRIN_COTS_PAR_IV_MOTIF_RESOURCE_SERVER", None)
COTS_PAR_IV_TOKEN_SERVER = os.getenv("KIRIN_COTS_PAR_IV_TOKEN_SERVER", None)
COTS_PAR_IV_CLIENT_ID = os.getenv("KIRIN_COTS_PAR_IV_CLIENT_ID", None)
COTS_PAR_IV_CLIENT_SECRET = os.getenv("KIRIN_COTS_PAR_IV_CLIENT_SECRET", None)
COTS_PAR_IV_GRANT_TYPE = os.getenv("KIRIN_COTS_PAR_IV_GRANT_TYPE", "client_credentials")

# * technical configuration
# max instance call failures before stopping attempt
COTS_PAR_IV_CIRCUIT_BREAKER_MAX_FAIL = int(os.getenv("KIRIN_COTS_COTS_PAR_IV_CIRCUIT_BREAKER_MAX_FAIL", 4))
# the circuit breaker retries after this timeout (in seconds)
COTS_PAR_IV_CIRCUIT_BREAKER_TIMEOUT_S = int(
    os.getenv("KIRIN_COTS_COTS_PAR_IV_CIRCUIT_BREAKER_TIMEOUT_S", timedelta(minutes=1).total_seconds())
)
COTS_PAR_IV_TIMEOUT_TOKEN = int(
    os.getenv("KIRIN_COTS_COTS_PAR_IV_TIMEOUT_TOKEN", timedelta(minutes=30).total_seconds())
)
COTS_PAR_IV_CACHE_TIMEOUT = int(
    os.getenv("KIRIN_COTS_COTS_PAR_IV_CACHE_TIMEOUT", timedelta(hours=1).total_seconds())
)
COTS_PAR_IV_REQUEST_TIMEOUT = int(
    os.getenv("KIRIN_COTS_COTS_PAR_IV_REQUEST_TIMEOUT", timedelta(seconds=2).total_seconds())
)


# PIV configuration
BROKER_CONSUMER_CONFIGURATION_RELOAD_INTERVAL = int(
    os.getenv("KIRIN_BROKER_CONSUMER_CONFIGURATION_RELOAD_INTERVAL", timedelta(seconds=1).total_seconds())
)


# TODO : Remove when conf from db is ready
NAVITIA_GTFS_RT_INSTANCE = os.getenv("KIRIN_NAVITIA_GTFS_RT_INSTANCE", None)
NAVITIA_GTFS_RT_TOKEN = os.getenv("KIRIN_NAVITIA_GTFS_RT_TOKEN", None)
GTFS_RT_CONTRIBUTOR = os.getenv("KIRIN_GTFS_RT_CONTRIBUTOR", None)
GTFS_RT_FEED_URL = os.getenv("KIRIN_GTFS_RT_FEED_URL", None)
NB_DAYS_TO_KEEP_TRIP_UPDATE = int(os.getenv("KIRIN_NB_DAYS_TO_KEEP_TRIP_UPDATE", 2))
NB_DAYS_TO_KEEP_RT_UPDATE = int(os.getenv("KIRIN_NB_DAYS_TO_KEEP_RT_UPDATE", 10))
GTFS_RT_TIMEOUT = int(os.getenv("KIRIN_GTFS_RT_TIMEOUT", 1))

USE_GEVENT = boolean(os.getenv("KIRIN_USE_GEVENT", False))

DEBUG = boolean(os.getenv("KIRIN_DEBUG", False))

# rabbitmq connections string: http://kombu.readthedocs.org/en/latest/userguide/connections.html#urls
RABBITMQ_CONNECTION_STRING = os.getenv(
    "KIRIN_RABBITMQ_CONNECTION_STRING", "pyamqp://guest:guest@localhost:5672//?heartbeat=60"
)

# max nb of retries before giving up publishing
MAX_RETRIES = 10

# queue used for task of type load_realtime, all instances of kirin must use the same queue
# to be able to load balance tasks between them
LOAD_REALTIME_QUEUE = "kirin_load_realtime"

# amqp exhange used for sending disruptions
EXCHANGE = os.getenv("KIRIN_RABBITMQ_EXCHANGE", "navitia")

ENABLE_RABBITMQ = boolean(os.getenv("KIRIN_ENABLE_RABBITMQ", True))


NAVITIA_QUERY_CACHE_TIMEOUT = int(
    os.getenv("KIRIN_NAVITIA_QUERY_CACHE_TIMEOUT", timedelta(days=1).total_seconds())
)  # in seconds
NAVITIA_PUBDATE_CACHE_TIMEOUT = int(
    os.getenv("KIRIN_NAVITIA_PUBDATE_CACHE_TIMEOUT", timedelta(minutes=5).total_seconds())
)  # in seconds

CACHE_TYPE = os.getenv("KIRIN_CACHE_TYPE", "simple")

NEW_RELIC_CONFIG_FILE = os.getenv("KIRIN_NEW_RELIC_CONFIG_FILE", None)

log_level = os.getenv("KIRIN_LOG_LEVEL", "DEBUG")
log_format = os.getenv(
    "KIRIN_LOG_FORMAT", "[%(asctime)s] [%(levelname)5s] [%(process)5s] [%(name)25s] %(message)s"
)

log_formatter = os.getenv("KIRIN_LOG_FORMATTER", "default")  # can be 'default' or 'json'

log_extras = json.loads(os.getenv("KIRIN_LOG_EXTRAS", "{}"))  # fields to add to the logger

# Log Level available
# - DEBUG
# - INFO
# - WARN
# - ERROR

# logger configuration

LOGGER = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "default": {"format": log_format},
        "json": {"format": log_format, "extras": log_extras, "()": "kirin.utils.CustomJsonFormatter"},
    },
    "filters": {"IdFilter": {"()": IdFilter}},
    "handlers": {
        "default": {
            "level": log_level,
            "class": "logging.StreamHandler",
            "formatter": log_formatter,
            "filters": ["IdFilter"],
        }
    },
    "loggers": {
        "": {"handlers": ["default"], "level": "DEBUG", "propagate": False},
        "kirin": {"handlers": ["default"], "level": "DEBUG", "propagate": False},
        "amqp": {"level": "INFO"},
        "sqlalchemy.engine": {"handlers": ["default"], "level": "WARN", "propagate": False},
        "sqlalchemy.pool": {"handlers": ["default"], "level": "WARN", "propagate": False},
        "sqlalchemy.dialects.postgresql": {"handlers": ["default"], "level": "WARN", "propagate": False},
        "werkzeug": {"handlers": ["default"], "level": "WARN", "propagate": False},
        "celery.bootsteps": {"handlers": ["default"], "level": "WARN", "propagate": False},
    },
}

CELERYD_HIJACK_ROOT_LOGGER = False
CELERYBEAT_SCHEDULE_FILENAME = "/tmp/celerybeat-schedule-kirin"

REDIS_HOST = os.getenv("KIRIN_REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("KIRIN_REDIS_PORT", 6379))
# index of the database use in redis, between 0 and 15 by default
REDIS_DB = int(os.getenv("KIRIN_REDIS_DB", 1))
REDIS_PASSWORD = os.getenv("KIRIN_REDIS_PASSWORD", "")  # No password is needed by default

REDIS_LOCK_TIMEOUT_POLLER = int(
    os.getenv("KIRIN_REDIS_LOCK_TIMEOUT_POLLER", timedelta(minutes=5).total_seconds())
)
REDIS_LOCK_TIMEOUT_PURGE = int(os.getenv("KIRIN_REDIS_LOCK_TIMEOUT_PURGE", timedelta(hours=12).total_seconds()))

TASK_LOCK_PREFIX = "kirin.lock"
TASK_LAST_CALL_DATETIME_PREFIX = "kirin.last_exec_datetime"

TASK_STOP_MAX_DELAY = int(os.getenv("KIRIN_TASK_STOP_MAX_DELAY", timedelta(seconds=10).total_seconds()))
TASK_WAIT_FIXED = int(os.getenv("KIRIN_TASK_WAIT_FIXED", timedelta(seconds=2).total_seconds()))

# Must be >= 1. Defines the general minimal interval (seconds) between 2 possible tries for polling.
# WARNING: this is anyway also managed by retrieval_interval for each contributor
# (can also  be longer if a task is running).
POLLER_MIN_INTERVAL = int(os.getenv("KIRIN_POLLER_MIN_INTERVAL", timedelta(seconds=1).total_seconds()))

CELERYBEAT_SCHEDULE = {
    "poller": {
        "task": "kirin.tasks.poller",
        "schedule": timedelta(seconds=POLLER_MIN_INTERVAL),
        "options": {"expires": timedelta(seconds=POLLER_MIN_INTERVAL).total_seconds()},
    },
    "purge_gtfs_trip_update": {
        "task": "kirin.tasks.purge_gtfs_trip_update",
        "schedule": schedules.crontab(hour="3", minute="0"),
        "options": {"expires": timedelta(hours=1).total_seconds()},
    },
    "purge_gtfs_rt_update": {
        "task": "kirin.tasks.purge_gtfs_rt_update",
        "schedule": schedules.crontab(hour="3", minute="15"),
        "options": {"expires": timedelta(hours=1).total_seconds()},
    },
    "purge_piv_trip_update": {
        "task": "kirin.tasks.purge_piv_trip_update",
        "schedule": schedules.crontab(hour="3", minute="30"),
        "options": {"expires": timedelta(hours=1).total_seconds()},
    },
    "purge_piv_rt_update": {
        "task": "kirin.tasks.purge_piv_rt_update",
        "schedule": schedules.crontab(hour="3", minute="45"),
        "options": {"expires": timedelta(hours=1).total_seconds()},
    },
    "purge_cots_trip_update": {
        "task": "kirin.tasks.purge_cots_trip_update",
        "schedule": schedules.crontab(hour="4", minute="0"),
        "options": {"expires": timedelta(hours=1).total_seconds()},
    },
    "purge_cots_rt_update": {
        "task": "kirin.tasks.purge_cots_rt_update",
        "schedule": schedules.crontab(hour="4", minute="15"),
        "options": {"expires": timedelta(hours=1).total_seconds()},
    },
}

# https://flask-sqlalchemy.palletsprojects.com/en/2.x/signals/
# deprecated and slow
SQLALCHEMY_TRACK_MODIFICATIONS = False
