# coding=utf-8

# Copyright (c) 2001, Canal TP and/or its affiliates. All rights reserved.
#
# This file is part of Navitia,
#     the software to build cool stuff with public transport.
#
# Hope you'll enjoy and contribute to this project,
#     powered by Canal TP (www.canaltp.fr).
# Help us simplify mobility and open public transport:
#     a non ending quest to the responsive locomotion way of traveling!
#
# LICENCE: This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Stay tuned using
# twitter @navitia
# [matrix] channel #navitia:matrix.org (https://app.element.io/#/room/#navitia:matrix.org)
# https://groups.google.com/d/forum/navitia
# www.navitia.io

from __future__ import absolute_import, print_function, unicode_literals, division
import docker
import logging
import psycopg2
from io import BytesIO

from redis import ConnectionPool
from kombu import Connection
from kirin import Redis
from kirin.rabbitmq_handler import RabbitMQHandler
from typing import List, Optional, Dict, IO
from retrying import retry

"""
This module contains classes about Docker management.
"""

logger = logging.getLogger(__name__)  # type: logging.Logger


class DockerWrapper(object):
    """
    launch a temporary docker
    """

    def __init__(
        self,
        image_name,  # type: unicode
        container_name=None,  # type: Optional[unicode]
        dockerfile_obj=None,  # type: Optional[IO]
        dockerfile_path=None,  # type: Optional[unicode]
        env_vars={},  # type: Dict[unicode, unicode]
        mounts=None,  # type: Optional[List[docker.types.Mount]]
    ):
        # type: (...) -> None
        base_url = "unix://var/run/docker.sock"
        self.docker_client = docker.DockerClient(base_url=base_url)
        self.docker_api_client = docker.APIClient(base_url=base_url)
        self.image_name = image_name
        self.dockerfile_obj = dockerfile_obj
        self.dockerfile_path = dockerfile_path
        self.container_name = container_name
        self.env_vars = env_vars
        self.mounts = mounts or []

        logger.info("Trying to build/update the docker image")

        try:
            if self.dockerfile_path or self.dockerfile_obj:
                for build_output in self.docker_client.images.build(
                    path=self.dockerfile_path, fileobj=self.dockerfile_obj, tag=self.image_name, rm=True
                ):
                    logger.debug(build_output)
            else:
                self.docker_client.images.pull(self.image_name)

        except docker.errors.APIError as e:
            if e.is_server_error():
                logger.warning(
                    "[docker server error] A server error occcured, maybe missing internet connection?"
                )
                logger.warning("[docker server error] Details: {}".format(e))
                logger.warning(
                    "[docker server error] Checking if '{}' docker image is already built".format(
                        self.image_name
                    )
                )
                self.docker_client.images.get(self.image_name)
                logger.warning(
                    "[docker server error] Going on, as '{}' docker image is already built".format(
                        self.image_name
                    )
                )
            else:
                raise

        self.container = self.docker_client.containers.create(
            self.image_name, name=self.container_name, environment=self.env_vars, mounts=self.mounts
        )
        logger.info("docker id is {}".format(self.container.id))
        logger.info("starting the temporary docker")
        self.container.start()
        self.ip_addr = (
            self.docker_api_client.inspect_container(self.container.id)
            .get("NetworkSettings", {})
            .get("IPAddress")
        )

        if not self.ip_addr:
            logger.error("temporary docker {} not started".format(self.container.id))
            exit(1)

    def close(self):
        # type: () -> None
        """
        Terminate the Docker and clean it.
        """
        logger.info("stopping the temporary docker")
        self.container.stop()

        logger.info("removing the temporary docker")
        self.container.remove(v=True)

        # test to be sure the docker is removed at the end
        try:
            self.docker_client.containers.get(self.container.id)
        except docker.errors.NotFound:
            logger.info("the container is properly removed")
        else:
            logger.error("something is strange, the container is still there ...")
            exit(1)


class DbParams(object):
    """
    Class to store and manipulate database parameters.
    """

    def __init__(self, host, dbname, user, password):
        # type: (unicode, unicode, unicode, unicode) -> None
        """
        Constructor of DbParams.

        :param host: the host name
        :param dbname: the database name
        :param user: the user name to use for the database
        :param password: the password of the user
        """
        self.host = host
        self.user = user
        self.dbname = dbname
        self.password = password

    def cnx_string(self):
        # type: () -> unicode
        """
        The connection string for the database.
        A string containing all the essentials data for a connection to a database.

        :return: the connection string
        """
        return "postgresql://{u}:{pwd}@{h}/{dbname}".format(
            h=self.host, u=self.user, dbname=self.dbname, pwd=self.password
        )


class PostgresDockerWrapper(DockerWrapper):
    def __init__(
        self,
        image_name,  # type: unicode
        container_name=None,  # type: Optional[unicode]
        dockerfile_obj=None,  # type: Optional[IO]
        dockerfile_path=None,  # type: Optional[unicode]
        env_vars={},  # type: Dict[unicode, unicode]
        mounts=None,  # type: Optional[List[docker.types.Mount]]
        db_name="kirin_test",  # type: unicode
        db_user="postgres",  # type: unicode
        db_password="postgres",  # type: unicode
    ):
        # type: (...) -> None
        super(PostgresDockerWrapper, self).__init__(
            image_name, container_name, dockerfile_obj, dockerfile_path, env_vars, mounts
        )
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password

    def get_db_params(self):
        # type: () -> DbParams
        """
        Create the connection parameters of the database.
        Default user and password are "docker" and default database is "postgres".

        :return: the DbParams for the database of the Docker
        """
        return DbParams(self.ip_addr, self.db_name, self.db_user, self.db_password)

    @retry(stop_max_delay=30000, wait_fixed=100, retry_on_exception=lambda e: isinstance(e, Exception))
    def test_db_cnx(self):
        # type: () -> None
        """
        Test the connection to the database.
        """
        params = self.get_db_params()
        psycopg2.connect(database=params.dbname, user=params.user, password=params.password, host=params.host)


def postgres_docker(db_name="kirin_test", db_user="postgres", db_password="postgres", mounts=None):
    # type: (unicode, unicode, unicode, Optional[List[docker.types.Mount]]) -> PostgresDockerWrapper
    env_vars = {"POSTGRES_DB": db_name, "POSTGRES_USER": db_user, "POSTGRES_PASSWORD": db_password}

    postgres_image = "postgres:9.4"

    # The best way to get the image would be to get it from dockerhub,
    # but with this dumb wrapper the runtime time of the unit tests is reduced by 10s
    dockerfile_obj = BytesIO(str("FROM " + postgres_image))

    pg_wrap = PostgresDockerWrapper(
        image_name=postgres_image,
        dockerfile_obj=dockerfile_obj,
        container_name="kirin_test_postgres",
        db_name=db_name,
        db_user=db_user,
        db_password=db_password,
        env_vars=env_vars,
        mounts=mounts,
    )
    pg_wrap.test_db_cnx()  # we poll to ensure that the database is ready
    return pg_wrap


class RedisDockerWrapper(DockerWrapper):
    def get_redis_connection_pool(self):
        # type: () -> ConnectionPool
        return ConnectionPool(host=self.ip_addr)

    @retry(stop_max_delay=30000, wait_fixed=100, retry_on_exception=lambda e: isinstance(e, Exception))
    def test_redis_cnx(self):
        # type: () -> None
        """
        Test the connection to redis.
        """
        redis_cli = Redis(connection_pool=self.get_redis_connection_pool())
        redis_cli.set("test_redis_up", True)
        redis_cli.delete("test_redis_up")


def redis_docker(mounts=None):
    # type: (Optional[List[docker.types.Mount]]) -> RedisDockerWrapper
    redis_image = "redis:5-alpine"

    # The best way to get the image would be to get it from dockerhub,
    # but with this dumb wrapper the runtime time of the unit tests is reduced by 10s
    dockerfile_obj = BytesIO(str("FROM " + redis_image))

    redis_wrap = RedisDockerWrapper(
        image_name=redis_image, dockerfile_obj=dockerfile_obj, container_name="kirin_test_redis", mounts=mounts
    )
    redis_wrap.test_redis_cnx()

    return redis_wrap


class RabbitMQDockerWrapper(DockerWrapper):
    def __init__(
        self,
        image_name,  # type: unicode
        container_name=None,  # type: Optional[unicode]
        dockerfile_obj=None,  # type: Optional[IO]
        dockerfile_path=None,  # type: Optional[unicode]
        env_vars={},  # type: Dict[unicode, unicode]
        mounts=None,  # type: Optional[List[docker.types.Mount]]
    ):
        # type: (...) -> None
        super(RabbitMQDockerWrapper, self).__init__(
            image_name, container_name, dockerfile_obj, dockerfile_path, env_vars, mounts
        )
        protocol = "pyamqp"
        username = "guest"
        password = "guest"
        self.url = "{0}://{1}:{2}@{3}//?heartbeat=60".format(protocol, username, password, self.ip_addr)
        self.handlers = []  # type: List[RabbitMQHandler]

    def create_rabbitmq_handler(
        self,
        exchange_name,  # type: str
        exchange_type,  # type: str
    ):
        # type: (...) -> RabbitMQHandler
        handler = RabbitMQHandler(self.url, exchange_name, exchange_type)
        self.handlers.append(handler)
        return handler

    @retry(stop_max_delay=30000, wait_fixed=100, retry_on_exception=lambda e: isinstance(e, Exception))
    def test_rabbitmq_handler(self):
        connection = Connection(self.url)
        connection.connect()
        connection.release()

    def close(self):
        for handler in self.handlers:
            handler.close()
        super(RabbitMQDockerWrapper, self).close()


def rabbitmq_docker():
    rabbitmq_image = "rabbitmq:3"

    # The best way to get the image would be to get it from dockerhub,
    # but with this dumb wrapper the runtime time of the unit tests is reduced by 10s
    dockerfile_obj = BytesIO(str("FROM " + rabbitmq_image))

    rabbitmq_wrap = RabbitMQDockerWrapper(
        image_name=rabbitmq_image, dockerfile_obj=dockerfile_obj, container_name="kirin_test_rabbitmq"
    )
    rabbitmq_wrap.test_rabbitmq_handler()
    return rabbitmq_wrap
