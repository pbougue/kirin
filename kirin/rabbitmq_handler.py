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

import six
from kombu import BrokerConnection, Exchange, Queue, Producer
import logging
from amqp.exceptions import ConnectionForced
import gevent
from retrying import retry
from kirin import task_pb2, gtfs_realtime_pb2
from google.protobuf.message import DecodeError
import socket
from kirin.core.model import TripUpdate, db
from kirin.core.populate_pb import convert_to_gtfsrt
from kirin.utils import str_to_date, record_call
from datetime import datetime
from kombu.mixins import ConsumerProducerMixin


class RTReloader(ConsumerProducerMixin):
    """
    ConsumerProducerMixin: a RPC model
    """

    def __init__(self, connection, rpc_queue, exchange, max_retries):
        self.connection = connection
        self.rpc_queue = rpc_queue
        self.exchange = exchange
        self.max_retries = max_retries

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=[self.rpc_queue], on_message=self.on_request, prefetch_count=1)]

    def on_request(self, message):
        self._on_request(message)
        message.ack()

    def _on_request(self, message):
        log = logging.getLogger(__name__)
        try:
            task = task_pb2.Task()
            try:
                # `body` is of unicode type, but we need str type for
                # `ParseFromString()` to work.  It seems to work.
                # Maybe kombu estimate that, without any information,
                # the body should be something as json, and thus a
                # unicode string.  On the c++ side, I didn't manage to
                # find a way to give a content-type or something like
                # that.
                body = str(message.payload)
                task.ParseFromString(body)
            except DecodeError as e:
                log.warning("invalid protobuf: {}".format(six.text_type(e)))
                return

            log.info("Getting a full feed publication request", extra={"task": task})
            if task.action != task_pb2.LOAD_REALTIME or not task.load_realtime:
                return
            start_datetime = datetime.utcnow()
            begin_date = None
            end_date = None
            if hasattr(task.load_realtime, "begin_date"):
                if task.load_realtime.begin_date:
                    begin_date = str_to_date(task.load_realtime.begin_date)

            if hasattr(task.load_realtime, "end_date"):
                if task.load_realtime.end_date:
                    end_date = str_to_date(task.load_realtime.end_date)
            feed = convert_to_gtfsrt(
                TripUpdate.find_by_contributor_period(task.load_realtime.contributors, begin_date, end_date),
                gtfs_realtime_pb2.FeedHeader.FULL_DATASET,
            )

            feed_str = feed.SerializeToString()
            log.info(
                "Starting of full feed publication {}, {}".format(len(feed_str), task),
                extra={"size": len(feed_str), "task": task},
            )
            # http://docs.celeryproject.org/projects/kombu/en/latest/userguide/producers.html#bypassing-routing-by-using-the-anon-exchange
            self.producer.publish(
                feed_str,
                routing_key=task.load_realtime.queue_name,
                retry=True,
                retry_policy={
                    "interval_start": 0,  # First retry immediately,
                    "interval_step": 2,  # then increase by 2s for every retry.
                    "interval_max": 10,  # but don't exceed 10s between retries.
                    "max_retries": self.max_retries,  # give up after 10 (by default) tries.
                },
            )
            duration = (datetime.utcnow() - start_datetime).total_seconds()
            log.info("End of full feed publication", extra={"duration": duration, "task": task})
            record_call(
                "Full feed publication",
                size=len(feed_str),
                routing_key=task.load_realtime.queue_name,
                duration=duration,
                trip_update_count=len(feed.entity),
                contributor=task.load_realtime.contributors,
            )
        finally:
            db.session.remove()


class RabbitMQHandler(object):
    def __init__(self, connection_string, exchange_name, exchange_type="topic"):
        self._connection = BrokerConnection(connection_string)
        self._connections = {self._connection}  # set of connection for the heartbeat
        self._exchange = Exchange(exchange_name, durable=True, delivery_mode=2, type=exchange_type)
        monitor_heartbeats(self._connections)

    @retry(wait_fixed=200, stop_max_attempt_number=3)
    def publish(self, item, contributor_id):
        with self._connection.channel() as channel:
            with Producer(channel) as producer:
                producer.publish(
                    item, exchange=self._exchange, routing_key=contributor_id, declare=[self._exchange]
                )

    def info(self):
        info = self._connection.info()
        info.pop("password", None)
        return info

    def connect(self):
        self._connection.connect()

    def close(self):
        for c in self._connections:
            c.release()

    def listen_load_realtime(self, queue_name, max_retries=10):
        log = logging.getLogger(__name__)

        route = "task.load_realtime.*"
        log.info("listening route {} on exchange {}...".format(route, self._exchange))
        rt_queue = Queue(queue_name, routing_key=route, exchange=self._exchange, durable=False)
        RTReloader(
            connection=self._connection, rpc_queue=rt_queue, exchange=self._exchange, max_retries=max_retries
        ).run()


def monitor_heartbeats(connections, rate=2):
    """
    launch the heartbeat of amqp, it's mostly for preventing the f@#$ firewall from dropping the connection
    """
    supports_heartbeats = False
    interval = 10000
    for conn in connections:
        if conn.heartbeat and conn.supports_heartbeats:
            supports_heartbeats = True
            interval = min(conn.heartbeat / 2, interval)

    if not supports_heartbeats:
        logging.getLogger(__name__).info("heartbeat is not enabled")
        return

    logging.getLogger(__name__).info("start rabbitmq monitoring")

    def heartbeat_check():
        to_remove = []
        for conn in connections:
            logging.getLogger(__name__).debug("heartbeat_check for %s", conn)
            try:
                conn.heartbeat_check(rate=rate)
            except (socket.error, ConnectionForced):
                logging.getLogger(__name__).info("connection %s dead: removing it!", conn)
                # actualy we don't do a close(), else we won't be able to reopen it after...
                to_remove.append(conn)

        for conn in to_remove:
            connections.remove(conn)

    def loop():
        while True:
            try:
                gevent.sleep(interval)
                heartbeat_check()
            except Exception as e:
                logging.getLogger(__name__).exception("unknown exception when heartbeating%s", e)

    gevent.Greenlet.spawn(loop)
