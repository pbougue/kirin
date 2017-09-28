# Copyright (c) 2001-2015, Canal TP and/or its affiliates. All rights reserved.
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
# IRC #navitia on freenode
# https://groups.google.com/d/forum/navitia
# www.navitia.io
from kombu import BrokerConnection, Exchange, Queue, Consumer
from kombu.pools import producers, connections
import logging
from amqp.exceptions import ConnectionForced
import gevent
from retrying import retry
from kirin import task_pb2
from google.protobuf.message import DecodeError
import socket
from kirin.core.model import TripUpdate, db
from kirin.core.populate_pb import convert_to_gtfsrt
import gtfs_realtime_pb2
from kirin.utils import str_to_date
from socket import error
import time


class RabbitMQHandler(object):
    def __init__(self, connection_string, exchange):
        self._connection = BrokerConnection(connection_string)
        self._connections = set([self._connection])  # set of connection for the heartbeat
        self._exchange = Exchange(exchange, durable=True, delivry_mode=2, type='topic')
        monitor_heartbeats(self._connections)

    def _get_producer(self):
        producer = producers[self._connection].acquire(block=True, timeout=2)
        self._connections.add(producer.connection)
        return producer

    @retry(wait_fixed=200, stop_max_attempt_number=3)
    def publish(self, item, contributor):
        with self._get_producer() as producer:
            producer.publish(item, exchange=self._exchange, routing_key=contributor, declare=[self._exchange])

    def info(self):
        with self._get_producer() as producer:
            if not producer.connection.connected:
                return {}
            res = producer.connection.info()
            if 'password' in res:
                del res['password']
            return res

    def listen_load_realtime(self, queue_name, retry_timeout=10):
        log = logging.getLogger(__name__)

        def callback(body, message):
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
                    body = str(body)
                    task.ParseFromString(body)
                except DecodeError as e:
                    log.warn('invalid protobuf: {}'.format(str(e)))
                    return

                log.info('Getting a full feed publication request', extra={'task': task})
                if task.action != task_pb2.LOAD_REALTIME or not task.load_realtime:
                    return
                begin_date = None
                end_date = None
                if hasattr(task.load_realtime, "begin_date"):
                    if task.load_realtime.begin_date:
                        begin_date = str_to_date(task.load_realtime.begin_date)

                if hasattr(task.load_realtime, "end_date"):
                    if task.load_realtime.end_date:
                        end_date = str_to_date(task.load_realtime.end_date)
                feed = convert_to_gtfsrt(TripUpdate.find_by_contributor_period(task.load_realtime.contributors,
                                                                               begin_date,
                                                                               end_date),
                                         gtfs_realtime_pb2.FeedHeader.FULL_DATASET)

                with self._get_producer() as producer:
                    feed_str = feed.SerializeToString()
                    log.info('Starting of full feed publication'.format(len(feed_str), task), extra={'size': len(feed_str), 'task': task})
                    producer.publish(feed_str, routing_key=task.load_realtime.queue_name)
                    log.info('End of full feed publication'.format(task), extra={'task': task})
            finally:
                db.session.remove()

        route = 'task.load_realtime.*'
        log.info('listening route {} on exchange {}...'.format(route, self._exchange))
        rt_queue = Queue(queue_name, routing_key=route, exchange=self._exchange, durable=False)
        while True:
            try:
                with connections[self._connection].acquire(block=True) as conn:
                    self._connections.add(conn)
                    with Consumer(conn, no_ack=True, queues=[rt_queue], callbacks=[callback]):
                        while True:
                            try:
                                conn.drain_events(timeout=1)
                            except socket.timeout:
                                pass
            except socket.error:
                log.exception('disconnected, retrying in %s sec', retry_timeout)
                time.sleep(retry_timeout)


def monitor_heartbeats(connections, rate=2):
    """
    launch the heartbeat of amqp, it's mostly for prevent the f@#$ firewall from dropping the connection
    """
    supports_heartbeats = False
    interval = 10000
    for conn in connections:
        if conn.heartbeat and conn.supports_heartbeats:
            supports_heartbeats = True
            interval = min(conn.heartbeat / 4, interval)

    if not supports_heartbeats:
        logging.getLogger(__name__).info('heartbeat is not enabled')
        return

    logging.getLogger(__name__).info('start rabbitmq monitoring')

    def heartbeat_check():
        to_remove = []
        for conn in connections:
            if conn.connected:
                logging.getLogger(__name__).debug('heartbeat_check for %s', conn)
                try:
                    conn.heartbeat_check(rate=rate)
                except socket.error:
                    logging.getLogger(__name__).info('connection %s dead: closing it!', conn)
                    #actualy we don't do a close(), else we won't be able to reopen it after...
                    to_remove.append(conn)
            else:
                to_remove.append(conn)
        for conn in to_remove:
            connections.remove(conn)
        gevent.spawn_later(interval, heartbeat_check)

    gevent.spawn_later(interval, heartbeat_check)
