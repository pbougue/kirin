#!/usr/bin/env python
# coding=utf-8

# Copyright (c) 2020, Canal TP and/or its affiliates. All rights reserved.
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
from kirin import manager, app
from kirin.core.model import Contributor, db
from kirin.core.types import ConnectorType
from kirin.core.abstract_builder import wrap_build
from kirin.piv import KirinModelBuilder
from kirin.piv.piv import get_piv_contributors, get_piv_contributor

from kombu.mixins import ConsumerMixin
from kombu import Connection, Exchange, Queue
from datetime import datetime, timedelta
from copy import deepcopy
import logging
import time

logger = logging.getLogger(__name__)

CONF_RELOAD_INTERVAL = timedelta(
    seconds=float(str(app.config.get("BROKER_CONSUMER_CONFIGURATION_RELOAD_INTERVAL")))
)


class PivWorker(ConsumerMixin):
    def __init__(self, contributor):
        print("Initializing PivWorker...")
        if contributor.connector_type != ConnectorType.piv.value:
            raise ValueError(
                "Contributor '{0}': PivWorker requires type {1}".format(contributor.id, ConnectorType.piv.value)
            )
        if not contributor.is_active:
            raise ValueError(
                "Contributor '{0}': PivWorker requires an activated contributor.".format(contributor.id)
            )
        if not contributor.broker_url:
            raise ValueError("Missing 'broker_url' configuration for contributor '{0}'".format(contributor.id))
        if not contributor.exchange_name:
            raise ValueError(
                "Missing 'exchange_name' configuration for contributor '{0}'".format(contributor.id)
            )
        if not contributor.queue_name:
            raise ValueError("Missing 'queue_name' configuration for contributor '{0}'".format(contributor.id))
        print("Initializing PivWorker... conditions checked")
        self.last_config_checked_time = datetime.now()
        self.broker_url = deepcopy(contributor.broker_url)
        self.builder = KirinModelBuilder(contributor)
        print("Initializing PivWorker... model built")

    def __enter__(self):
        print("Entering PivWorker...")
        self.connection = Connection(self.builder.contributor.broker_url)
        print("Entering PivWorker... Connection infos")
        print("{0}".format(self.connection.info()))
        print("Entering PivWorker... connection created")
        self.exchange = self._get_exchange(self.builder.contributor.exchange_name)
        print("Entering PivWorker... exchange created")
        self.queue = self._get_queue(self.exchange, self.builder.contributor.queue_name)
        print("Entering PivWorker... queue created")
        return self

    def __exit__(self, type, value, traceback):
        print("Exiting PivWorker...")
        self.connection.release()
        print("Exiting PivWorker... connection released")

    def _get_exchange(self, exchange_name):
        print("Get Exchange...")
        return Exchange(exchange_name, "fanout", durable=True, no_declare=True)

    def _get_queue(self, exchange, queue_name):
        print("Get Queue...")
        return Queue(queue_name, exchange, durable=True, auto_delete=False)

    def get_consumers(self, Consumer, channel):
        print("Get Consumers...")
        return [
            Consumer(
                queues=[self.queue],
                accept=["application/json"],
                prefetch_count=1,
                callbacks=[self.process_message],
            )
        ]

    def process_message(self, body, message):
        print("Processing message...")
        wrap_build(self.builder, body)
        print("Processing message... message processed")
        # TODO: We might want to not acknowledge the message in case of error in
        # the processing.
        message.ack()
        print("Processing message... message acknowledged")

    def on_iteration(self):
        print("Iterating...")
        if datetime.now() - self.last_config_checked_time < CONF_RELOAD_INTERVAL:
            print("Iterating... not iterating (not enough time since last iteration)")
            return
        else:
            # SQLAlchemy is not querying the DB for read (uses cache instead),
            # unless we specifically tell that the data is expired.
            print("Iterating... expiring DB data")
            print("Iterating... db = {0}".format(db))
            print("Iterating... db.session = {0}".format(db.session))
            db.session.expire(self.builder.contributor, ["broker_url", "exchange_name", "queue_name"])
            self.last_config_checked_time = datetime.now()
            print("Iterating... DB expired")
        contributor = get_piv_contributor(self.builder.contributor.id)
        print("Iterating... contributor updated")
        if not contributor:
            print("Iterating... killing the worker since contributor disappeared")
            logger.info(
                "contributor '{0}' doesn't exist anymore, let the worker die".format(self.builder.contributor.id)
            )
            self.should_stop = True
            return
        if contributor.broker_url != self.broker_url:
            print("Iterating... killing the worker since broker URL changed")
            logger.info("broker URL for contributor '{0}' changed, let the worker die".format(contributor.id))
            self.should_stop = True
            return
        if contributor.exchange_name != self.exchange.name:
            print("Iterating... exchange name changed")
            logger.info(
                "exchange name for contributor '{0}' changed, worker updated".format(contributor.exchange_name)
            )
            self.exchange = self._get_exchange(contributor.exchange_name)
            self.queue = self._get_queue(self.exchange, contributor.queue_name)
        if contributor.queue_name != self.queue.name:
            print("Iterating... queue name changed")
            logger.info(
                "queue name for contributor '{0}' changed, worker updated".format(contributor.queue_name)
            )
            self.queue = self._get_queue(self.exchange, contributor.queue_name)
        print("Iterating... updating model")
        self.builder = KirinModelBuilder(contributor)
        print("Iterating... model updated")


@manager.command
def piv_worker():
    import sys

    # We assume one and only one PIV contributor is going to exist in the DB
    while True:
        contributors = get_piv_contributors()
        if len(contributors) == 0:
            logger.warning("no PIV contributor")
            time.sleep(CONF_RELOAD_INTERVAL.total_seconds())
            continue
        contributor = contributors[0]
        if len(contributors) > 1:
            logger.warning(
                "more than one PIV contributors: {0}; choosing '{1}'".format(
                    map(lambda c: c.id, contributors), contributor.id
                )
            )
        try:
            with PivWorker(contributor) as worker:
                logger.info("launching the PIV worker for '{0}'".format(contributor.id))
                worker.run()
        except Exception as e:
            logger.warning("worker died unexpectedly: {0}".format(e))
            time.sleep(CONF_RELOAD_INTERVAL.total_seconds())
