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
        self.last_config_checked_time = datetime.now()
        self.broker_url = deepcopy(contributor.broker_url)
        self.builder = KirinModelBuilder(contributor)

    def __enter__(self):
        self.connection = Connection(self.builder.contributor.broker_url)
        self.exchange = self._get_exchange(self.builder.contributor.exchange_name)
        self.queue = self._get_queue(self.exchange, self.builder.contributor.queue_name)
        return self

    def __exit__(self, type, value, traceback):
        self.connection.release()

    def _get_exchange(self, exchange_name):
        return Exchange(exchange_name, "fanout", durable=True, no_declare=True, auto_delete=False)

    def _get_queue(self, exchange, queue_name):
        return Queue(queue_name, exchange, durable=True, auto_delete=False)

    def get_consumers(self, Consumer, channel):
        return [
            Consumer(
                queues=[self.queue],
                accept=["application/json"],
                prefetch_count=1,
                callbacks=[self.process_message],
            )
        ]

    def process_message(self, body, message):
        wrap_build(self.builder, body)
        # TODO: We might want to not acknowledge the message in case of error in
        # the processing.
        message.ack()

    def on_iteration(self):
        if datetime.now() - self.last_config_checked_time < CONF_RELOAD_INTERVAL:
            return
        else:
            # SQLAlchemy is not querying the DB for read (uses cache instead),
            # unless we specifically tell that the data is expired.
            db.session.expire(self.builder.contributor, ["broker_url", "exchange_name", "queue_name"])
            self.last_config_checked_time = datetime.now()
        contributor = get_piv_contributor(self.builder.contributor.id)
        if not contributor:
            logger.info(
                "contributor '{0}' doesn't exist anymore, let the worker die".format(self.builder.contributor.id)
            )
            self.should_stop = True
            return
        if contributor.broker_url != self.broker_url:
            logger.info("broker URL for contributor '{0}' changed, let the worker die".format(contributor.id))
            self.should_stop = True
            return
        if contributor.exchange_name != self.exchange.name:
            logger.info(
                "exchange name for contributor '{0}' changed, worker updated".format(contributor.exchange_name)
            )
            self.exchange = self._get_exchange(contributor.exchange_name)
            self.queue = self._get_queue(self.exchange, contributor.queue_name)
        if contributor.queue_name != self.queue.name:
            logger.info(
                "queue name for contributor '{0}' changed, worker updated".format(contributor.queue_name)
            )
            self.queue = self._get_queue(self.exchange, contributor.queue_name)
        self.builder = KirinModelBuilder(contributor)


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
