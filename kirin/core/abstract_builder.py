# coding=utf-8

# Copyright (c) 2001-2020, Canal TP and/or its affiliates. All rights reserved.
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

from __future__ import absolute_import, print_function, unicode_literals, division

import logging
from abc import ABCMeta
from datetime import datetime

import navitia_wrapper
import six
from flask import current_app

from kirin import core, redis_client
from kirin.core import model
from kirin.exceptions import KirinException
from kirin.new_relic import is_invalid_input_exception, record_custom_parameter
from kirin.utils import set_rtu_status_ko, allow_reprocess_same_data, record_call


def wrap_build(builder, input_raw):
    contributor = builder.contributor
    start_datetime = datetime.utcnow()
    rt_update = None
    log_dict = {"contributor": contributor.id}
    status = "OK"

    try:
        # create a raw rt_update obj, save the raw_input into the db
        rt_update, rtu_log_dict = builder.build_rt_update(input_raw)
        log_dict.update(rtu_log_dict)

        # raw_input is interpreted
        trip_updates, tu_log_dict = builder.build_trip_updates(rt_update)
        log_dict.update(tu_log_dict)

        # finally confront to previously existing information (base_schedule, previous real-time)
        _, handler_log_dict = core.handle(rt_update, trip_updates, contributor.id, builder.is_new_complete)
        log_dict.update(handler_log_dict)

    except Exception as e:
        status = "failure"
        allow_reprocess = True
        if is_invalid_input_exception(e):
            status = "warning"  # Kirin did his job correctly if the input is invalid and rejected
            allow_reprocess = False  # reprocess is useless if input is invalid

        if rt_update is not None:
            error = e.data["error"] if (isinstance(e, KirinException) and "error" in e.data) else e.message
            set_rtu_status_ko(rt_update, error, is_reprocess_same_data_allowed=allow_reprocess)
            model.db.session.add(rt_update)
            model.db.session.commit()
        else:
            # rt_update is not built, make sure reprocess is allowed
            allow_reprocess_same_data(contributor.id)

        log_dict.update({"exc_summary": six.text_type(e), "reason": e})

        record_custom_parameter("reason", e)  # using __str__() here to have complete details
        raise  # filters later for APM (auto.)

    finally:
        log_dict.update({"duration": (datetime.utcnow() - start_datetime).total_seconds()})
        record_call(status, **log_dict)
        if status == "OK":
            logging.getLogger(__name__).info(status, extra=log_dict)
        elif status == "warning":
            logging.getLogger(__name__).warning(status, extra=log_dict)
        else:
            logging.getLogger(__name__).error(status, extra=log_dict)


class AbstractKirinModelBuilder(six.with_metaclass(ABCMeta, object)):
    def __init__(self, contributor, is_new_complete):
        self.navitia = navitia_wrapper.Navitia(
            url=current_app.config.get(str("NAVITIA_URL")),
            token=contributor.navitia_token,
            timeout=current_app.config.get(str("NAVITIA_TIMEOUT"), 5),
            cache=redis_client,
            query_timeout=current_app.config.get(str("NAVITIA_QUERY_CACHE_TIMEOUT"), 600),
            pubdate_timeout=current_app.config.get(str("NAVITIA_PUBDATE_CACHE_TIMEOUT"), 600),
        ).instance(contributor.navitia_coverage)
        self.contributor = contributor
        self.is_new_complete = is_new_complete

    def build_rt_update(self, input_raw):
        pass

    def build_trip_updates(self, rt_update):
        pass
