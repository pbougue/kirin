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

from kirin import core

# For perf benches:
# https://artem.krylysov.com/blog/2015/09/29/benchmark-python-json-libraries/
import ujson

from kirin.exceptions import InvalidArguments
from kirin.utils import make_rt_update


class KirinModelBuilder(object):
    def build(self, contributor, input):
        # create a raw rt_update obj, save the raw_input into the db
        rt_update = make_rt_update(input, connector=contributor.connector_type, contributor=contributor.id)
        # assuming UTF-8 encoding for all input
        rt_update.raw_data = rt_update.raw_data.encode("utf-8")

        # raw_input is interpreted
        trip_updates = self.build_trip_updates(rt_update)
        _, handler_log_dict = core.handle(rt_update, trip_updates, contributor.id, is_new_complete=True)
        handler_log_dict = {}
        return handler_log_dict

    def build_trip_updates(self, rt_update):
        """
        parse the PIV raw json stored in the rt_update object (in Kirin db)
        and return a list of trip updates

        The TripUpdates are not yet associated with the RealTimeUpdate
        """
        try:
            json = ujson.loads(rt_update.raw_data)
        except ValueError as e:
            raise InvalidArguments("invalid json: {}".format(e.message))

        # TODO: build trip_update from PIV feed
        trip_updates = []

        return trip_updates
