# coding=utf-8
# Copyright (c) 2001-2018, Canal TP and/or its affiliates. All rights reserved.
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
from datetime import timedelta

import jmespath

from kirin.utils import record_internal_failure, to_navitia_utc_str
from kirin.exceptions import ObjectNotFound, InvalidArguments, InternalException
from abc import ABCMeta
import six
from kirin.core import model
from enum import Enum

TRAIN_ID_FORMAT = "OCE:SN:{}"
SNCF_SEARCH_MARGIN = timedelta(hours=1)


class TripStatus(Enum):
    AJOUTEE = 1  # Added
    SUPPRIMEE = 2  # Deleted
    PERTURBEE = 3  # Impacted or Reactivated


class ActionOnTrip(Enum):
    NOT_ADDED = 1
    FIRST_TIME_ADDED = 2  # Add trip for the first time / delete followed by add
    PREVIOUSLY_ADDED = 3  # add followed by update


def make_navitia_empty_vj(headsign):
    headsign = TRAIN_ID_FORMAT.format(headsign)
    return {"id": headsign, "trip": {"id": headsign}}


def headsigns(str_headsign):
    """
    we remove leading 0 for the headsigns and handle the train's parity.
    The parity is the number after the '/', it gives an alternative train number.

    >>> headsigns('2038')
    [u'2038']
    >>> headsigns('002038')
    [u'2038']
    >>> headsigns('002038/12')
    [u'2038', u'2012']
    >>> headsigns('2038/3')
    [u'2038', u'2033']
    >>> headsigns('2038/123')
    [u'2038', u'2123']
    >>> headsigns('2038/12345')
    [u'2038', u'12345']

    """
    h = str_headsign.lstrip("0")
    if "/" not in h:
        return [h]
    signs = h.split("/", 1)
    alternative_headsign = signs[0][: -len(signs[1])] + signs[1]
    return [signs[0], alternative_headsign]


def get_navitia_stop_time_sncf(cr, ci, ch, nav_vj):
    nav_external_code = "{cr}-{ci}-{ch}".format(cr=cr, ci=ci, ch=ch)

    nav_stop_times = jmespath.search(
        "stop_times[? stop_point.stop_area.codes[? value==`{nav_ext_code}` && type==`CR-CI-CH`]]".format(
            nav_ext_code=nav_external_code
        ),
        nav_vj,
    )

    log_dict = None
    if not nav_stop_times:
        log_dict = {"log": "missing stop point", "stop_point_code": nav_external_code}
        return None, log_dict

    if len(nav_stop_times) > 1:
        log_dict = {"log": "duplicate stops", "stop_point_code": nav_external_code}

    return nav_stop_times[0], log_dict


class AbstractSNCFKirinModelBuilder(six.with_metaclass(ABCMeta, object)):
    def __init__(self, nav, contributor):
        self.navitia = nav
        self.contributor = contributor

    def _get_navitia_vjs(self, headsign_str, since_dt, until_dt, action_on_trip=ActionOnTrip.NOT_ADDED.name):
        """
        Search for navitia's vehicle journeys with given headsigns, in the period provided
        :param headsign_str: the headsigns to search for (can be multiple expressed in one string, like "2512/3")
        :param since_dt: naive UTC datetime that starts the search period.
            Typically the supposed datetime of first base-schedule stop_time.
        :param until_dt: naive UTC datetime that ends the search period.
            Typically the supposed datetime of last base-schedule stop_time.
        :param action_on_trip: action to be performed on trip. This param is used to do consistency check
        """
        log = logging.getLogger(__name__)

        if (since_dt is None) or (until_dt is None):
            return []

        if since_dt.tzinfo is not None or until_dt.tzinfo is not None:
            raise InternalException("Invalid datetime provided: must be naive (and UTC)")

        vjs = {}
        # to get the date of the vj we use the start/end of the vj + some tolerance
        # since the SNCF data and navitia data might not be synchronized
        extended_since_dt = since_dt - SNCF_SEARCH_MARGIN
        extended_until_dt = until_dt + SNCF_SEARCH_MARGIN

        # using a set to deduplicate
        # one headsign_str (ex: "96320/1") can lead to multiple headsigns (ex: ["96320", "96321"])
        # but most of the time (if not always) they refer to the same VJ
        # (the VJ switches headsign along the way).
        # So we do one VJ search for each headsign to ensure we get it, then deduplicate VJs
        for train_number in headsigns(headsign_str):

            log.debug(
                "searching for vj {} during period [{} - {}] in navitia".format(
                    train_number, extended_since_dt, extended_until_dt
                )
            )

            navitia_vjs = self.navitia.vehicle_journeys(
                q={
                    "headsign": train_number,
                    "since": to_navitia_utc_str(extended_since_dt),
                    "until": to_navitia_utc_str(extended_until_dt),
                    "depth": "2",  # we need this depth to get the stoptime's stop_area
                    "show_codes": "true",  # we need the stop_points CRCICH codes
                }
            )

            # Consistency check on action applied to trip
            if action_on_trip == ActionOnTrip.NOT_ADDED.name:
                if not navitia_vjs:
                    logging.getLogger(__name__).info(
                        "impossible to find train {t} on [{s}, {u}[".format(
                            t=train_number, s=extended_since_dt, u=extended_until_dt
                        )
                    )
                    record_internal_failure("missing train", contributor=self.contributor)

            else:
                if action_on_trip == ActionOnTrip.FIRST_TIME_ADDED.name and navitia_vjs:
                    raise InvalidArguments(
                        "Invalid action, trip {} already present in navitia".format(train_number)
                    )

                navitia_vjs = [make_navitia_empty_vj(train_number)]

            for nav_vj in navitia_vjs:

                try:
                    vj = model.VehicleJourney(nav_vj, extended_since_dt, extended_until_dt, vj_start_dt=since_dt)
                    vjs[nav_vj["id"]] = vj
                except Exception as e:
                    logging.getLogger(__name__).exception(
                        "Error while creating kirin VJ of {}: {}".format(nav_vj.get("id"), e)
                    )
                    record_internal_failure("Error while creating kirin VJ", contributor=self.contributor)

        if not vjs:
            raise ObjectNotFound("no train found for headsign(s) {}".format(headsign_str))

        return vjs.values()
