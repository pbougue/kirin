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
# [matrix] channel #navitia:matrix.org (https://app.element.io/#/room/#navitia:matrix.org)
# https://groups.google.com/d/forum/navitia
# www.navitia.io

from __future__ import absolute_import, print_function, unicode_literals, division

import logging
from datetime import datetime
from operator import itemgetter

import jmespath
from dateutil import parser
from flask.globals import current_app
from pytz import utc

# For perf benches:
# https://artem.krylysov.com/blog/2015/09/29/benchmark-python-json-libraries/
import ujson

from kirin.core import model
from kirin.core.abstract_builder import AbstractKirinModelBuilder
from kirin.cots.message_handler import MessageHandler
from kirin.exceptions import InvalidArguments, InternalException, ObjectNotFound
from kirin.utils import record_internal_failure, make_rt_update, to_navitia_utc_str
from kirin.core.types import (
    TripEffect,
    ModificationType,
    get_higher_status,
    get_effect_by_stop_time_status,
    get_mode_filter,
)
from enum import Enum
from datetime import timedelta

DEFAULT_COMPANY_ID = "1187"
TRAIN_ID_FORMAT = "OCE:SN:{}"
SNCF_SEARCH_MARGIN = timedelta(hours=1)


class TripStatus(Enum):
    AJOUTEE = 1  # Added
    SUPPRIMEE = 2  # Deleted
    PERTURBEE = 3  # Modified, Impacted or Reactivated


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


def get_value(sub_json, key, nullable=False):
    """
    get a unique element in an json dict
    raise an exception if the element does not exists
    """
    res = sub_json.get(key)
    if res is None and not nullable:
        raise InvalidArguments(
            'invalid json, impossible to find "{key}" in json dict {elt}'.format(
                key=key, elt=ujson.dumps(sub_json)
            )
        )
    return res


def is_station(pdp):
    """
    determine if a Point de Parcours is a legit station
    :param pdp: stop_time to be checked
    :return: True if pdp is a station, False otherwise
    """
    t = get_value(pdp, "typeArret", nullable=True)
    return (t is None) or (t in ["", "CD", "CH", "FD", "FH"])


def _retrieve_interesting_pdp(list_pdp):
    """
    Filter and sort "Points de Parcours" (corresponding to stop_times in Navitia) to get only the relevant
    ones from a Navitia's perspective (stations, where travelers can hop on or hop off)
    Context: COTS may contain operating informations, useless for traveler
    :param list_pdp: an array of "Point de Parcours" (typically the one from the feed)
    :return: Filtered array
    Notes:  - written in a yield-fashion to switch implementation if possible, but we need random access for now
            - see 'test_retrieve_interesting_pdp' for a functional example
    """
    res = []
    picked_one = False
    sorted_list_pdp = sorted(list_pdp, key=itemgetter("rang"))
    for idx, pdp in enumerate(sorted_list_pdp):
        # At start, do not consume until there's a departure time (horaireVoyageurDepart)
        if not picked_one and not get_value(pdp, "horaireVoyageurDepart", nullable=True):
            continue
        # exclude stop_times that are not legit stations
        if not is_station(pdp):
            continue
        # exclude stop_times that have no departure nor arrival time (empty stop_times)
        if not get_value(pdp, "horaireVoyageurDepart", nullable=True) and not get_value(
            pdp, "horaireVoyageurArrivee", nullable=True
        ):
            continue
        # stop consuming once all following stop_times are missing arrival time
        # * if a stop_time only has departure time, travelers can only hop in, but if they are be able to
        #   hop off later because some stop_time has arrival time then the current stop_time is useful,
        #   so we keep current stop_time.
        # * if no stop_time has arrival time anymore, then stop_times are useless as traveler cannot
        #   hop off, so no point hopping in anymore, so we remove all the stop_times until the end
        #   (should not happen in practice).
        if not get_value(pdp, "horaireVoyageurArrivee", nullable=True):
            has_following_arrival = any(
                get_value(follow_pdp, "horaireVoyageurArrivee", nullable=True) and is_station(follow_pdp)
                for follow_pdp in sorted_list_pdp[idx:]
            )
            if not has_following_arrival:
                break

        picked_one = True
        res.append(pdp)
    return res


def as_utc_naive_dt(str_time):
    try:
        return (
            parser.parse(str_time, dayfirst=False, yearfirst=True, ignoretz=False)
            .astimezone(utc)
            .replace(tzinfo=None)
        )
    except Exception as e:
        raise InvalidArguments(
            'Impossible to parse timezoned datetime from "{s}": {m}'.format(s=str_time, m=e.message)
        )


def as_duration(seconds):
    """
    transform a number of seconds into a timedelta
    >>> as_duration(None)

    >>> as_duration(900)
    datetime.timedelta(0, 900)
    >>> as_duration(-400)
    datetime.timedelta(-1, 86000)
    >>> as_duration("bob")
    Traceback (most recent call last):
    TypeError: a float is required
    """
    if seconds is None:
        return None
    return datetime.utcfromtimestamp(seconds) - datetime.utcfromtimestamp(0)


def _retrieve_stop_event_datetime(cots_traveler_time):
    base_schedule_datetime = get_value(cots_traveler_time, "dateHeure", nullable=True)
    return as_utc_naive_dt(base_schedule_datetime) if base_schedule_datetime else None


def _retrieve_projected_time(source_ref, list_proj_time):
    """
    pick the good projected arrival/departure objects from the list provided,
    using the source-reference if existing
    """
    # if a source-reference is defined
    if source_ref is not None:
        # retrieve the one mentioned if it exists
        if list_proj_time:
            for p in list_proj_time:
                s = get_value(p, "source", nullable=True)
                if s is not None and s == source_ref:
                    return p

        # if a reference is provided but impossible to retrieve corresponding element, reject whole COTS feed
        raise InvalidArguments(
            'invalid json, impossible to find source "{s}" in any json dict '
            "of list: {list}".format(s=source_ref, list=ujson.dumps(list_proj_time))
        )

    elif list_proj_time:
        # if no source-reference exists, but only one element in the list, we take it
        if len(list_proj_time) == 1:
            return list_proj_time[0]
        # if list has multiple elements but no source-reference, reject whole COTS feed
        raise InvalidArguments(
            "invalid json, impossible no source but multiple json dicts "
            "in list: {list}".format(list=ujson.dumps(list_proj_time))
        )

    return None


def _retrieve_stop_event_delay(pdp, arrival_departure_toggle):
    cots_ref_planned = get_value(
        pdp, "sourceHoraireProjete{}Reference".format(arrival_departure_toggle), nullable=True
    )
    cots_planned_stop_times = get_value(
        pdp, "listeHoraireProjete{}".format(arrival_departure_toggle), nullable=True
    )
    cots_planned_stop_time = _retrieve_projected_time(cots_ref_planned, cots_planned_stop_times)

    if cots_planned_stop_time is not None:
        delay = get_value(cots_planned_stop_time, "pronosticIV", nullable=True)
        return as_duration(delay)

    return None


def _is_fully_added_pdp(pdp):
    """
    Check if a projected arrival/departure object is fully created
    """
    # retrieve expressed statuses
    dep_arr_statuses = []
    for arrival_departure_toggle in ["Arrivee", "Depart"]:
        cots_traveler_time = get_value(pdp, "horaireVoyageur{}".format(arrival_departure_toggle), nullable=True)
        if cots_traveler_time:
            dep_arr_statuses.append(get_value(cots_traveler_time, "statutCirculationOPE", nullable=True))

    # if there are expressed cots_traveler_times and all are 'CREATION', pdp is fully added
    return dep_arr_statuses and all(s == "CREATION" for s in dep_arr_statuses)


def _get_first_stop_datetime(list_pdps, hour_obj_name, skip_fully_added_stops=True):
    if skip_fully_added_stops:
        p = next((p for p in list_pdps if not _is_fully_added_pdp(p)), None)
    else:
        p = next((p for p in list_pdps), None)

    str_time = get_value(get_value(p, hour_obj_name), "dateHeure") if p else None
    return as_utc_naive_dt(str_time) if str_time else None


def _get_action_on_trip(train_numbers, dict_version, pdps):
    """
    Verify if trip in cots feed is a newly added one and check possible actions
    :param dict_version: Value of attribut nouvelleVersion in cots feed
    :return: NOT_ADDED if the trip is not added by current feed,
        FIRST_TIME_ADDED if it's the first time this trip is added,
        PREVIOUSLY_ADDED if this train is already added (present in db)
    """
    cots_trip_status = get_value(dict_version, "statutOperationnel", TripStatus.PERTURBEE.name)

    # We have to verify if the trip exists in database
    vj_start = _get_first_stop_datetime(pdps, "horaireVoyageurDepart", skip_fully_added_stops=False)
    vj_end = _get_first_stop_datetime(reversed(pdps), "horaireVoyageurArrivee", skip_fully_added_stops=False)
    train_id = TRAIN_ID_FORMAT.format(train_numbers)
    trip_added_in_db = model.TripUpdate.find_vj_by_period(
        train_id, start_date=vj_start - SNCF_SEARCH_MARGIN, end_date=vj_end + SNCF_SEARCH_MARGIN
    )

    action_on_trip = ActionOnTrip.NOT_ADDED.name
    if trip_added_in_db:
        # Raise exception on forbidden / inconsistent actions
        # No addition multiple times
        # No update or delete on trip already deleted.
        if cots_trip_status == TripStatus.AJOUTEE.name and trip_added_in_db.status == ModificationType.add.name:
            raise InvalidArguments(
                "Invalid action, trip {} can not be added multiple times".format(train_numbers)
            )
        elif (
            cots_trip_status != TripStatus.AJOUTEE.name
            and trip_added_in_db.status == ModificationType.delete.name
        ):
            raise InvalidArguments("Invalid action, trip {} already deleted in database".format(train_numbers))

        # Trip added then deleted followed by add should be handled as FIRST_TIME_ADDED
        if (
            cots_trip_status == TripStatus.AJOUTEE.name
            and trip_added_in_db.status == ModificationType.delete.name
        ):
            action_on_trip = ActionOnTrip.FIRST_TIME_ADDED.name
        # Trip already added should be handled as PREVIOUSLY_ADDED
        elif trip_added_in_db.status == ModificationType.add.name:
            action_on_trip = ActionOnTrip.PREVIOUSLY_ADDED.name

    else:
        if cots_trip_status == TripStatus.AJOUTEE.name:
            action_on_trip = ActionOnTrip.FIRST_TIME_ADDED.name
    return action_on_trip


class KirinModelBuilder(AbstractKirinModelBuilder):
    def __init__(self, contributor):
        super(KirinModelBuilder, self).__init__(contributor, is_new_complete=True)
        self.message_handler = MessageHandler(
            api_key=current_app.config[str("COTS_PAR_IV_API_KEY")],
            resource_server=current_app.config[str("COTS_PAR_IV_MOTIF_RESOURCE_SERVER")],
            token_server=current_app.config[str("COTS_PAR_IV_TOKEN_SERVER")],
            client_id=current_app.config[str("COTS_PAR_IV_CLIENT_ID")],
            client_secret=current_app.config[str("COTS_PAR_IV_CLIENT_SECRET")],
            grant_type=current_app.config[str("COTS_PAR_IV_GRANT_TYPE")],
            timeout=current_app.config[str("COTS_PAR_IV_REQUEST_TIMEOUT")],
        )

    def build_rt_update(self, input_raw):
        rt_update = make_rt_update(
            input_raw, connector=self.contributor.connector_type, contributor=self.contributor.id
        )
        log_dict = {}
        return rt_update, log_dict

    def build_trip_updates(self, rt_update):
        """
        parse the COTS raw json stored in the rt_update object (in Kirin db)
        and return a list of trip updates

        The TripUpdates are not yet associated with the RealTimeUpdate

        Most of the realtime information parsed is contained in the 'nouvelleVersion' sub-object
        (see fixtures and documentation)
        """
        # assuming UTF-8 encoding for all input
        rt_update.raw_data = rt_update.raw_data.encode("utf-8")

        try:
            json = ujson.loads(rt_update.raw_data)
        except ValueError as e:
            raise InvalidArguments("invalid json: {}".format(e.message))

        if "nouvelleVersion" not in json:
            raise InvalidArguments('No object "nouvelleVersion" available in feed provided')

        dict_version = get_value(json, "nouvelleVersion")
        train_numbers = get_value(dict_version, "numeroCourse")
        pdps = _retrieve_interesting_pdp(get_value(dict_version, "listePointDeParcours"))
        if not pdps:
            raise InvalidArguments(
                'invalid json, "listePointDeParcours" has no valid stop_time in '
                "json elt {elt}".format(elt=ujson.dumps(dict_version))
            )

        action_on_trip = _get_action_on_trip(train_numbers, dict_version, pdps)
        vjs = self._get_vjs(train_numbers, pdps, action_on_trip=action_on_trip)
        trip_updates = [self._make_trip_update(dict_version, vj, action_on_trip=action_on_trip) for vj in vjs]

        log_dict = {}
        return trip_updates, log_dict

    def _get_vjs(self, train_numbers, pdps, action_on_trip=ActionOnTrip.NOT_ADDED.name):
        vj_start = _get_first_stop_datetime(
            pdps, "horaireVoyageurDepart", skip_fully_added_stops=(action_on_trip == ActionOnTrip.NOT_ADDED.name)
        )
        vj_end = _get_first_stop_datetime(
            reversed(pdps),
            "horaireVoyageurArrivee",
            skip_fully_added_stops=(action_on_trip == ActionOnTrip.NOT_ADDED.name),
        )

        return self._get_navitia_vjs(train_numbers, vj_start, vj_end, action_on_trip=action_on_trip)

    @staticmethod
    def _check_stop_time_consistency(last_stop_time_depart, projected_stop_time, pdp_code):
        last_stop_time_depart = (
            last_stop_time_depart if last_stop_time_depart is not None else datetime.fromtimestamp(0)
        )

        projected_arrival = projected_stop_time.get("Arrivee")
        projected_arrival = projected_arrival if projected_arrival is not None else last_stop_time_depart

        projected_departure = projected_stop_time.get("Depart")
        projected_departure = projected_departure if projected_departure is not None else projected_arrival

        if not (projected_departure >= projected_arrival >= last_stop_time_depart):
            raise InvalidArguments("invalid cots: stop_point's({}) time is not consistent".format(pdp_code))

    def _make_trip_update(self, json_train, vj, action_on_trip=ActionOnTrip.NOT_ADDED.name):
        """
        create the new TripUpdate object
        Following the COTS spec: https://github.com/CanalTP/kirin/blob/master/documentation/cots_connector.md
        """
        trip_update = model.TripUpdate(vj=vj, contributor=self.contributor.id)
        trip_message_id = get_value(json_train, "idMotifInterneReference", nullable=True)
        if trip_message_id:
            trip_update.message = self.message_handler.get_message(index=trip_message_id)
        cots_company_id = get_value(json_train, "codeCompagnieTransporteur", nullable=True) or DEFAULT_COMPANY_ID
        trip_update.company_id = self._get_navitia_company(cots_company_id)

        trip_status = get_value(json_train, "statutOperationnel")

        if trip_status == TripStatus.SUPPRIMEE.name:
            # the whole trip is deleted
            trip_update.status = ModificationType.delete.name
            trip_update.stop_time_updates = []
            trip_update.effect = TripEffect.NO_SERVICE.name
            return trip_update

        elif action_on_trip != ActionOnTrip.NOT_ADDED.name:
            # the trip is created from scratch
            trip_update.effect = TripEffect.ADDITIONAL_SERVICE.name
            trip_update.status = ModificationType.add.name
            cots_physical_mode = get_value(json_train, "indicateurFer", nullable=True)
            trip_update.physical_mode_id = self._get_navitia_physical_mode(cots_physical_mode)
            trip_update.headsign = get_value(json_train, "numeroCourse", nullable=True)

        # all other status is considered an 'update' of the trip_update and effect is calculated
        # from stop_time status list. This part is also done in kraken and is to be deleted later
        # Ordered stop_time status= 'nochange', 'add', 'delete', 'update'
        # 'nochange' or 'update' -> SIGNIFICANT_DELAYS, add -> MODIFIED_SERVICE, delete = DETOUR
        else:
            trip_update.status = ModificationType.update.name
            trip_update.effect = TripEffect.MODIFIED_SERVICE.name

        # Initialize stop_time status to nochange
        highest_st_status = ModificationType.none.name
        pdps = _retrieve_interesting_pdp(get_value(json_train, "listePointDeParcours"))

        # this variable is used to memoize the last stop_time's departure in order to check the stop_time consistency
        # ex. stop_time[i].arrival/departure must be greater than stop_time[i-1].departure
        last_stop_time_depart = None

        # manage realtime information stop_time by stop_time
        for pdp in pdps:
            # retrieve navitia's stop_point corresponding to the current COTS pdp
            nav_stop, log_dict = self._get_navitia_stop_point(pdp, vj.navitia_vj)
            projected_stop_time = {"Arrivee": None, "Depart": None}  # used to check consistency

            if log_dict:
                record_internal_failure(log_dict["log"], contributor=self.contributor.id)
                log_dict.update({"contributor": self.contributor.id})
                logging.getLogger(__name__).info("metrology", extra=log_dict)

            if nav_stop is None:
                continue

            st_update = model.StopTimeUpdate(nav_stop)
            trip_update.stop_time_updates.append(st_update)
            # using the message from departure-time in priority, if absent fallback on arrival-time's message
            st_message_id = get_value(pdp, "idMotifInterneDepartReference", nullable=True)
            if not st_message_id:
                st_message_id = get_value(pdp, "idMotifInterneArriveeReference", nullable=True)
            if st_message_id:
                st_update.message = self.message_handler.get_message(index=st_message_id)

            _status_map = {"Arrivee": "arrival_status", "Depart": "departure_status"}
            _delay_map = {"Arrivee": "arrival_delay", "Depart": "departure_delay"}
            _stop_event_datetime_map = {"Arrivee": "arrival", "Depart": "departure"}

            # compute realtime information and fill st_update for arrival and departure
            for arrival_departure_toggle in ["Arrivee", "Depart"]:
                cots_traveler_time = get_value(
                    pdp, "horaireVoyageur{}".format(arrival_departure_toggle), nullable=True
                )

                if cots_traveler_time is None:
                    continue

                cots_stop_time_status = get_value(cots_traveler_time, "statutCirculationOPE", nullable=True)

                if cots_stop_time_status is None or cots_stop_time_status == "REACTIVATION":
                    # if no cots_stop_time_status, it is considered an 'update' of the stop_time
                    # (can be a delay, back to normal, normal, reactivation...)
                    cots_base_datetime = _retrieve_stop_event_datetime(cots_traveler_time)
                    if cots_base_datetime:
                        projected_stop_time[arrival_departure_toggle] = cots_base_datetime
                    cots_delay = _retrieve_stop_event_delay(pdp, arrival_departure_toggle)

                    if cots_delay is not None:
                        # It's an update only if there is delay
                        projected_stop_time[arrival_departure_toggle] += cots_delay
                        setattr(st_update, _status_map[arrival_departure_toggle], ModificationType.update.name)
                        setattr(st_update, _delay_map[arrival_departure_toggle], cots_delay)
                    # otherwise nothing to do (status none, delay none, time none)

                elif cots_stop_time_status == "SUPPRESSION":
                    # partial delete
                    setattr(st_update, _status_map[arrival_departure_toggle], ModificationType.delete.name)

                elif cots_stop_time_status == "SUPPRESSION_DETOURNEMENT":
                    # stop_time is replaced by another one
                    setattr(
                        st_update,
                        _status_map[arrival_departure_toggle],
                        ModificationType.deleted_for_detour.name,
                    )

                elif cots_stop_time_status in ["CREATION", "DETOURNEMENT"]:
                    # new stop_time added
                    cots_base_datetime = _retrieve_stop_event_datetime(cots_traveler_time)
                    if cots_base_datetime:
                        projected_stop_time[arrival_departure_toggle] = cots_base_datetime
                    cots_delay = _retrieve_stop_event_delay(pdp, arrival_departure_toggle)
                    if cots_delay is not None:
                        projected_stop_time[arrival_departure_toggle] += cots_delay

                    setattr(
                        st_update,
                        _stop_event_datetime_map[arrival_departure_toggle],
                        projected_stop_time[arrival_departure_toggle],
                    )
                    # delay already added to stop_event datetime
                    setattr(st_update, _delay_map[arrival_departure_toggle], None)

                    if cots_stop_time_status == "CREATION":
                        # pure add
                        setattr(st_update, _status_map[arrival_departure_toggle], ModificationType.add.name)
                    elif cots_stop_time_status == "DETOURNEMENT":
                        # add to replace another stop_time
                        setattr(
                            st_update,
                            _status_map[arrival_departure_toggle],
                            ModificationType.added_for_detour.name,
                        )

                else:
                    raise InvalidArguments(
                        "invalid value {} for field horaireVoyageur{}/statutCirculationOPE".format(
                            cots_stop_time_status, arrival_departure_toggle
                        )
                    )

                arr_dep_status = getattr(
                    st_update, _status_map[arrival_departure_toggle], ModificationType.none.name
                )
                highest_st_status = get_higher_status(highest_st_status, arr_dep_status)

            self._check_stop_time_consistency(
                last_stop_time_depart,
                projected_stop_time,
                pdp_code="-".join(pdp[key] for key in ["cr", "ci", "ch"]),
            )
            last_stop_time_depart = projected_stop_time["Depart"]

        # Calculates effect from stop_time status list(this work is also done in kraken and has to be deleted)
        if trip_update.effect == TripEffect.MODIFIED_SERVICE.name:
            trip_update.effect = get_effect_by_stop_time_status(highest_st_status)
        return trip_update

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
        log = logging.LoggerAdapter(logging.getLogger(__name__), extra={"contributor": self.contributor.id})

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
                    log.info(
                        "impossible to find train {t} on [{s}, {u}[".format(
                            t=train_number, s=extended_since_dt, u=extended_until_dt
                        )
                    )
                    record_internal_failure("missing train", contributor=self.contributor.id)

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
                    log.exception("Error while creating kirin VJ of {}: {}".format(nav_vj.get("id"), e))
                    record_internal_failure("Error while creating kirin VJ", contributor=self.contributor.id)

        if not vjs:
            raise ObjectNotFound("no train found for headsign(s) {}".format(headsign_str))

        return vjs.values()

    def _get_navitia_stop_point(self, pdp, nav_vj):
        """
        Get a navitia stop point from the stop_time in a 'Point de Parcours' dict.
        The dict MUST contain cr, ci, ch tags.
        It searches in the vj's stops for a stop_area with the external code cr-ci-ch

        If the stop_time isn't found in the vj, in case of an additional stop_time,
        a request is made to Navitia.

        Error messages are also returned as 'missing stop point', 'duplicate stops'
        """
        nav_st, log_dict = get_navitia_stop_time_sncf(
            cr=get_value(pdp, "cr"), ci=get_value(pdp, "ci"), ch=get_value(pdp, "ch"), nav_vj=nav_vj
        )
        if not nav_st:
            nav_stop, log_dict = self._request_navitia_stop_point(
                cr=get_value(pdp, "cr"), ci=get_value(pdp, "ci"), ch=get_value(pdp, "ch")
            )
        else:
            nav_stop = nav_st.get("stop_point", None)
        return nav_stop, log_dict

    def _request_navitia_stop_point(self, cr, ci, ch):
        external_code = "{}-{}-{}".format(cr, ci, ch)
        stop_points = self.navitia.stop_points(
            q={"filter": 'stop_area.has_code("CR-CI-CH", "{}")'.format(external_code), "count": "1"}
        )
        if stop_points:
            return stop_points[0], None

        return None, {"log": "No stop point found", "stop_point_code": external_code}

    def _get_navitia_company(self, code):
        """
        Get a navitia company for the code present in COTS
        If the company doesn't exist in navitia, another request is made to
        find company for key="RefProd" and value="1187"
        """
        return self._request_navitia_company(code) or self._request_navitia_company(DEFAULT_COMPANY_ID)

    def _request_navitia_company(self, code):
        companies = self.navitia.companies(
            q={"filter": 'company.has_code("RefProd", "{}")'.format(code), "count": "1"}
        )
        if companies:
            return companies[0].get("id", None)
        return None

    def _get_navitia_physical_mode(self, indicator=None):
        """
        Get a navitia physical_mode for the codes present in COTS ("indicateurFer" : FERRE / ROUTIER)
        If the physical_mode doesn't exist in navitia, another request is made default physical_mode
        with filter=physical_mode.id=physical_mode:LongDistanceTrain
        """
        return self._request_navitia_physical_mode(indicator) or self._request_navitia_physical_mode()

    def _request_navitia_physical_mode(self, indicator=None):
        physical_modes = self.navitia.physical_modes(q={"filter": get_mode_filter(indicator), "count": "1"})
        if physical_modes:
            return physical_modes[0].get("id", None)
        return None
