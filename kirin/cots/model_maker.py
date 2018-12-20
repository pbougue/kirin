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

from __future__ import absolute_import, print_function, division

import logging
from datetime import datetime
from dateutil import parser, tz
from flask.globals import current_app
from pytz import utc

from kirin.abstract_sncf_model_maker import AbstractSNCFKirinModelBuilder, get_navitia_stop_time_sncf
# For perf benches:
# https://artem.krylysov.com/blog/2015/09/29/benchmark-python-json-libraries/
import ujson

from kirin.core import model
from kirin.cots.message_handler import MessageHandler
from kirin.exceptions import InvalidArguments
from kirin.utils import record_internal_failure
from kirin.core.types import TripEffect, ModificationType, get_higher_status, get_effect_by_stop_time_status


DEFAULT_COMPANY_ID = "1187"


def get_value(sub_json, key, nullable=False):
    """
    get a unique element in an json dict
    raise an exception if the element does not exists
    """
    res = sub_json.get(key)
    if res is None and not nullable:
        raise InvalidArguments('invalid json, impossible to find "{key}" in json dict {elt}'.format(
            key=key, elt=ujson.dumps(sub_json)))
    return res


def is_station(pdp):
    """
    determine if a Point de Parcours is a legit station
    :param pdp: stop_time to be checked
    :return: True if pdp is a station, False otherwise
    """
    t = get_value(pdp, 'typeArret', nullable=True)
    return (t is None) or (t in ['', 'CD', 'CH', 'FD', 'FH'])


def _retrieve_interesting_pdp(list_pdp):
    """
    Filter "Points de Parcours" (corresponding to stop_times in Navitia) to get only the relevant ones from
    a Navitia's perspective (stations, where travelers can hop on or hop off)
    Context: COTS may contain operating informations, useless for traveler
    :param list_pdp: an array of "Point de Parcours" (typically the one from the feed)
    :return: Filtered array
    Notes:  - written in a yield-fashion to switch implementation if possible, but we need random access for now
            - see 'test_retrieve_interesting_pdp' for a functional example
    """
    res = []
    picked_one = False
    for idx, pdp in enumerate(list_pdp):
        # At start, do not consume until there's a departure time (horaireVoyageurDepart)
        if not picked_one and not get_value(pdp, 'horaireVoyageurDepart', nullable=True):
            continue
        # exclude stop_times that are not legit stations
        if not is_station(pdp):
            continue
        # exclude stop_times that have no departure nor arrival time (empty stop_times)
        if not get_value(pdp, 'horaireVoyageurDepart', nullable=True) and \
                not get_value(pdp, 'horaireVoyageurArrivee', nullable=True):
            continue
        # stop consuming once all following stop_times are missing arrival time
        # * if a stop_time only has departure time, travelers can only hop in, but if they are be able to
        #   hop off later because some stop_time has arrival time then the current stop_time is useful,
        #   so we keep current stop_time.
        # * if no stop_time has arrival time anymore, then stop_times are useless as traveler cannot
        #   hop off, so no point hopping in anymore, so we remove all the stop_times until the end
        #   (should not happen in practice).
        if not get_value(pdp, 'horaireVoyageurArrivee', nullable=True):
            has_following_arrival = any(
                get_value(follow_pdp, 'horaireVoyageurArrivee', nullable=True) and is_station(follow_pdp)
                for follow_pdp in list_pdp[idx:])
            if not has_following_arrival:
                break

        picked_one = True
        res.append(pdp)
    return res


def as_date(s):
    if s is None:
        return None
    return parser.parse(s, dayfirst=False, yearfirst=True)


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
                s = get_value(p, 'source', nullable=True)
                if s is not None and s == source_ref:
                    return p

        # if a reference is provided but impossible to retrieve corresponding element, reject whole COTS feed
        raise InvalidArguments('invalid json, impossible to find source "{s}" in any json dict '
                               'of list: {list}'.format(s=source_ref, list=ujson.dumps(list_proj_time)))

    elif list_proj_time:
        # if no source-reference exists, but only one element in the list, we take it
        if len(list_proj_time) == 1:
            return list_proj_time[0]
        # if list has multiple elements but no source-reference, reject whole COTS feed
        raise InvalidArguments('invalid json, impossible no source but multiple json dicts '
                               'in list: {list}'.format(list=ujson.dumps(list_proj_time)))

    return None


def _is_fully_added_pdp(pdp):
    """
    Check if a projected arrival/departure object is fully created
    """
    # retrieve expressed statuses
    dep_arr_statuses = []
    for arrival_departure_toggle in ['Arrivee', 'Depart']:
        cots_traveler_time = get_value(pdp,
                                       'horaireVoyageur{}'.format(arrival_departure_toggle),
                                       nullable=True)
        if cots_traveler_time:
            dep_arr_statuses.append(get_value(cots_traveler_time, 'statutCirculationOPE', nullable=True))

    # if there are expressed cots_traveler_times and all are 'CREATION', pdp is fully added
    return dep_arr_statuses and all(s == 'CREATION' for s in dep_arr_statuses)


class KirinModelBuilder(AbstractSNCFKirinModelBuilder):

    def __init__(self, nav, contributor=None):
        super(KirinModelBuilder, self).__init__(nav, contributor)
        self.message_handler = MessageHandler(
            api_key=current_app.config['COTS_PAR_IV_API_KEY'],
            resource_server=current_app.config['COTS_PAR_IV_MOTIF_RESOURCE_SERVER'],
            token_server=current_app.config['COTS_PAR_IV_TOKEN_SERVER'],
            client_id=current_app.config['COTS_PAR_IV_CLIENT_ID'],
            client_secret=current_app.config['COTS_PAR_IV_CLIENT_SECRET'],
            grant_type=current_app.config['COTS_PAR_IV_GRANT_TYPE'],
            timeout=current_app.config['COTS_PAR_IV_REQUEST_TIMEOUT'])

    def build(self, rt_update):
        """
        parse the COTS raw json stored in the rt_update object (in Kirin db)
        and return a list of trip updates

        The TripUpdates are not yet associated with the RealTimeUpdate

        Most of the realtime information parsed is contained in the 'nouvelleVersion' sub-object
        (see fixtures and documentation)
        """
        try:
            json = ujson.loads(rt_update.raw_data)
        except ValueError as e:
            raise InvalidArguments("invalid json: {}".format(e.message))

        if 'nouvelleVersion' not in json:
            raise InvalidArguments('No object "nouvelleVersion" available in feed provided')

        dict_version = get_value(json, 'nouvelleVersion')
        vjs = self._get_vjs(dict_version)

        trip_updates = [self._make_trip_update(vj, dict_version) for vj in vjs]

        return trip_updates

    def _get_vjs(self, json_train):
        train_numbers = get_value(json_train, 'numeroCourse')
        pdps = _retrieve_interesting_pdp(get_value(json_train, 'listePointDeParcours'))
        if not pdps:
            raise InvalidArguments('invalid json, "listePointDeParcours" has no valid stop_time in '
                                   'json elt {elt}'.format(elt=ujson.dumps(json_train)))

        # retrieve base-schedule's first departure and last arrival
        def get_first_fully_added(list_pdps, hour_obj_name):
            p = next(p for p in list_pdps if not _is_fully_added_pdp(p))
            str_time = get_value(get_value(p, hour_obj_name), 'dateHeure') if p else None
            return parser.parse(str_time, dayfirst=False, yearfirst=True, ignoretz=False) if str_time else None

        utc_vj_start = get_first_fully_added(pdps, 'horaireVoyageurDepart').astimezone(utc)
        utc_vj_end = get_first_fully_added(reversed(pdps), 'horaireVoyageurArrivee').astimezone(utc)

        return self._get_navitia_vjs(train_numbers, utc_vj_start, utc_vj_end)

    def _record_and_log(self, logger, log_str):
        log_dict = {'log': log_str}
        record_internal_failure(log_dict['log'], contributor=self.contributor)
        log_dict.update({'contributor': self.contributor})
        logger.info('internal failure', extra=log_dict)

    @staticmethod
    def _check_stop_time_consistency(last_stop_time_depart, projected_stop_time, pdp_code):
        last_stop_time_depart = last_stop_time_depart if last_stop_time_depart is not None else \
            datetime.fromtimestamp(0, tz.tzutc())

        projected_arrival = projected_stop_time.get('Arrivee')
        projected_arrival = projected_arrival if projected_arrival is not None else last_stop_time_depart

        projected_departure = projected_stop_time.get('Depart')
        projected_departure = projected_departure if projected_departure is not None else projected_arrival

        if not (projected_departure >= projected_arrival >= last_stop_time_depart):
            raise InvalidArguments('invalid cots: stop_point\'s({}) time is not consistent'
                                   .format(pdp_code))

    def _make_trip_update(self, vj, json_train):
        """
        create the new TripUpdate object
        Following the COTS spec: https://github.com/CanalTP/kirin/blob/master/documentation/cots_connector.md
        """
        logger = logging.getLogger(__name__)
        trip_update = model.TripUpdate(vj=vj)
        trip_update.contributor = self.contributor
        trip_message_id = get_value(json_train, 'idMotifInterneReference', nullable=True)
        if trip_message_id:
            trip_update.message = self.message_handler.get_message(index=trip_message_id)
        cots_company_id = get_value(json_train, 'codeCompagnieTransporteur', nullable=True) or DEFAULT_COMPANY_ID
        trip_update.company_id = self._get_navitia_company(cots_company_id)

        trip_status = get_value(json_train, 'statutOperationnel')

        if trip_status == 'SUPPRIMEE':
            # the whole trip is deleted
            trip_update.status = ModificationType.delete.name
            trip_update.stop_time_updates = []
            trip_update.effect = TripEffect.NO_SERVICE.name
            return trip_update

        elif trip_status == 'AJOUTEE':
            # the trip is created from scratch
            # not handled yet
            self._record_and_log(logger, 'nouvelleVersion/statutOperationnel == "AJOUTEE" is not handled (yet)')
            trip_update.effect = TripEffect.ADDITIONAL_SERVICE.name
            return trip_update

        # all other status is considered an 'update' of the trip_update and effect is calculated
        # from stop_time status list. This part is also done in kraken and is to be deleted later
        # Ordered stop_time status= 'nochange', 'add', 'delete', 'update'
        # 'nochange' or 'update' -> SIGNIFICANT_DELAYS, add -> MODIFIED_SERVICE, delete = DETOUR
        trip_update.status = ModificationType.update.name
        trip_update.effect = TripEffect.MODIFIED_SERVICE.name

        # Initialize stop_time status to nochange
        highest_st_status = ModificationType.none.name
        pdps = _retrieve_interesting_pdp(get_value(json_train, 'listePointDeParcours'))

        # this variable is used to memoize the last stop_time's departure in order to check the stop_time consistency
        # ex. stop_time[i].arrival/departure must be greater than stop_time[i-1].departure
        last_stop_time_depart = None

        # manage realtime information stop_time by stop_time
        for pdp in pdps:
            # retrieve navitia's stop_point corresponding to the current COTS pdp
            nav_stop, log_dict = self._get_navitia_stop_point(pdp, vj.navitia_vj)
            projected_stop_time = {'Arrivee': None, 'Depart': None}

            if log_dict:
                record_internal_failure(log_dict['log'], contributor=self.contributor)
                log_dict.update({'contributor': self.contributor})
                logging.getLogger(__name__).info('metrology', extra=log_dict)

            if nav_stop is None:
                continue

            st_update = model.StopTimeUpdate(nav_stop)
            trip_update.stop_time_updates.append(st_update)
            # using the message from departure-time in priority, if absent fallback on arrival-time's message
            st_message_id = get_value(pdp, 'idMotifInterneDepartReference', nullable=True)
            if not st_message_id:
                st_message_id = get_value(pdp, 'idMotifInterneArriveeReference', nullable=True)
            if st_message_id:
                st_update.message = self.message_handler.get_message(index=st_message_id)

            _status_map = {'Arrivee': 'arrival_status', 'Depart': 'departure_status'}
            _delay_map = {'Arrivee': 'arrival_delay', 'Depart': 'departure_delay'}
            _add_map = {'Arrivee': 'arrival', 'Depart': 'departure'}

            # compute realtime information and fill st_update for arrival and departure
            for arrival_departure_toggle in ['Arrivee', 'Depart']:
                cots_traveler_time = get_value(pdp, 'horaireVoyageur{}'.format(arrival_departure_toggle),
                                               nullable=True)

                if cots_traveler_time is None:
                    continue

                cots_stop_time_status = get_value(cots_traveler_time, 'statutCirculationOPE', nullable=True)
                base_schedule_datetime = get_value(cots_traveler_time, 'dateHeure', nullable=True)

                if cots_stop_time_status is None:
                    # if no cots_stop_time_status, it is considered an 'update' of the stop_time
                    # (can be a delay, back to normal, normal, ...)
                    if base_schedule_datetime:
                        projected_stop_time[arrival_departure_toggle] = parser.parse(base_schedule_datetime)

                    cots_ref_planned = get_value(pdp,
                                                 'sourceHoraireProjete{}Reference'.format(arrival_departure_toggle),
                                                 nullable=True)
                    cots_planned_stop_times = get_value(pdp,
                                                        'listeHoraireProjete{}'.format(arrival_departure_toggle),
                                                        nullable=True)
                    cots_planned_stop_time = _retrieve_projected_time(cots_ref_planned, cots_planned_stop_times)

                    if cots_planned_stop_time is None:
                        continue

                    cots_delay = get_value(cots_planned_stop_time, 'pronosticIV', nullable=True)
                    if cots_delay is None:
                        continue

                    setattr(st_update, _status_map[arrival_departure_toggle], ModificationType.update.name)
                    setattr(st_update, _delay_map[arrival_departure_toggle], as_duration(cots_delay))

                    projected_stop_time[arrival_departure_toggle] += as_duration(cots_delay)

                elif cots_stop_time_status == 'SUPPRESSION':
                    # partial delete
                    setattr(st_update, _status_map[arrival_departure_toggle], ModificationType.delete.name)

                elif cots_stop_time_status == 'SUPPRESSION_DETOURNEMENT':
                    # stop_time is replaced by another one
                    setattr(st_update, _status_map[arrival_departure_toggle], ModificationType.deleted_for_detour.name)

                elif cots_stop_time_status == 'CREATION':
                    # new stop_time added
                    setattr(st_update, _status_map[arrival_departure_toggle], ModificationType.add.name)
                    setattr(st_update, _add_map[arrival_departure_toggle], base_schedule_datetime)

                    projected_stop_time[arrival_departure_toggle] = parser.parse(base_schedule_datetime)

                elif cots_stop_time_status == 'DETOURNEMENT':
                    setattr(st_update, _status_map[arrival_departure_toggle], 'added_for_detour')
                    setattr(st_update, _add_map[arrival_departure_toggle], base_schedule_datetime)

                else:
                    raise InvalidArguments('invalid value {} for field horaireVoyageur{}/statutCirculationOPE'.
                                           format(cots_stop_time_status, arrival_departure_toggle))

                arr_dep_status = getattr(st_update, _status_map[arrival_departure_toggle], ModificationType.none.name)
                highest_st_status = get_higher_status(highest_st_status, arr_dep_status)

            self._check_stop_time_consistency(last_stop_time_depart, projected_stop_time,
                                              pdp_code='-'.join(pdp[key] for key in ['cr', 'ci', 'ch']))
            last_stop_time_depart = projected_stop_time['Depart']

        # Calculates effect from stop_time status list(this work is also done in kraken and has to be deleted)
        trip_update.effect = get_effect_by_stop_time_status(highest_st_status)
        return trip_update

    def _get_navitia_stop_point(self, pdp, nav_vj):
        """
        Get a navitia stop point from the stop_time in a 'Point de Parcours' dict.
        The dict MUST contain cr, ci, ch tags.
        It searches in the vj's stops for a stop_area with the external code cr-ci-ch

        If the stop_time isn't found in the vj, in case of an additional stop_time,
        a request is made to Navitia.

        Error messages are also returned as 'missing stop point', 'duplicate stops'
        """
        nav_st, log_dict = get_navitia_stop_time_sncf(cr=get_value(pdp, 'cr'),
                                                      ci=get_value(pdp, 'ci'),
                                                      ch=get_value(pdp, 'ch'),
                                                      nav_vj=nav_vj)
        if not nav_st:
            nav_stop, log_dict = self._request_navitia_stop_point(cr=get_value(pdp, 'cr'),
                                                                  ci=get_value(pdp, 'ci'),
                                                                  ch=get_value(pdp, 'ch'))
        else:
            nav_stop = nav_st.get('stop_point', None)
        return nav_stop, log_dict

    def _request_navitia_stop_point(self, cr, ci, ch):
        external_code = '{}-{}-{}'.format(cr, ci, ch)
        stop_points = self.navitia.stop_points(q={
            'filter': 'stop_area.has_code("CR-CI-CH", "{}")'.format(external_code),
            'count': '1'
        })
        if stop_points:
            return stop_points[0], None

        return None, {'log': 'No stop point found', 'stop_point_code': external_code}

    def _get_navitia_company(self, code):
        """
        Get a navitia company for the code present in COTS
        If the company doesn't exist in navitia, another request is made to
        find company for key="RefProd" and value="1187"
        """
        return self._request_navitia_company(code) or self._request_navitia_company(DEFAULT_COMPANY_ID)

    def _request_navitia_company(self, code):
        companies = self.navitia.companies(q={
            'filter': 'company.has_code("RefProd", "{}")'.format(code),
            'count': '1'
        })
        if companies:
            return companies[0].get("id", None)
        return None
