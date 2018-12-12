# coding=utf-8

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

import datetime
import logging
import socket
from datetime import timedelta
from dateutil import parser
import pytz

import kirin
from kirin.core import model
from kirin.core.model import TripUpdate, StopTimeUpdate
from kirin.core.populate_pb import convert_to_gtfsrt
from kirin.exceptions import MessageNotPublished


def persist(real_time_update):
    """
    receive a RealTimeUpdate and persist it in the database
    """
    model.db.session.add(real_time_update)
    model.db.session.commit()


def log_stu_modif(trip_update, stu, string_additional_info):
    logger = logging.getLogger(__name__)
    logger.debug("TripUpdate on navitia vj {nav_id} on {date}, "
                 "StopTimeUpdate {order} modified: {add_info}".format(
                    nav_id=trip_update.vj.navitia_trip_id,
                    date=trip_update.vj.get_utc_circulation_date(),
                    order=stu.order,
                    add_info=string_additional_info))


def manage_consistency(trip_update):
    """
    receive a TripUpdate, then manage and adjust its consistency
    returns False if trip update cannot be managed
    """
    logger = logging.getLogger(__name__)
    previous_stu = None
    for current_order, stu in enumerate(trip_update.stop_time_updates):
        # rejections
        if stu.order != current_order:
            logger.warning("TripUpdate on navitia vj {nav_id} on {date} rejected: "
                           "order problem [STU index ({stu_index}) != kirin index ({kirin_index})]".format(
                                nav_id=trip_update.vj.navitia_trip_id,
                                date=trip_update.vj.get_utc_circulation_date(),
                                stu_index=stu.order,
                                kirin_index=current_order))
            return False

        # modifications
        if stu.arrival is None:
            stu.arrival = stu.departure
            if stu.arrival is None and previous_stu is not None:
                stu.arrival = previous_stu.departure
            if stu.arrival is None:
                logger.warning("TripUpdate on navitia vj {nav_id} on {date} rejected: "
                               "StopTimeUpdate missing arrival time".format(
                                    nav_id=trip_update.vj.navitia_trip_id,
                                    date=trip_update.vj.get_utc_circulation_date()))
                return False
            log_stu_modif(trip_update, stu, "arrival = {v}".format(v=stu.arrival))
            if not stu.arrival_delay and stu.departure_delay:
                stu.arrival_delay = stu.departure_delay
                log_stu_modif(trip_update, stu, "arrival_delay = {v}".format(v=stu.arrival_delay))

        if stu.departure is None:
            stu.departure = stu.arrival
            log_stu_modif(trip_update, stu, "departure = {v}".format(v=stu.departure))
            if not stu.departure_delay and stu.arrival_delay:
                stu.departure_delay = stu.arrival_delay
                log_stu_modif(trip_update, stu, "departure_delay = {v}".format(v=stu.departure_delay))

        if stu.arrival_delay is None:
            stu.arrival_delay = datetime.timedelta(0)
            log_stu_modif(trip_update, stu, "arrival_delay = {v}".format(v=stu.arrival_delay))

        if stu.departure_delay is None:
            stu.departure_delay = datetime.timedelta(0)
            log_stu_modif(trip_update, stu, "departure_delay = {v}".format(v=stu.departure_delay))

        if previous_stu is not None and previous_stu.departure > stu.arrival:
            delay_diff = previous_stu.departure_delay - stu.arrival_delay
            stu.arrival += delay_diff
            stu.arrival_delay += delay_diff
            log_stu_modif(trip_update, stu, "arrival = {a} and arrival_delay = {a_d}".format(
                                                        a=stu.arrival, a_d=stu.arrival_delay))

        if stu.arrival > stu.departure:
            stu.departure_delay += stu.arrival - stu.departure
            stu.departure = stu.arrival
            log_stu_modif(trip_update, stu, "departure = {a} and departure_delay = {a_d}".format(
                                                        a=stu.departure, a_d=stu.departure_delay))

        previous_stu = stu

    return True


def find_st_in_vj(st_id, vj_sts):
    """
    Find a stop_time in the navitia vehicle journey
    :param st_id: id of the requested stop_time
    :param vj_sts: list of stop_times available in the vj
    :return: stop_time if found else None
    """
    return next((vj_st for vj_st in vj_sts if vj_st.get('stop_point', {}).get('id') == st_id), None)


def extract_str_utc_time(str_time):
    """
    Return UTC time of day from given str
    :param str_time: datetime+timezone (type: str)
    :return: corresponding time of day in UTC timezone (type: datetime.time)

    >>> str_time = '20181108T093000+0000'
    >>> extract_str_utc_time(str_time)
    datetime.time(9, 30)
    >>> str_time = '20181108T093000+0100'
    >>> extract_str_utc_time(str_time)
    datetime.time(8, 30)
    >>> str_time = '20181108T093000+0900'
    >>> extract_str_utc_time(str_time)
    datetime.time(0, 30)
    """
    if str_time:
        return parser.parse(str_time).astimezone(pytz.utc).time()


def handle(real_time_update, trip_updates, contributor, is_new_complete=False):
    """
    receive a RealTimeUpdate with at least one TripUpdate filled with the data received
    by the connector. each TripUpdate is associated with the VehicleJourney returned by jormugandr
    Returns real_time_update and the log_dict
    """
    if not real_time_update:
        raise TypeError()
    id_timestamp_tuples = [(tu.vj.navitia_trip_id, tu.vj.get_start_timestamp()) for tu in trip_updates]
    old_trip_updates = TripUpdate.find_by_dated_vjs(id_timestamp_tuples)
    for trip_update in trip_updates:
        # find if there is already a row in db
        old = next((tu for tu in old_trip_updates if tu.vj.navitia_trip_id == trip_update.vj.navitia_trip_id
                    and tu.vj.get_start_timestamp() == trip_update.vj.get_start_timestamp()), None)
        # merge the base schedule, the current realtime, and the new realtime
        current_trip_update = merge(trip_update.vj.navitia_vj, old, trip_update, is_new_complete=is_new_complete)

        # manage and adjust consistency if possible
        if current_trip_update and manage_consistency(current_trip_update):
            # we have to link the current_vj_update with the new real_time_update
            # this link is done quite late to avoid too soon persistence of trip_update by sqlalchemy
            current_trip_update.real_time_updates.append(real_time_update)

    persist(real_time_update)

    feed = convert_to_gtfsrt(real_time_update.trip_updates)
    feed_str = feed.SerializeToString()
    publish(feed_str, contributor)

    data_time = datetime.datetime.utcfromtimestamp(feed.header.timestamp)
    log_dict = {'contributor': contributor, 'timestamp': data_time, 'trip_update_count': len(feed.entity),
                'size': len(feed_str)}
    return real_time_update, log_dict


def _get_datetime(utc_circulation_date, utc_time):
    # in the db, dt with timezone cannot coexist with dt without timezone
    # since at the beginning there was dt without tz, we keep naive dt
    return datetime.datetime.combine(utc_circulation_date, utc_time)


def _get_update_info_of_stop_time(base_time, input_status, input_delay):
    new_time = None
    status = 'none'
    delay = timedelta(0)
    if input_status == 'update':
        new_time = (base_time + input_delay) if base_time else None
        status = input_status
        delay = input_delay
    elif input_status in ('delete', 'deleted_for_detour'):
        # passing status 'delete' on the stop_time
        # Note: we keep providing base_schedule stop_time to better identify the stop_time
        # in the vj (for lollipop lines for example)
        status = input_status
    elif input_status in ('add', 'added_for_detour'):
        status = input_status
        new_time = base_time
    else:
        new_time = base_time
    return new_time, status, delay


def _make_stop_time_update(base_arrival, base_departure, last_departure, input_st, stop_point, order):
    dep, dep_status, dep_delay = _get_update_info_of_stop_time(base_departure,
                                                               input_st.departure_status,
                                                               input_st.departure_delay)
    arr, arr_status, arr_delay = _get_update_info_of_stop_time(base_arrival,
                                                               input_st.arrival_status,
                                                               input_st.arrival_delay)

    # in case where arrival/departure time are None
    if arr is None:
        arr = dep if dep is not None else last_departure
    dep = dep if dep is not None else arr

    # in case where the previous departure time are greater than the current arrival
    if last_departure and last_departure > arr:
        arr_delay += (last_departure - arr)
        arr = last_departure

    # in the real world, the departure time must be greater or equal to the arrival time
    if arr > dep:
        dep_delay += (arr - dep)
        dep = arr

    return StopTimeUpdate(navitia_stop=stop_point,
                          departure=dep,
                          departure_delay=dep_delay,
                          dep_status=dep_status,
                          arrival=arr,
                          arrival_delay=arr_delay,
                          arr_status=arr_status,
                          message=input_st.message,
                          order=order)


def merge(navitia_vj, db_trip_update, new_trip_update, is_new_complete=False):
    """
    We need to merge the info from 3 sources:
        * the navitia base schedule
        * the trip update already in the db (potentially nonexistent)
        * the incoming trip update

    The result is either the db_trip_update if it exists, or the new_trip_update (it is updated as a side
    effect)

    The mechanism is quite simple:
        * the result trip status is the new_trip_update's status
            (ie in the db the trip was cancelled, and a new update is only an update, the trip update is
            not cancelled anymore, only updated)

        * for each navitia's stop_time and for departure|arrival:
            - if there is an update on this stoptime (in new_trip_update):
                we compute the new datetime based on the new information and the navitia's base schedule
            - else if there is the stoptime in the db:
                we keep this db stoptime
            - else we keep the navitia's base schedule

    Note that the results is either 'db_trip_update' or 'new_trip_update'. Side effects on this object are
    thus wanted because of database persistency (update or creation of new objects)

    If is_new_complete==True, then new_trip_update is considered as a complete trip, so it will erase and
    replace the (old) db_trip_update.
    Detail: is_new_complete changes the way None is interpreted in the new_trip_update:
        - if is_new_complete==False, None means there is no new information, so we keep old ones
        - if is_new_complete==True, None means we are back to normal, so we keep the new None
          (for now it only impacts messages to allow removal)


    ** Important Note **:
    we DO NOT HANDLE changes in navitia's schedule for the moment
    it will need to be handled, but it will be done after
    """
    res = db_trip_update if db_trip_update else new_trip_update
    res_stoptime_updates = []

    res.status = new_trip_update.status
    if new_trip_update.message is not None or is_new_complete:
        res.message = new_trip_update.message
    res.contributor = new_trip_update.contributor

    if res.status == 'delete':
        # for trip cancellation, we delete all stoptimes update
        res.stop_time_updates = []
        return res

    last_nav_dep = None
    last_departure = None
    utc_circulation_date = new_trip_update.vj.get_utc_circulation_date()

    def get_next_stop():
        if is_new_complete:
            # Iterate on the new trip update stop_times if it is complete (all stop_times present in it)
            for order, st in enumerate(new_trip_update.stop_time_updates):
                # Find corresponding stop_time in the theoretical VJ
                vj_st = find_st_in_vj(st.stop_id, new_trip_update.vj.navitia_vj.get('stop_times', []))
                if vj_st is None and st.departure_status in ('add', 'added_for_detour')  \
                        or st.arrival_status in ('add', 'added_for_detour'):
                    # It is an added stop_time, create a new stop time
                    added_st = {
                        'stop_point': st.navitia_stop,
                        'utc_departure_time': extract_str_utc_time(st.departure),
                        'utc_arrival_time': extract_str_utc_time(st.arrival),
                        'departure_status': st.departure_status,
                        'arrival_status': st.arrival_status
                    }
                    yield order, added_st
                else:
                    yield order, vj_st

        else:
            # Iterate on the theoretical VJ if the new trip update doesn't list all stop_times
            for order, vj_st in enumerate(navitia_vj.get('stop_times', [])):
                yield order, vj_st

    has_changes = False
    for nav_order, navitia_stop in get_next_stop():
        if navitia_stop is None:
            logging.getLogger(__name__).warning('No stop point found (order:{}'.format(nav_order))
            continue

        # TODO handle forbidden pickup/dropoff (in those case set departure/arrival at None)
        utc_nav_departure_time = navitia_stop.get('utc_departure_time')
        utc_nav_arrival_time = navitia_stop.get('utc_arrival_time')

        # we compute the arrival time and departure time on base schedule and take past mid-night into
        # consideration
        base_arrival = base_departure = None
        if utc_nav_arrival_time is not None:
            if last_nav_dep is not None and last_nav_dep > utc_nav_arrival_time:
                # last departure is after arrival, it's a past-midnight
                utc_circulation_date += timedelta(days=1)
            base_arrival = _get_datetime(utc_circulation_date, utc_nav_arrival_time)

        if utc_nav_departure_time is not None:
            if utc_nav_arrival_time is not None and utc_nav_arrival_time > utc_nav_departure_time:
                # departure is before arrival, it's a past-midnight
                utc_circulation_date += timedelta(days=1)
            base_departure = _get_datetime(utc_circulation_date, utc_nav_departure_time)

        stop_id = navitia_stop.get('stop_point', {}).get('id')
        new_st = new_trip_update.find_stop(stop_id, nav_order)

        if db_trip_update is not None and new_st is not None:
            """
            First case: we already have recorded the delay and we find update info in the new trip update
            Then      : we should probably update it or not if the input info is exactly the same as the one in db
            """
            db_st = db_trip_update.find_stop(stop_id, nav_order)
            new_st_update = _make_stop_time_update(base_arrival,
                                                   base_departure,
                                                   last_departure,
                                                   new_st,
                                                   navitia_stop['stop_point'],
                                                   order=nav_order)
            has_changes |= (db_st is None) or db_st.is_not_equal(new_st_update)
            res_st = new_st_update if has_changes else db_st

        elif db_trip_update is None and new_st is not None:
            """
            Second case: we have not yet recorded the delay
            Then       : it's time to create one in the db
            """
            has_changes = True
            res_st = _make_stop_time_update(base_arrival,
                                            base_departure,
                                            last_departure,
                                            new_st,
                                            navitia_stop['stop_point'],
                                            order=nav_order)
            res_st.message = new_st.message

        elif db_trip_update is not None and new_st is None:
            """
            Third case: we have already recorded a delay but nothing is mentioned in the new trip update
            Then      : For IRE, we do nothing but only update stop time's order
                        For gtfs-rt, according to the specification, we should use the delay from the previous
                        stop time, which will be handled sooner by the connector-specified model maker
                        
                        *** Here, we MUST NOT do anything, only update stop time's order ***
            """
            db_st = db_trip_update.find_stop(stop_id, nav_order)
            res_st = db_st if db_st is not None else StopTimeUpdate(navitia_stop['stop_point'],
                                                                    departure=base_departure,
                                                                    arrival=base_arrival,
                                                                    order=nav_order)
            has_changes |= (db_st is None)
        else:
            """
            Last case: nothing is recorded yet and there is no update info in the new trip update
            Then     : take the base schedule's arrival/departure time and let's create a whole new world!
            """
            has_changes = True
            res_st = StopTimeUpdate(navitia_stop['stop_point'],
                                    departure=base_departure,
                                    arrival=base_arrival,
                                    order=nav_order)

        last_departure = res_st.departure
        res_stoptime_updates.append(res_st)

        # For a stop_time deleted or deleted_for_detour, we don't use previous value for
        # arrival and departure time consistency
        if res_st.arrival_status not in ('deleted', 'deleted_for_detour'):
            last_nav_dep = utc_nav_departure_time

    if has_changes:
        res.stop_time_updates = res_stoptime_updates
        return res

    return None


def publish(feed, contributor):
    """
    send RT feed to navitia
    """
    try:
        kirin.rabbitmq_handler.publish(feed, contributor)

    except socket.error:
        logging.getLogger(__name__).exception('impossible to publish in rabbitmq')
        raise MessageNotPublished()
