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
# [matrix] channel #navitia:matrix.org (https://app.element.io/#/room/#navitia:matrix.org)
# https://groups.google.com/d/forum/navitia
# www.navitia.io
from __future__ import absolute_import, print_function, unicode_literals, division
from datetime import timedelta

from kirin.core.model import TripUpdate, VehicleJourney, StopTimeUpdate
from kirin.core.populate_pb import convert_to_gtfsrt, to_posix_time, fill_stop_times
import datetime
from kirin import app, db
from kirin import gtfs_realtime_pb2, kirin_pb2
from kirin.cots.model_maker import make_navitia_empty_vj
from kirin.utils import make_rt_update
from tests.check_utils import _dt
from tests.integration.conftest import COTS_CONTRIBUTOR


def test_populate_pb_with_one_stop_time():
    """
    an easy one: we have one vj with only one stop time updated
    fill protobuf from trip_update
    Verify protobuf
    """
    navitia_vj = {
        "trip": {"id": "vehicle_journey:1"},
        "stop_times": [
            {
                "utc_arrival_time": None,
                "utc_departure_time": datetime.time(6, 10),
                "stop_point": {"id": "sa:1", "stop_area": {"timezone": "Europe/Paris"}},
            },
            {
                "utc_arrival_time": datetime.time(7, 10),
                "utc_departure_time": None,
                "stop_point": {"id": "sa:2", "stop_area": {"timezone": "Europe/Paris"}},
            },
        ],
    }

    with app.app_context():
        vj = VehicleJourney(
            navitia_vj, datetime.datetime(2015, 9, 8, 5, 10, 0), datetime.datetime(2015, 9, 8, 8, 10, 0)
        )
        trip_update = TripUpdate(vj=vj, contributor=COTS_CONTRIBUTOR)
        st = StopTimeUpdate({"id": "sa:1"}, departure=_dt("8:15"), arrival=None)
        real_time_update = make_rt_update(raw_data=None, connector="cots", contributor=COTS_CONTRIBUTOR)
        real_time_update.trip_updates.append(trip_update)
        trip_update.stop_time_updates.append(st)

        db.session.add(real_time_update)
        db.session.commit()

        feed_entity = convert_to_gtfsrt(real_time_update.trip_updates)

        assert feed_entity.header.incrementality == gtfs_realtime_pb2.FeedHeader.DIFFERENTIAL
        assert feed_entity.header.gtfs_realtime_version == "1"
        pb_trip_update = feed_entity.entity[0].trip_update
        assert pb_trip_update.trip.trip_id == "vehicle_journey:1"
        assert pb_trip_update.trip.start_date == "20150908"
        assert pb_trip_update.trip.schedule_relationship == gtfs_realtime_pb2.TripDescriptor.SCHEDULED
        pb_stop_time = feed_entity.entity[0].trip_update.stop_time_update[0]
        assert pb_stop_time.arrival.time == 0
        assert pb_stop_time.departure.time == to_posix_time(_dt("8:15"))
        assert pb_stop_time.stop_id == "sa:1"

        assert pb_trip_update.HasExtension(kirin_pb2.trip_message) is False
        assert pb_trip_update.trip.HasExtension(kirin_pb2.contributor) is True
        assert pb_trip_update.trip.HasExtension(kirin_pb2.company_id) is False
        assert pb_trip_update.HasExtension(kirin_pb2.effect) is False


def test_populate_pb_with_two_stop_time():
    """
    an easy one: we have one vj with only one stop time updated
    fill protobuf from trip_update
    Verify protobuf
    """

    # we add another impacted stop time to the Model
    navitia_vj = {
        "trip": {"id": "vehicle_journey:1"},
        "stop_times": [
            {
                "utc_arrival_time": None,
                "utc_departure_time": datetime.time(6, 10),
                "stop_point": {"id": "sa:1", "stop_area": {"timezone": "Europe/Paris"}},
            },
            {"utc_arrival_time": datetime.time(7, 10), "utc_departure_time": None, "stop_point": {"id": "sa:2"}},
        ],
    }

    with app.app_context():
        vj = VehicleJourney(
            navitia_vj, datetime.datetime(2015, 9, 8, 5, 10, 0), datetime.datetime(2015, 9, 8, 8, 10, 0)
        )
        trip_update = TripUpdate(vj=vj, contributor=COTS_CONTRIBUTOR)
        real_time_update = make_rt_update(raw_data=None, connector="cots", contributor=COTS_CONTRIBUTOR)
        real_time_update.trip_updates.append(trip_update)
        st = StopTimeUpdate(
            {"id": "sa:1"}, departure=_dt("8:15"), departure_delay=timedelta(minutes=5), arrival=None
        )
        trip_update.stop_time_updates.append(st)

        st = StopTimeUpdate(
            {"id": "sa:2"},
            departure=_dt("8:21"),
            departure_delay=timedelta(minutes=-40),
            arrival=_dt("8:20"),
            arrival_delay=timedelta(minutes=-40),
            message="bob's on the track",
        )
        real_time_update.trip_updates[0].stop_time_updates.append(st)

        db.session.add(real_time_update)
        db.session.commit()

        feed_entity = convert_to_gtfsrt(real_time_update.trip_updates)

        assert feed_entity.header.incrementality == gtfs_realtime_pb2.FeedHeader.DIFFERENTIAL
        assert feed_entity.header.gtfs_realtime_version, "1"
        assert len(feed_entity.entity) == 1
        pb_trip_update = feed_entity.entity[0].trip_update
        assert pb_trip_update.trip.trip_id == "vehicle_journey:1"
        assert pb_trip_update.trip.start_date == "20150908"
        assert pb_trip_update.HasExtension(kirin_pb2.trip_message) is False
        assert pb_trip_update.trip.HasExtension(kirin_pb2.contributor) is True
        assert pb_trip_update.trip.HasExtension(kirin_pb2.company_id) is False
        assert pb_trip_update.HasExtension(kirin_pb2.effect) is False
        assert pb_trip_update.trip.schedule_relationship == gtfs_realtime_pb2.TripDescriptor.SCHEDULED

        assert len(pb_trip_update.stop_time_update) == 2

        pb_stop_time = pb_trip_update.stop_time_update[0]
        assert pb_stop_time.stop_id == "sa:1"
        assert pb_stop_time.arrival.time == 0
        assert pb_stop_time.arrival.delay == 0
        assert pb_stop_time.departure.time == to_posix_time(_dt("8:15"))
        assert pb_stop_time.departure.delay == 5 * 60
        assert pb_stop_time.Extensions[kirin_pb2.stoptime_message] == ""

        pb_stop_time = pb_trip_update.stop_time_update[1]
        assert pb_stop_time.stop_id == "sa:2"
        assert pb_stop_time.arrival.time == to_posix_time(_dt("8:20"))
        assert pb_stop_time.arrival.delay == -40 * 60
        assert pb_stop_time.departure.time == to_posix_time(_dt("8:21"))
        assert pb_stop_time.departure.delay == -40 * 60
        assert pb_stop_time.Extensions[kirin_pb2.stoptime_message] == "bob's on the track"


def test_populate_pb_with_deleted_stop_time():
    """
    test protobuf for partial delete

    nav vj
    sa:1 * ---- * sa:2 * ---- * sa:3 * ---- * sa:4

    we stop the vj from going to sa:3 and sa:4

    sa:1 * ---- * sa:2

    And we delay sa:1 of 5 minutes

    Note: in the message sent sa:2 departure will be removed as well
    as sa:3 departures/arrival and sa:4' arrival
    """
    # we add another impacted stop time to the Model
    from datetime import time

    navitia_vj = {
        "trip": {"id": "vehicle_journey:1"},
        "stop_times": [
            {
                "utc_arrival_time": None,
                "utc_departure_time": time(6, 11),
                "stop_point": {"id": "sa:1", "stop_area": {"timezone": "Europe/Paris"}},
            },
            {
                "utc_arrival_time": time(7, 10),
                "utc_departure_time": time(7, 11),
                "stop_point": {"id": "sa:2", "stop_area": {"timezone": "Europe/Paris"}},
            },
            {
                "utc_arrival_time": time(8, 10),
                "utc_departure_time": time(8, 11),
                "stop_point": {"id": "sa:3", "stop_area": {"timezone": "Europe/Paris"}},
            },
            {
                "utc_arrival_time": time(9, 10),
                "utc_departure_time": None,
                "stop_point": {"id": "sa:4", "stop_area": {"timezone": "Europe/Paris"}},
            },
        ],
    }

    with app.app_context():
        vj = VehicleJourney(
            navitia_vj, datetime.datetime(2015, 9, 8, 5, 11, 0), datetime.datetime(2015, 9, 8, 10, 10, 0)
        )
        trip_update = TripUpdate(vj=vj, contributor=COTS_CONTRIBUTOR)
        real_time_update = make_rt_update(raw_data=None, connector="cots", contributor=COTS_CONTRIBUTOR)
        real_time_update.trip_updates.append(trip_update)
        st = StopTimeUpdate(
            {"id": "sa:1"}, departure=_dt("8:15"), departure_delay=timedelta(minutes=5), arrival=None
        )
        trip_update.stop_time_updates.append(st)

        st = StopTimeUpdate(
            {"id": "sa:2"},
            arrival=_dt("9:10"),
            departure=_dt("9:11"),
            dep_status="delete",  # we delete the departure
            message="bob's on the track",
        )
        real_time_update.trip_updates[0].stop_time_updates.append(st)

        st = StopTimeUpdate(
            {"id": "sa:3"},
            # Note: we still send the departure/arrival for coherence
            arrival=_dt("10:10"),
            dep_status="delete",  # we delete both departure and arrival
            departure=_dt("10:11"),
            arr_status="delete",
            message="bob's on the track",
        )
        real_time_update.trip_updates[0].stop_time_updates.append(st)

        st = StopTimeUpdate(
            {"id": "sa:4"},
            arrival=_dt("11:10"),
            arr_status="delete",  # we delete only the arrival
            departure=_dt("11:11"),
            message="bob's on the track",
        )
        real_time_update.trip_updates[0].stop_time_updates.append(st)

        db.session.add(real_time_update)
        db.session.commit()

        feed_entity = convert_to_gtfsrt(real_time_update.trip_updates)

        assert feed_entity.header.incrementality == gtfs_realtime_pb2.FeedHeader.DIFFERENTIAL
        assert feed_entity.header.gtfs_realtime_version, "1"
        assert len(feed_entity.entity) == 1
        pb_trip_update = feed_entity.entity[0].trip_update
        assert pb_trip_update.trip.trip_id == "vehicle_journey:1"
        assert pb_trip_update.trip.start_date == "20150908"
        assert pb_trip_update.HasExtension(kirin_pb2.trip_message) is False
        assert pb_trip_update.trip.HasExtension(kirin_pb2.contributor) is True
        assert pb_trip_update.trip.HasExtension(kirin_pb2.company_id) is False
        assert pb_trip_update.HasExtension(kirin_pb2.effect) is False
        assert pb_trip_update.trip.schedule_relationship == gtfs_realtime_pb2.TripDescriptor.SCHEDULED

        assert len(pb_trip_update.stop_time_update) == 4

        pb_stop_time = pb_trip_update.stop_time_update[0]
        assert pb_stop_time.stop_id == "sa:1"
        assert pb_stop_time.arrival.time == 0
        assert pb_stop_time.arrival.delay == 0
        assert pb_stop_time.departure.time == to_posix_time(_dt("8:15"))
        assert pb_stop_time.departure.delay == 5 * 60
        assert pb_stop_time.Extensions[kirin_pb2.stoptime_message] == ""

        pb_stop_time = pb_trip_update.stop_time_update[1]
        assert pb_stop_time.stop_id == "sa:2"
        assert pb_stop_time.arrival.time == to_posix_time(_dt("9:10"))
        assert pb_stop_time.arrival.delay == 0
        # the arrival at the stop is still scheduled, but the departure is skipped
        assert (
            pb_stop_time.arrival.Extensions[kirin_pb2.stop_time_event_relationship]
            == gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SCHEDULED
        )
        assert pb_stop_time.departure.time == to_posix_time(_dt("9:11"))
        assert pb_stop_time.departure.delay == 0
        assert (
            pb_stop_time.departure.Extensions[kirin_pb2.stop_time_event_relationship]
            == gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED
        )
        assert pb_stop_time.Extensions[kirin_pb2.stoptime_message] == "bob's on the track"

        pb_stop_time = pb_trip_update.stop_time_update[2]
        assert pb_stop_time.stop_id == "sa:3"
        # both departure and arrival are SKIPPED
        assert pb_stop_time.arrival.time == to_posix_time(_dt("10:10"))
        assert pb_stop_time.arrival.delay == 0
        assert (
            pb_stop_time.arrival.Extensions[kirin_pb2.stop_time_event_relationship]
            == gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED
        )
        assert pb_stop_time.departure.time == to_posix_time(_dt("10:11"))
        assert pb_stop_time.departure.delay == 0
        assert (
            pb_stop_time.departure.Extensions[kirin_pb2.stop_time_event_relationship]
            == gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED
        )
        assert pb_stop_time.Extensions[kirin_pb2.stoptime_message] == "bob's on the track"

        pb_stop_time = pb_trip_update.stop_time_update[3]
        assert pb_stop_time.stop_id == "sa:4"
        # only the arrival is skipped, the departure does not exists as it's the last stop
        assert pb_stop_time.arrival.time == to_posix_time(_dt("11:10"))
        assert pb_stop_time.arrival.delay == 0
        assert (
            pb_stop_time.arrival.Extensions[kirin_pb2.stop_time_event_relationship]
            == gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED
        )
        assert pb_stop_time.departure.time == to_posix_time(_dt("11:11"))
        assert pb_stop_time.departure.delay == 0
        assert (
            pb_stop_time.departure.Extensions[kirin_pb2.stop_time_event_relationship]
            == gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SCHEDULED
        )
        assert pb_stop_time.Extensions[kirin_pb2.stoptime_message] == "bob's on the track"


def test_populate_pb_with_cancelation():
    """
    VJ cancelation
    """
    navitia_vj = {
        "trip": {"id": "vehicle_journey:1"},
        "stop_times": [
            {"utc_arrival_time": datetime.time(8, 10), "stop_point": {"stop_area": {"timezone": "UTC"}}}
        ],
    }

    with app.app_context():
        vj = VehicleJourney(
            navitia_vj, datetime.datetime(2015, 9, 8, 7, 10, 0), datetime.datetime(2015, 9, 8, 11, 5, 0)
        )
        trip_update = TripUpdate(vj=vj, contributor=COTS_CONTRIBUTOR)
        trip_update.vj = vj
        trip_update.status = "delete"
        trip_update.message = "Message Test"
        real_time_update = make_rt_update(raw_data=None, connector="cots", contributor=COTS_CONTRIBUTOR)
        trip_update.company_id = "sncf"
        trip_update.effect = "REDUCED_SERVICE"
        real_time_update.trip_updates.append(trip_update)

        db.session.add(real_time_update)
        db.session.commit()

        feed_entity = convert_to_gtfsrt(real_time_update.trip_updates)

        assert feed_entity.header.incrementality == gtfs_realtime_pb2.FeedHeader.DIFFERENTIAL
        assert feed_entity.header.gtfs_realtime_version == "1"
        pb_trip_update = feed_entity.entity[0].trip_update
        assert pb_trip_update.trip.trip_id == "vehicle_journey:1"
        assert pb_trip_update.trip.start_date == "20150908"
        assert pb_trip_update.HasExtension(kirin_pb2.trip_message) is True
        assert pb_trip_update.Extensions[kirin_pb2.trip_message] == "Message Test"
        assert pb_trip_update.trip.schedule_relationship == gtfs_realtime_pb2.TripDescriptor.CANCELED

        assert pb_trip_update.trip.HasExtension(kirin_pb2.contributor) is True
        assert pb_trip_update.trip.Extensions[kirin_pb2.contributor] == COTS_CONTRIBUTOR
        assert pb_trip_update.trip.Extensions[kirin_pb2.company_id] == "sncf"
        assert pb_trip_update.Extensions[kirin_pb2.effect] == gtfs_realtime_pb2.Alert.REDUCED_SERVICE

        assert len(feed_entity.entity[0].trip_update.stop_time_update) == 0


def test_populate_pb_with_full_dataset():
    """
    VJ cancelation
    """
    navitia_vj = {
        "trip": {"id": "vehicle_journey:1"},
        "stop_times": [{"utc_arrival_time": datetime.time(8, 10)}],
    }

    with app.app_context():
        vj = VehicleJourney(
            navitia_vj, datetime.datetime(2015, 9, 8, 7, 10, 0), datetime.datetime(2015, 9, 8, 9, 10, 0)
        )
        trip_update = TripUpdate(vj=vj, contributor=COTS_CONTRIBUTOR)
        trip_update.status = "delete"
        trip_update.message = "Message Test"
        real_time_update = make_rt_update(raw_data=None, connector="cots", contributor=COTS_CONTRIBUTOR)
        trip_update.company_id = "keolis"
        trip_update.effect = "DETOUR"
        real_time_update.trip_updates.append(trip_update)

        db.session.add(real_time_update)
        db.session.commit()

        feed_entity = convert_to_gtfsrt(real_time_update.trip_updates, gtfs_realtime_pb2.FeedHeader.FULL_DATASET)

        assert feed_entity.header.incrementality == gtfs_realtime_pb2.FeedHeader.FULL_DATASET
        assert feed_entity.header.gtfs_realtime_version == "1"
        pb_trip_update = feed_entity.entity[0].trip_update
        assert pb_trip_update.trip.trip_id == "vehicle_journey:1"
        assert pb_trip_update.trip.start_date == "20150908"
        assert pb_trip_update.HasExtension(kirin_pb2.trip_message) == True
        assert pb_trip_update.Extensions[kirin_pb2.trip_message] == "Message Test"
        assert pb_trip_update.trip.schedule_relationship == gtfs_realtime_pb2.TripDescriptor.CANCELED

        assert pb_trip_update.trip.HasExtension(kirin_pb2.contributor) is True
        assert pb_trip_update.trip.Extensions[kirin_pb2.contributor] == COTS_CONTRIBUTOR
        assert pb_trip_update.trip.Extensions[kirin_pb2.company_id] == "keolis"
        assert pb_trip_update.Extensions[kirin_pb2.effect] == gtfs_realtime_pb2.Alert.DETOUR

        assert len(feed_entity.entity[0].trip_update.stop_time_update) == 0


def test_populate_pb_no_status_stop_times_status():
    st_no_status = StopTimeUpdate({"id": "id1"}, dep_status="none", arr_status="none")
    pb_stop_time = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate()

    fill_stop_times(pb_stop_time, st_no_status)

    assert pb_stop_time.departure.Extensions[kirin_pb2.stop_time_event_status] == kirin_pb2.SCHEDULED
    assert pb_stop_time.arrival.Extensions[kirin_pb2.stop_time_event_status] == kirin_pb2.SCHEDULED


def test_populate_pb_added_for_detour_stop_times_status():
    st_added_status = StopTimeUpdate({"id": "id1"}, dep_status="added_for_detour", arr_status="added_for_detour")
    pb_stop_time = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate()

    fill_stop_times(pb_stop_time, st_added_status)

    assert pb_stop_time.departure.Extensions[kirin_pb2.stop_time_event_status] == kirin_pb2.ADDED_FOR_DETOUR
    assert pb_stop_time.arrival.Extensions[kirin_pb2.stop_time_event_status] == kirin_pb2.ADDED_FOR_DETOUR


def test_populate_pb_added_stop_times_status():
    st_added_status = StopTimeUpdate({"id": "id1"}, dep_status="add", arr_status="add")
    pb_stop_time = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate()

    fill_stop_times(pb_stop_time, st_added_status)

    assert pb_stop_time.departure.Extensions[kirin_pb2.stop_time_event_status] == kirin_pb2.ADDED
    assert pb_stop_time.arrival.Extensions[kirin_pb2.stop_time_event_status] == kirin_pb2.ADDED


def test_populate_pb_skipped_for_detour_stop_times_status():
    st_added_status = StopTimeUpdate(
        {"id": "id1"}, dep_status="deleted_for_detour", arr_status="deleted_for_detour"
    )
    pb_stop_time = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate()

    fill_stop_times(pb_stop_time, st_added_status)

    assert pb_stop_time.departure.Extensions[kirin_pb2.stop_time_event_status] == kirin_pb2.DELETED_FOR_DETOUR
    assert pb_stop_time.arrival.Extensions[kirin_pb2.stop_time_event_status] == kirin_pb2.DELETED_FOR_DETOUR


def test_populate_pb_deleted_stop_times_status():
    st_added_status = StopTimeUpdate({"id": "id1"}, dep_status="delete", arr_status="delete")
    pb_stop_time = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate()

    fill_stop_times(pb_stop_time, st_added_status)

    assert pb_stop_time.departure.Extensions[kirin_pb2.stop_time_event_status] == kirin_pb2.DELETED
    assert pb_stop_time.arrival.Extensions[kirin_pb2.stop_time_event_status] == kirin_pb2.DELETED


def test_populate_pb_for_added_trip():
    """
    For an added trip we don't call navitia to get a vj instead we create and initialize one vj only with 'id'
    Fill protobuf from trip_update
    Verify protobuf
    """
    navitia_vj = make_navitia_empty_vj("vehicle_journey:1")

    with app.app_context():
        vj = VehicleJourney(
            navitia_vj,
            since_dt=datetime.datetime(2015, 9, 8, 5, 10, 0),
            until_dt=datetime.datetime(2015, 9, 8, 8, 10, 0),
            vj_start_dt=datetime.datetime(2015, 9, 8, 5, 10, 0),
        )
        trip_update = TripUpdate(vj=vj, contributor=COTS_CONTRIBUTOR)
        trip_update.vj = vj
        trip_update.status = "add"
        trip_update.effect = "ADDITIONAL_SERVICE"
        trip_update.physical_mode_id = "physical_mode:LongDistanceTrain"
        real_time_update = make_rt_update(raw_data=None, connector="cots", contributor=COTS_CONTRIBUTOR)
        real_time_update.trip_updates.append(trip_update)
        st = StopTimeUpdate(
            {"id": "sa:1"},
            departure=_dt("8:15"),
            departure_delay=timedelta(minutes=5),
            arrival=None,
            arr_status="none",
            dep_status="add",
        )
        trip_update.stop_time_updates.append(st)

        st = StopTimeUpdate(
            {"id": "sa:2"},
            departure=_dt("8:21"),
            departure_delay=timedelta(minutes=-40),
            arrival=_dt("8:20"),
            arrival_delay=timedelta(minutes=-40),
            message="bob's on the track",
            arr_status="add",
            dep_status="none",
        )
        trip_update.stop_time_updates.append(st)

        db.session.add(real_time_update)
        db.session.commit()

        feed_entity = convert_to_gtfsrt(real_time_update.trip_updates)

        assert feed_entity.header.incrementality == gtfs_realtime_pb2.FeedHeader.DIFFERENTIAL
        assert feed_entity.header.gtfs_realtime_version, "1"
        assert len(feed_entity.entity) == 1
        pb_trip_update = feed_entity.entity[0].trip_update
        assert pb_trip_update.trip.trip_id == "OCE:SN:vehicle_journey:1"
        assert pb_trip_update.trip.start_date == "20150908"
        assert pb_trip_update.HasExtension(kirin_pb2.trip_message) is False
        assert pb_trip_update.trip.HasExtension(kirin_pb2.contributor) is True
        assert pb_trip_update.trip.HasExtension(kirin_pb2.company_id) is False
        assert pb_trip_update.HasExtension(kirin_pb2.effect) is True
        assert pb_trip_update.Extensions[kirin_pb2.effect] == gtfs_realtime_pb2.Alert.ADDITIONAL_SERVICE
        assert pb_trip_update.vehicle.Extensions[kirin_pb2.physical_mode_id] == "physical_mode:LongDistanceTrain"

        assert len(pb_trip_update.stop_time_update) == 2

        pb_stop_time = pb_trip_update.stop_time_update[0]
        assert pb_stop_time.stop_id == "sa:1"
        assert pb_stop_time.arrival.time == 0
        assert pb_stop_time.arrival.delay == 0
        assert pb_stop_time.departure.time == to_posix_time(_dt("8:15"))
        assert pb_stop_time.departure.delay == 5 * 60
        assert pb_stop_time.Extensions[kirin_pb2.stoptime_message] == ""
        assert pb_stop_time.arrival.Extensions[kirin_pb2.stop_time_event_status] == kirin_pb2.SCHEDULED
        assert pb_stop_time.departure.Extensions[kirin_pb2.stop_time_event_status] == kirin_pb2.ADDED

        pb_stop_time = pb_trip_update.stop_time_update[1]
        assert pb_stop_time.stop_id == "sa:2"
        assert pb_stop_time.arrival.time == to_posix_time(_dt("8:20"))
        assert pb_stop_time.arrival.delay == -40 * 60
        assert pb_stop_time.departure.time == to_posix_time(_dt("8:21"))
        assert pb_stop_time.departure.delay == -40 * 60
        assert pb_stop_time.Extensions[kirin_pb2.stoptime_message] == "bob's on the track"
        assert pb_stop_time.arrival.Extensions[kirin_pb2.stop_time_event_status] == kirin_pb2.ADDED
        assert pb_stop_time.departure.Extensions[kirin_pb2.stop_time_event_status] == kirin_pb2.SCHEDULED
