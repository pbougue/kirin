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
from datetime import timedelta, datetime
import pytest

from kirin import app
from kirin.core import model
from kirin.core.abstract_builder import wrap_build
from kirin.core.model import TripUpdate
from kirin.core.types import ConnectorType
from kirin.cots import KirinModelBuilder, model_maker
from kirin.cots.model_maker import ActionOnTrip
from tests.check_utils import get_fixture_data
from tests.integration.utils_cots_test import requests_mock_cause_message
from tests.integration.conftest import clean_db, COTS_CONTRIBUTOR
import json


@pytest.fixture(scope="function", autouse=True)
def mock_cause_message(requests_mock):
    """
    Mock all calls to cause message sub-service for this fixture
    """
    return requests_mock_cause_message(requests_mock)


def test_cots_train_delayed(mock_navitia_fixture):
    """
    test the import of cots_train_96231_delayed.json
    """

    input_train_delayed = get_fixture_data("cots_train_96231_delayed.json")

    with app.app_context():
        contributor = model.Contributor(
            id=COTS_CONTRIBUTOR, navitia_coverage=None, connector_type=ConnectorType.cots.value
        )
        wrap_build(KirinModelBuilder(contributor), input_train_delayed)

        trip_updates = TripUpdate.query.all()

        assert len(trip_updates) == 1
        trip_up = trip_updates[0]
        assert trip_up.vj.navitia_trip_id == "trip:OCETrainTER-87212027-85000109-3:11859"
        assert trip_up.vj_id == trip_up.vj.id
        assert trip_up.status == "update"
        assert trip_up.effect == "SIGNIFICANT_DELAYS"

        # 5 stop times must have been created
        assert len(trip_up.stop_time_updates) == 6

        # first impacted stop time should be 'gare de Sélestat'
        st = trip_up.stop_time_updates[1]
        assert st.id
        assert st.stop_id == "stop_point:OCE:SP:TrainTER-87214056"
        # the COTS data has no listeHoraireProjeteArrivee, so the status is 'none'
        assert st.arrival == datetime(2015, 9, 21, 15, 38, 0)
        assert st.arrival_delay == timedelta(minutes=0)
        assert st.arrival_status == "none"
        assert st.departure == datetime(2015, 9, 21, 15, 55, 0)
        assert st.departure_delay == timedelta(minutes=15)
        assert st.departure_status == "update"
        assert st.message == "Affluence exceptionnelle de voyageurs"

        # second impacted should be 'gare de Colmar'
        st = trip_up.stop_time_updates[2]
        assert st.id
        assert st.stop_id == "stop_point:OCE:SP:TrainTER-87182014"
        assert st.arrival == datetime(2015, 9, 21, 16, 6, 0)
        assert st.arrival_delay == timedelta(minutes=15)
        assert st.arrival_status == "update"
        assert st.departure == datetime(2015, 9, 21, 16, 8, 0)
        assert st.departure_delay == timedelta(minutes=15)
        assert st.departure_status == "update"
        assert st.message == "Affluence exceptionnelle de voyageurs"

        # last should be 'gare de Basel-SBB'
        st = trip_up.stop_time_updates[-1]
        assert st.id
        assert st.stop_id == "stop_point:OCE:SP:TrainTER-85000109"
        assert st.arrival == datetime(2015, 9, 21, 16, 54, 0)
        assert st.arrival_delay == timedelta(minutes=15)
        assert st.arrival_status == "update"
        # no departure since it's the last (thus the departure will be before the arrival)
        assert st.departure == datetime(2015, 9, 21, 16, 54, 0)
        assert st.departure_delay == timedelta(minutes=15)
        assert st.departure_status == "none"
        assert st.message == "Affluence exceptionnelle de voyageurs"


def test_cots_train_trip_removal(mock_navitia_fixture):
    """
    test the import of cots_train_6113_trip_removal.json
    """

    input_train_trip_removed = get_fixture_data("cots_train_6113_trip_removal.json")

    with app.app_context():
        contributor = model.Contributor(
            id=COTS_CONTRIBUTOR, navitia_coverage=None, connector_type=ConnectorType.cots.value
        )
        wrap_build(KirinModelBuilder(contributor), input_train_trip_removed)

        trip_updates = TripUpdate.query.all()
        assert len(trip_updates) == 1
        trip_up = trip_updates[0]
        assert trip_up.vj.navitia_trip_id == "trip:OCETGV-87686006-87751008-2:25768"
        assert trip_up.vj_id == trip_up.vj.id
        assert trip_up.status == "delete"
        # full trip removal : no stop_time to precise
        assert len(trip_up.stop_time_updates) == 0
        # verify trip_update effect:
        assert trip_up.effect == "NO_SERVICE"


def test_get_action_on_trip_add(mock_navitia_fixture):
    """
    Test the function _get_action_on_trip with different type of flux cots
    returns:
    1. Fist trip add(AJOUTEE)->  FIRST_TIME_ADDED
    2. Add followed by update (PERTURBEE) -> PREVIOUSLY_ADDED
    3. Delete followed by add -> FIRST_TIME_ADDED
    """

    with app.app_context():
        # Test for the first add: should be FIRST_TIME_ADDED
        input_trip_add = get_fixture_data("cots_train_151515_added_trip.json")
        json_data = json.loads(input_trip_add)
        dict_version = model_maker.get_value(json_data, "nouvelleVersion")
        train_numbers = model_maker.get_value(dict_version, "numeroCourse")
        pdps = model_maker._retrieve_interesting_pdp(model_maker.get_value(dict_version, "listePointDeParcours"))

        action_on_trip = model_maker._get_action_on_trip(train_numbers, dict_version, pdps)
        assert action_on_trip == ActionOnTrip.FIRST_TIME_ADDED.name

        # Test for add followed by update should be PREVIOUSLY_ADDED
        contributor = model.Contributor(
            id=COTS_CONTRIBUTOR, navitia_coverage=None, connector_type=ConnectorType.cots.value
        )
        builder = KirinModelBuilder(contributor)
        wrap_build(builder, input_trip_add)

        input_update_added_trip = get_fixture_data("cots_train_151515_added_trip_with_delay.json")
        json_data = json.loads(input_update_added_trip)
        dict_version = model_maker.get_value(json_data, "nouvelleVersion")
        train_numbers = model_maker.get_value(dict_version, "numeroCourse")
        pdps = model_maker._retrieve_interesting_pdp(model_maker.get_value(dict_version, "listePointDeParcours"))

        action_on_trip = model_maker._get_action_on_trip(train_numbers, dict_version, pdps)
        assert action_on_trip == ActionOnTrip.PREVIOUSLY_ADDED.name

        # Clean database for further test
        clean_db()

        # Delete the recently added trip followed by add: should be FIRST_TIME_ADDED
        wrap_build(builder, input_trip_add)
        input_trip_delete = get_fixture_data(
            "cots_train_151515_deleted_trip_with_delay_and_stop_time_added.json"
        )
        wrap_build(builder, input_trip_delete)

        input_added_trip = get_fixture_data("cots_train_151515_added_trip.json")
        json_data = json.loads(input_added_trip)
        dict_version = model_maker.get_value(json_data, "nouvelleVersion")
        train_numbers = model_maker.get_value(dict_version, "numeroCourse")
        pdps = model_maker._retrieve_interesting_pdp(model_maker.get_value(dict_version, "listePointDeParcours"))

        action_on_trip = model_maker._get_action_on_trip(train_numbers, dict_version, pdps)
        assert action_on_trip == ActionOnTrip.FIRST_TIME_ADDED.name
