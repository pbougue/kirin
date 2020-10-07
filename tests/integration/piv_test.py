# coding: utf8
#
# Copyright (c) 2001, Canal TP and/or its affiliates. All rights reserved.
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

import datetime
import threading

import pytest

from kombu import Connection, Queue
from amqp.exceptions import NotFound
from kirin import app, db
from kirin.core.model import RealTimeUpdate, TripUpdate, StopTimeUpdate, VehicleJourney
from kirin.core.types import ConnectorType
from kirin.tasks import purge_trip_update, purge_rt_update
from kirin.piv.piv import get_piv_contributor
from kirin.command.piv_worker import PivWorker
from tests.check_utils import api_post, api_get
from tests import mock_navitia
from tests.integration.conftest import PIV_CONTRIBUTOR_ID
from waiting import wait, TimeoutExpired


@pytest.fixture(scope="function", autouse=True)
def navitia(monkeypatch):
    """
    Mock all calls to navitia for this fixture
    """
    monkeypatch.setattr("navitia_wrapper._NavitiaWrapper.query", mock_navitia.mock_navitia_query)


@pytest.fixture(scope="function")
def mock_rabbitmq(monkeypatch):
    """
    Mock all publishes to navitia for this fixture
    """
    from mock import MagicMock

    mock_amqp = MagicMock()
    monkeypatch.setattr("kombu.messaging.Producer.publish", mock_amqp)

    return mock_amqp


@pytest.yield_fixture
def test_client():
    app.testing = True
    with app.app_context(), app.test_client() as tester:
        yield tester


def test_wrong_get_piv_with_id():
    """
    GET /piv/id.contributor (so with an id) is not allowed, only POST is possible
    """
    resp, status = api_get("/piv/{}".format(PIV_CONTRIBUTOR_ID), check=False)
    assert status == 405
    assert resp.get("message") == "The method is not allowed for the requested URL."


def test_piv_post_wrong_data():
    """
    simple json post on the api
    """
    wrong_piv_feed = "{}"
    res, status = api_post("/piv/{}".format(PIV_CONTRIBUTOR_ID), check=False, data=wrong_piv_feed)

    # For now, no check on data
    # TODO xfail: check and change test
    assert status == 200
    assert "PIV feed processed" in res.get("message")

    with app.app_context():
        # Raw data is saved in db, even when an error occurred
        assert len(RealTimeUpdate.query.all()) == 1
        assert len(TripUpdate.query.all()) == 0
        assert len(StopTimeUpdate.query.all()) == 0

        assert RealTimeUpdate.query.first().status == "KO"
        assert RealTimeUpdate.query.first().error == "No new information destined to navitia for this piv"
        assert RealTimeUpdate.query.first().raw_data == wrong_piv_feed


def test_piv_post_no_data():
    """
    Post with a missing id or missing data returns an error 400
    Post with an unknown id returns an error 404
    """

    def post_and_check(url, expected_status, expected_message, expected_error):
        resp, status = api_post(url, check=False)
        assert status == expected_status
        assert expected_message in resp.get("message")
        if expected_error:
            assert expected_error == resp.get("error")

        with app.app_context():
            assert len(RealTimeUpdate.query.all()) == 0
            assert len(TripUpdate.query.all()) == 0
            assert len(StopTimeUpdate.query.all()) == 0

    post_and_check("/piv/", 405, "The method is not allowed for the requested URL.", None)
    post_and_check("/piv/{}".format(PIV_CONTRIBUTOR_ID), 400, "invalid arguments", "no piv data provided")
    post_and_check("/piv/unknown_id", 404, "Contributor 'unknown_id' not found", None)


def test_piv_simple_post(mock_rabbitmq):
    """
    simple PIV post should be stored in db as a RealTimeUpdate
    """
    piv_feed = "{}"  # TODO: use a valid PIV feed
    res = api_post("/piv/{}".format(PIV_CONTRIBUTOR_ID), data=piv_feed)
    assert "PIV feed processed" in res.get("message")

    with app.app_context():
        rtu_array = RealTimeUpdate.query.all()
        assert len(rtu_array) == 1
        rtu = rtu_array[0]
        assert "-" in rtu.id
        assert rtu.created_at
        assert rtu.status == "KO"  # TODO xfail: should be "OK"
        assert rtu.error is not None  # TODO xfail: no error for a valid PIV feed
        assert rtu.contributor_id == PIV_CONTRIBUTOR_ID
        assert rtu.connector == ConnectorType.piv.value
        assert rtu.raw_data == piv_feed
    assert mock_rabbitmq.call_count == 1


def test_piv_purge(mock_rabbitmq):
    """
    Simple PIV post, then test the purge
    """
    piv_feed = "{}"  # TODO: use a valid PIV feed
    res = api_post("/piv/{}".format(PIV_CONTRIBUTOR_ID), data=piv_feed)
    assert "PIV feed processed" in res.get("message")

    with app.app_context():
        # Check there's really something before purge
        assert len(RealTimeUpdate.query.all()) == 1

        # Put an old (realistic) date to RealTimeUpdate object so that RTU purge affects it
        rtu = RealTimeUpdate.query.all()[0]
        rtu.created_at = datetime.datetime(2012, 6, 15, 15, 33)

        assert len(TripUpdate.query.all()) == 0  # TODO xfail: =1 with a valid feed and processing
        assert len(VehicleJourney.query.all()) == 0  # TODO xfail: =1
        assert len(StopTimeUpdate.query.all()) == 0  # TODO xfail: =some
        assert (
            db.session.execute("select * from associate_realtimeupdate_tripupdate").rowcount == 0
        )  # TODO xfail: =1

        # TODO VehicleJourney affected is old, so it's affected by TripUpdate purge (based on base-VJ's date)
        config = {
            "contributor": PIV_CONTRIBUTOR_ID,
            "nb_days_to_keep": int(app.config.get("NB_DAYS_TO_KEEP_TRIP_UPDATE")),
        }
        purge_trip_update(config)

        assert len(TripUpdate.query.all()) == 0
        assert len(VehicleJourney.query.all()) == 0
        assert len(StopTimeUpdate.query.all()) == 0
        assert db.session.execute("select * from associate_realtimeupdate_tripupdate").rowcount == 0
        assert len(RealTimeUpdate.query.all()) == 1  # keeping RTU longer for potential debug need

        config = {
            "nb_days_to_keep": app.config.get(str("NB_DAYS_TO_KEEP_RT_UPDATE")),
            "connector": ConnectorType.piv.value,
        }
        purge_rt_update(config)

        assert len(TripUpdate.query.all()) == 0
        assert len(VehicleJourney.query.all()) == 0
        assert len(StopTimeUpdate.query.all()) == 0
        assert db.session.execute("select * from associate_realtimeupdate_tripupdate").rowcount == 0
        assert len(RealTimeUpdate.query.all()) == 0


def test_piv_worker(test_client, pg_docker_fixture, rabbitmq_docker_fixture):
    contributor_id = "realtime.wuhan"
    exchange_name = "piv"
    queue_name = "piv_kirin"

    # Launch a PivWorker
    def piv_worker(pg_docker_fixture, contributor):
        import kirin
        from kirin import app, db, manager
        from tests.conftest import init_flask_db

        with app.app_context():
            # re-init the db by overriding the db_url
            init_flask_db(pg_docker_fixture)
            with PivWorker(contributor) as worker:
                worker.run()

    #####
    ## Init test environment
    #####

    # Create the test MQ Handler (to publish messages on the queue)
    mq_handler = rabbitmq_docker_fixture.create_rabbitmq_handler(exchange_name, "fanout")

    # Create the PIV contributor
    new_contrib = {
        "id": contributor_id,
        "navitia_coverage": "zh",
        "navitia_token": "dengdengdengdeng",
        "connector_type": ConnectorType.piv.value,
        "broker_url": rabbitmq_docker_fixture.url,
        "exchange_name": exchange_name,
        "queue_name": queue_name,
    }
    print("Setting up the PIV contributor")
    resp = test_client.post("/contributors", json=new_contrib)
    assert resp.status_code == 201

    contributor = get_piv_contributor("realtime.wuhan")
    piv_worker_thread = threading.Thread(target=piv_worker, args=(pg_docker_fixture, contributor))
    piv_worker_thread.start()
    try:
        wait(lambda: piv_worker_thread.is_alive() == True, timeout_seconds=5)
    except TimeoutExpired:
        raise AssertionError("Failed to start PivWorker in a thread")

    #####
    ## Test
    #####
    def test_queue(broker_url, queue_name):
        try:
            connection = Connection(broker_url)
            queue = Queue(queue_name, channel=connection.default_channel, no_declare=True)
            queue.queue_declare(passive=True)
            return True
        except NotFound as e:
            return False

    try:
        wait(lambda: test_queue(rabbitmq_docker_fixture.url, queue_name), timeout_seconds=5)
    except TimeoutExpired:
        raise AssertionError("Queue was never created")
    mq_handler.publish(str('{"key": "Some valid JSON"}'), contributor_id)
    try:
        wait(lambda: db.session.execute("select * from real_time_update").rowcount == 1, timeout_seconds=3)
    except TimeoutExpired:
        raise AssertionError("PIV MQ Message was never saved to database")

    #####
    ## Clean test environment
    #####
    test_client.delete("/contributors/{}".format(contributor_id))
    # PivWorker should die eventually when no PIV contributors is available
    wait(lambda: piv_worker_thread.is_alive() == False, timeout_seconds=3)
