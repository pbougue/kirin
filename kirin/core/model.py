# coding=utf-8

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
from datetime import timedelta
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import backref, deferred
from sqlalchemy.ext.orderinglist import ordering_list
from flask_sqlalchemy import SQLAlchemy
import datetime
import sqlalchemy
from sqlalchemy import desc
from kirin.core.types import ModificationType, TripEffect, ConnectorType, DELETED_STATUSES, ADDED_STATUSES
from kirin.exceptions import ObjectNotFound, InternalException

db = SQLAlchemy()

# default name convention for db constraints (when not specified), for future alembic updates
meta = sqlalchemy.schema.MetaData(
    naming_convention={
        "ix": "ix_%(column_0_label)s",
        "uq": "uq_%(table_name)s_%(column_0_name)s",
        "ck": "ck_%(table_name)s_%(constraint_name)s",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "pk": "pk_%(table_name)s",
    }
)

DEFAULT_DAYS_TO_KEEP_TRIP_UPDATE = 3
DEFAULT_DAYS_TO_KEEP_RT_UPDATE = 30
GTFS_RT_DAYS_TO_KEEP_TRIP_UPDATE = 3
GTFS_RT_DAYS_TO_KEEP_RT_UPDATE = 10

# force the server to use UTC time for each connection checkouted from the pool
@sqlalchemy.event.listens_for(sqlalchemy.pool.Pool, "checkout")
def set_utc_on_connect(dbapi_con, connection_record, connection_proxy):
    c = dbapi_con.cursor()
    c.execute("SET timezone='utc'")
    c.close()


def gen_uuid():
    """
    Generate uuid as string
    """
    import uuid

    return str(uuid.uuid4())


class TimestampMixin(object):
    created_at = db.Column(db.DateTime(), default=datetime.datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime(), default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)


Db_TripEffect = db.Enum(*[e.name for e in TripEffect], name="trip_effect")
Db_ModificationType = db.Enum(*[t.name for t in ModificationType], name="modification_type")
Db_ConnectorType = db.Enum(*[c.value for c in ConnectorType], name="connector_type", metadata=meta)


class VehicleJourney(db.Model):  # type: ignore
    """
    Base-schedule Vehicle Journey on a given day (navitia VJ + UTC datetime of first stop)
    """

    id = db.Column(postgresql.UUID, default=gen_uuid, primary_key=True)
    navitia_trip_id = db.Column(db.Text, nullable=False)

    # timestamp of base VJ's start (stored in UTC to be safe with db without timezone)
    start_timestamp = db.Column(db.DateTime, nullable=False)
    db.Index("start_timestamp_idx", start_timestamp)

    db.UniqueConstraint(
        navitia_trip_id, start_timestamp, name="vehicle_journey_navitia_trip_id_start_timestamp_idx"
    )

    def __init__(self, navitia_vj, since_dt, until_dt, vj_start_dt=None):
        """
        Create a circulation (VJ on a given day) from:
            * the navitia VJ (circulation times without a specific day)
            * a datetime that's close but BEFORE the start of the circulation considered

        As Navitia doesn't return the start-timestamp that matches the search period (only a time, no date),
        Kirin needs to re-process it here:
        This processes start-timestamp of the circulation to be the closest one after since_dt.

                                 day:       01               02               03               04
                                hour:      00:00            00:00            00:00            00:00
          navitia VJ starts everyday:        | 02:00          | 02:00          | 02:00          | 02:00
        search period [since, until]:        |                |            [23:00       09:00]  |
                                             |                |                |   ^            |
              actual start-timestamp:        |                |                | 03T02:00       |

        :param navitia_vj: json dict of navitia's response when looking for a VJ.
        :param since_dt: naive UTC datetime BEFORE start of considered circulation,
            typically the "since" parameter of the search in navitia.
        :param until_dt: naive UTC datetime AFTER start of considered circulation,
            typically the "until" parameter of the search in navitia.
        :param vj_start_dt: naive UTC datetime of the first stop_time of vj.
        """
        if (
            since_dt.tzinfo is not None
            or until_dt.tzinfo is not None
            or (vj_start_dt is not None and vj_start_dt.tzinfo is not None)
        ):
            raise InternalException("Invalid datetime provided: must be naive (and UTC)")

        self.id = gen_uuid()
        if "trip" in navitia_vj and "id" in navitia_vj["trip"]:
            self.navitia_trip_id = navitia_vj["trip"]["id"]

        # For an added trip, we use vj_start_dt as in flux cots whereas for an existing one
        # compute start_timestamp (in UTC) from first stop_time, to be the closest AFTER provided since_dt.
        if not navitia_vj.get("stop_times", None) and vj_start_dt:
            self.start_timestamp = vj_start_dt
        else:
            first_stop_time = navitia_vj.get("stop_times", [{}])[0]
            start_time = first_stop_time["utc_arrival_time"]  # converted in datetime.time() in python wrapper
            if start_time is None:
                start_time = first_stop_time[
                    "utc_departure_time"
                ]  # converted in datetime.time() in python wrapper
            self.start_timestamp = datetime.datetime.combine(since_dt.date(), start_time)

            # if since = 20010102T2300 and start_time = 0200, actual start_timestamp = 20010103T0200.
            # So adding one day to start_timestamp obtained (20010102T0200) if it's before since_dt.
            if self.start_timestamp < since_dt:
                self.start_timestamp += timedelta(days=1)
            # simple consistency check (for now): the start timestamp must also be BEFORE until_dt
            if until_dt < self.start_timestamp:
                msg = "impossible to calculate the circulation date of vj: {} on period [{}, {}]".format(
                    navitia_vj.get("id"), since_dt, until_dt
                )
                raise ObjectNotFound(msg)

        self.navitia_vj = navitia_vj  # Not persisted

    def get_circulation_date(self):
        return self.start_timestamp.date()


class StopTimeUpdate(db.Model, TimestampMixin):  # type: ignore
    """
    Stop time
    """

    id = db.Column(postgresql.UUID, default=gen_uuid, primary_key=True)
    trip_update_id = db.Column(
        postgresql.UUID, db.ForeignKey("trip_update.vj_id", ondelete="CASCADE"), nullable=False
    )
    db.Index("trip_update_id_idx", trip_update_id)

    # stop time's order in the vj
    order = db.Column(db.Integer, nullable=False)

    stop_id = db.Column(db.Text, nullable=False)

    message = db.Column(db.Text, nullable=True)

    # Note: for departure (and arrival), we store its datetime ('departure' or 'arrival')
    # and the delay to be able to handle the base navitia schedule changes
    departure = db.Column(db.DateTime, nullable=True)
    departure_delay = db.Column(db.Interval, nullable=True)
    departure_status = db.Column(Db_ModificationType, nullable=False, default="none")

    arrival = db.Column(db.DateTime, nullable=True)
    arrival_delay = db.Column(db.Interval, nullable=True)
    arrival_status = db.Column(Db_ModificationType, nullable=False, default="none")

    def __init__(
        self,
        navitia_stop,
        departure=None,
        arrival=None,
        departure_delay=None,
        arrival_delay=None,
        dep_status="none",
        arr_status="none",
        message=None,
        order=None,
    ):
        self.id = gen_uuid()
        # Not persisted in the table stop_time_update
        self.navitia_stop = navitia_stop
        self.stop_id = navitia_stop["id"]
        self.departure_status = dep_status
        self.arrival_status = arr_status
        self.departure_delay = departure_delay
        self.arrival_delay = arrival_delay
        self.departure = departure
        self.arrival = arrival
        self.message = message
        self.order = order

    def update_departure(self, time=None, delay=None, status=None):
        if time:
            self.departure = time
        if delay is not None:
            self.departure_delay = delay
        if status:
            self.departure_status = status

    def update_arrival(self, time=None, delay=None, status=None):
        if time:
            self.arrival = time
        if delay is not None:
            self.arrival_delay = delay
        if status:
            self.arrival_status = status

    def is_equal(self, other):
        """
        we don't want to override the __eq__ function to avoid side effects
        :param other:
        :return:
        """
        return (
            self.stop_id == other.stop_id
            and self.message == other.message
            and self.order == other.order
            and self.departure == other.departure
            and self.departure_delay == other.departure_delay
            and self.departure_status == other.departure_status
            and self.arrival == other.arrival
            and self.arrival_delay == other.arrival_delay
            and self.arrival_status == other.arrival_status
        )

    def get_stop_event_status(self, event_name):
        if not hasattr(self, "{}_status".format(event_name)):
            raise Exception('StopTimeUpdate has no attribute "{}_status"'.format(event_name))
        return getattr(self, "{}_status".format(event_name), ModificationType.none.name)

    def is_stop_event_deleted(self, event_name):
        status = self.get_stop_event_status(event_name)
        return status in DELETED_STATUSES

    def is_stop_event_added(self, event_name):
        status = self.get_stop_event_status(event_name)
        return status in ADDED_STATUSES

    def is_fully_added(self, index):
        if index == 0:
            # first stop
            return self.departure_status in ADDED_STATUSES
        if index == -1:
            # last stop
            return self.arrival_status in ADDED_STATUSES
        return self.arrival_status in ADDED_STATUSES and self.departure_status in ADDED_STATUSES


associate_realtimeupdate_tripupdate = db.Table(
    "associate_realtimeupdate_tripupdate",
    db.metadata,
    db.Column("real_time_update_id", postgresql.UUID, db.ForeignKey("real_time_update.id", ondelete="CASCADE")),
    db.Column("trip_update_id", postgresql.UUID, db.ForeignKey("trip_update.vj_id", ondelete="CASCADE")),
    db.PrimaryKeyConstraint(
        "real_time_update_id", "trip_update_id", name="associate_realtimeupdate_tripupdate_pkey"
    ),
)


class TripUpdate(db.Model, TimestampMixin):  # type: ignore
    """
    Update information for Vehicle Journey
    In db, this contains a COMPLETE trip and associated RT information
    (result of all received RT feeds on base trip)
    """

    vj_id = db.Column(
        postgresql.UUID,
        db.ForeignKey("vehicle_journey.id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    )
    db.Index("vj_id_idx", vj_id)
    status = db.Column(Db_ModificationType, nullable=False, default="none")
    vj = db.relationship(
        "VehicleJourney",
        uselist=False,
        lazy="joined",
        backref=backref("trip_update", cascade="all, delete-orphan", single_parent=True),
        cascade="all, delete-orphan",
        single_parent=True,
    )
    message = db.Column(db.Text, nullable=True)
    stop_time_updates = db.relationship(
        "StopTimeUpdate",
        backref="trip_update",
        lazy="joined",
        order_by="StopTimeUpdate.order",
        collection_class=ordering_list("order"),
        cascade="all, delete-orphan",
    )
    company_id = db.Column(db.Text, nullable=True)
    effect = db.Column(Db_TripEffect, nullable=True)
    physical_mode_id = db.Column(db.Text, nullable=True)
    headsign = db.Column(db.Text, nullable=True)
    contributor_id = db.Column(db.Text, db.ForeignKey("contributor.id"), nullable=False)
    db.Index("contributor_id_idx", contributor_id)

    def __init__(
        self,
        vj,
        contributor_id,
        status="none",
        company_id=None,
        effect=None,
        physical_mode_id=None,
        headsign=None,
    ):
        self.created_at = datetime.datetime.utcnow()
        self.vj = vj
        self.status = status
        self.company_id = company_id
        self.effect = effect
        self.physical_mode_id = physical_mode_id
        self.headsign = headsign
        self.contributor_id = contributor_id

    def __repr__(self):
        return "<TripUpdate %r>" % self.vj_id

    @classmethod
    def find_by_dated_vj(cls, navitia_trip_id, start_timestamp):
        return (
            cls.query.join(VehicleJourney)
            .filter(
                VehicleJourney.navitia_trip_id == navitia_trip_id,
                VehicleJourney.start_timestamp == start_timestamp,
            )
            .first()
        )

    @classmethod
    def find_vj_by_period(cls, navitia_trip_id, start_date, end_date):
        return (
            cls.query.join(VehicleJourney)
            .filter(
                VehicleJourney.navitia_trip_id == navitia_trip_id,
                VehicleJourney.start_timestamp >= start_date,
                VehicleJourney.start_timestamp <= end_date,
            )
            .first()
        )

    @classmethod
    def find_by_dated_vjs(cls, id_timestamp_tuples):
        from sqlalchemy import tuple_

        return (
            cls.query.join(VehicleJourney)
            .filter(
                tuple_(VehicleJourney.navitia_trip_id, VehicleJourney.start_timestamp).in_(id_timestamp_tuples)
            )
            .order_by(VehicleJourney.navitia_trip_id)
            .all()
        )

    @classmethod
    def find_by_contributor_period(cls, contributors, start_date=None, end_date=None):
        query = cls.query.filter(cls.contributor_id.in_(contributors))
        if start_date:
            start_dt = datetime.datetime.combine(start_date, datetime.time(0, 0))
            query = query.filter(
                sqlalchemy.text("vehicle_journey_1.start_timestamp >= '{start_dt}'".format(start_dt=start_dt))
            )
        if end_date:
            end_dt = datetime.datetime.combine(end_date, datetime.time(0, 0))
            query = query.filter(
                sqlalchemy.text("vehicle_journey_1.start_timestamp < '{end_dt}'".format(end_dt=end_dt))
            )
        return query.all()

    @classmethod
    def remove_by_contributors_and_period(cls, contributors, start_date=None, end_date=None):
        trip_updates_to_remove = cls.find_by_contributor_period(
            contributors=contributors, start_date=start_date, end_date=end_date
        )
        for t in trip_updates_to_remove:
            f = sqlalchemy.text("associate_realtimeupdate_tripupdate.trip_update_id='{}'".format(t.vj_id))
            db.session.query(associate_realtimeupdate_tripupdate).filter(f).delete(synchronize_session=False)
            db.session.delete(t)

        db.session.commit()

    def find_stop(self, stop_id, order=None):
        # To handle a vj with the same stop served multiple times (lollipop) we search first with
        # stop_id and order.
        # For COTS, since we don't care about the order, search only with stop_id if no element found
        # Note: if the trip_update stops list is not a strict ending sublist of stops list of navitia_vj
        # then the whole trip is ignored in model_maker.
        first = next((st for st in self.stop_time_updates if st.stop_id == stop_id and st.order == order), None)
        if first:
            return first
        return next((st for st in self.stop_time_updates if st.stop_id == stop_id), None)


class RealTimeUpdate(db.Model, TimestampMixin):  # type: ignore
    """
    Real Time Update received from POST request

    This model is used to persist the raw_data: .
    A real time update object will be constructed from the raw_xml then the
    constructed real_time_update's id should be affected to TripUpdate's real_time_update_id

    There is a one-to-many relationship between RealTimeUpdate and TripUpdate.
    """

    id = db.Column(postgresql.UUID, default=gen_uuid, primary_key=True)
    connector = db.Column(Db_ConnectorType, nullable=False)
    status = db.Column(db.Enum("OK", "KO", "pending", name="rt_status"), nullable=False)
    db.Index("status_idx", status)
    error = db.Column(db.Text, nullable=True)
    raw_data = deferred(db.Column(db.Text, nullable=True))
    contributor_id = db.Column(db.Text, db.ForeignKey("contributor.id"), nullable=False)

    trip_updates = db.relationship(
        "TripUpdate",
        secondary=associate_realtimeupdate_tripupdate,
        cascade="all",
        lazy="select",
        backref=backref("real_time_updates", cascade="all"),
    )

    __table_args__ = (
        db.Index("realtime_update_created_at", "created_at"),
        db.Index("realtime_update_contributor_id_and_created_at", "created_at", "contributor_id"),
    )

    def __init__(self, raw_data, connector_type, contributor_id, status="OK", error=None):
        self.id = gen_uuid()
        self.raw_data = raw_data
        self.connector = connector_type
        self.status = status
        self.error = error
        self.contributor_id = contributor_id

    @classmethod
    def get_probes_by_contributor(cls):
        """
        create a dict of probes
        """
        result = {"last_update": {}, "last_valid_update": {}, "last_update_error": {}}

        contributor_ids = [contributor.id for contributor in Contributor.query_existing().all()]
        for c_id in contributor_ids:
            sql = db.session.query(cls.created_at, cls.status, cls.updated_at, cls.error)
            sql = sql.filter(cls.contributor_id == c_id)
            sql = sql.order_by(desc(cls.created_at))
            row = sql.first()
            if row:
                date = row.updated_at if row.updated_at else row.created_at  # update if exist, otherwise created
                result["last_update"][c_id] = date.strftime("%Y-%m-%dT%H:%M:%SZ")
                if row.status == "OK":
                    result["last_valid_update"][c_id] = row.created_at.strftime("%Y-%m-%dT%H:%M:%SZ")
                    # no error to populate
                else:
                    result["last_update_error"][c_id] = row.error
                    sql_ok = sql.filter(cls.status == "OK")
                    row_ok = sql_ok.first()
                    if row_ok:
                        result["last_valid_update"][c_id] = row_ok.created_at.strftime("%Y-%m-%dT%H:%M:%SZ")

        return result

    @classmethod
    def remove_by_contributors_until(cls, contributors, until):
        sub_query = (
            db.session.query(cls.id)
            .outerjoin(associate_realtimeupdate_tripupdate)
            .filter(cls.contributor_id.in_(contributors))
            .filter(cls.created_at < until)
            .filter(associate_realtimeupdate_tripupdate.c.real_time_update_id == None)
        )  # '==' works, not 'is'
        cls.query.filter(cls.id.in_(sub_query)).delete(synchronize_session=False)

        db.session.commit()

    @classmethod
    def get_last_rtu(cls, connector_type, contributor_id):
        q = cls.query.filter_by(connector=connector_type, contributor_id=contributor_id)
        q = q.order_by(desc(cls.created_at))
        return q.first()


class Contributor(db.Model):  # type: ignore
    """
    Contributor models a feeder for a specific coverage.
    Its ID refers to its Kraken's name (eg. 'realtime.bla')
    """

    id = db.Column(db.Text, nullable=False, primary_key=True)
    navitia_coverage = db.Column(db.Text, nullable=False)
    navitia_token = db.Column(db.Text, nullable=True)
    feed_url = db.Column(db.Text, nullable=True)
    connector_type = db.Column(Db_ConnectorType, nullable=False)
    retrieval_interval = db.Column(db.Integer, nullable=True, default=10)
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    broker_url = db.Column(db.Text, nullable=True)
    exchange_name = db.Column(db.Text, nullable=True)
    queue_name = db.Column(db.Text, nullable=True)
    nb_days_to_keep_trip_update = db.Column(db.Integer, default=DEFAULT_DAYS_TO_KEEP_TRIP_UPDATE, nullable=False)
    nb_days_to_keep_rt_update = db.Column(db.Integer, default=DEFAULT_DAYS_TO_KEEP_RT_UPDATE, nullable=False)

    def __init__(
        self,
        id,
        navitia_coverage,
        connector_type,
        navitia_token=None,
        feed_url=None,
        retrieval_interval=10,
        is_active=True,
        broker_url=None,
        exchange_name=None,
        queue_name=None,
        nb_days_to_keep_trip_update=None,
        nb_days_to_keep_rt_update=None,
    ):
        self.id = id
        self.navitia_coverage = navitia_coverage
        self.connector_type = connector_type
        self.navitia_token = navitia_token
        self.feed_url = feed_url
        self.retrieval_interval = retrieval_interval
        self.is_active = is_active
        self.broker_url = broker_url
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.nb_days_to_keep_trip_update = nb_days_to_keep_trip_update
        self.nb_days_to_keep_rt_update = nb_days_to_keep_rt_update

    @classmethod
    def find_by_connector_type(cls, type, include_deactivated=False):
        if include_deactivated:
            return cls.query.filter_by(connector_type=type).all()
        return cls.query.filter_by(connector_type=type, is_active=True).all()

    @classmethod
    def query_existing(cls):
        return cls.query.filter_by(is_active=True)
