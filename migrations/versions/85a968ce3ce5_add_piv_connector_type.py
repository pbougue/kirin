"""
Add 'piv' value to possible connector type

Revision ID: 85a968ce3ce5
Revises: fdc0f3db714b
Create Date: 2020-09-04 15:04:15.310092

"""
from __future__ import absolute_import, print_function, unicode_literals, division
from alembic import op

# revision identifiers, used by Alembic.
revision = "85a968ce3ce5"
down_revision = "fdc0f3db714b"


def upgrade():
    # create new type, then switch type, finally remove old type
    op.execute("ALTER TYPE connector_type RENAME TO connector_type_tmp")
    op.execute("CREATE TYPE connector_type AS ENUM('piv', 'cots', 'gtfs-rt')")  # add 'piv' here
    op.execute(
        "LOCK TABLE real_time_update; ALTER TABLE real_time_update DROP CONSTRAINT fk_real_time_update_contributor_id"
    )
    op.execute(
        "ALTER TABLE real_time_update ALTER COLUMN connector TYPE connector_type USING connector::text::connector_type"
    )
    op.execute(
        "ALTER TABLE contributor ALTER COLUMN connector_type \
            TYPE connector_type USING connector_type::text::connector_type"
    )
    op.create_foreign_key(
        "fk_real_time_update_contributor_id", "real_time_update", "contributor", ["contributor_id"], ["id"]
    )
    op.execute("DROP TYPE connector_type_tmp")


def downgrade():
    # Delete from trip_update and related tables lines related to the connector 'piv'
    op.execute(
        "DELETE FROM trip_update WHERE vj_id IN ( \
               SELECT trip_update_id FROM associate_realtimeupdate_tripupdate \
               INNER JOIN real_time_update ON (real_time_update_id = real_time_update.id) \
               WHERE connector = 'piv')"
    )

    # Delete lines in vehicle_journey without any use in trip_update
    op.execute("DELETE FROM vehicle_journey WHERE id NOT IN (SELECT vj_id FROM trip_update)")

    # Delete from real_time_update lines related to the connector 'piv'
    op.execute("DELETE FROM real_time_update WHERE connector='piv'")

    # Delete remaining 'piv' contributors
    op.execute("DELETE FROM contributor WHERE connector_type = 'piv'")

    # delete type 'piv'
    op.execute("ALTER TYPE connector_type RENAME TO connector_type_tmp")
    op.execute("CREATE TYPE connector_type AS ENUM('cots', 'gtfs-rt')")  # no more 'piv'
    op.execute(
        "LOCK TABLE real_time_update; ALTER TABLE real_time_update DROP CONSTRAINT fk_real_time_update_contributor_id"
    )
    op.execute(
        "ALTER TABLE real_time_update ALTER COLUMN connector TYPE connector_type USING connector::text::connector_type"
    )
    op.execute(
        "ALTER TABLE contributor ALTER COLUMN connector_type \
            TYPE connector_type USING connector_type::text::connector_type"
    )
    op.create_foreign_key(
        "fk_real_time_update_contributor_id", "real_time_update", "contributor", ["contributor_id"], ["id"]
    )
    op.execute("DROP TYPE connector_type_tmp")
