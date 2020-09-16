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
import flask
from flask.globals import current_app
import navitia_wrapper
import logging

from kirin.abstract_sncf_resource import AbstractSNCFResource
from kirin.cots import KirinModelBuilder
from kirin.exceptions import InvalidArguments, SubServiceError
from kirin.core import model
from kirin.core.types import ConnectorType


def get_cots_contributor(include_deactivated=False):
    """
    :return 1 COTS contributor from config file or db
    File has priority over db
    TODO: Remove from config file
    """
    if "COTS_CONTRIBUTOR" in current_app.config and current_app.config.get(str("COTS_CONTRIBUTOR")):
        return model.Contributor(
            id=current_app.config.get(str("COTS_CONTRIBUTOR")),
            navitia_coverage=current_app.config.get(str("NAVITIA_INSTANCE")),
            connector_type=ConnectorType.cots.value,
            navitia_token=current_app.config.get(str("NAVITIA_TOKEN")),
        )
    else:
        contributor = model.Contributor.find_by_connector_type(
            ConnectorType.cots.value, include_deactivated=include_deactivated
        )
        if len(contributor) == 0:
            logging.getLogger(__name__).error("No COTS contributor found")
            raise SubServiceError

        if len(contributor) > 1:
            logging.getLogger(__name__).warning(
                "{n} COTS contributors found in db - {id} taken into account ".format(
                    n=len(contributor), id=contributor[0].id
                )
            )
        return contributor[0]


def get_cots(req):
    """
    get COTS stream, for the moment, it's the raw json
    """
    if not req.data:
        raise InvalidArguments("no COTS data provided")
    return req.data


class Cots(AbstractSNCFResource):
    def __init__(self):
        contributor = get_cots_contributor()
        super(Cots, self).__init__(
            navitia_wrapper.Navitia(
                url=current_app.config[str("NAVITIA_URL")], token=contributor.navitia_token
            ).instance(contributor.navitia_coverage),
            current_app.config.get(str("NAVITIA_TIMEOUT"), 5),
            contributor.id,
            KirinModelBuilder,
        )

    def post(self):
        raw_json = get_cots(flask.globals.request)
        return self.process_post(raw_json, "cots")
