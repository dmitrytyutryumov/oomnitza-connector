from __future__ import absolute_import

import os
import logging
import ldap
import ldapurl
import json
import errno
import csv
import cStringIO
import codecs
import glob

from ldap.controls import SimplePagedResultsControl
from lib.connector import UserConnector, AuthenticationError
from lib.error import ConfigError


LOGGER = logging.getLogger("connectors/json_users")  # pylint:disable=invalid-name


class Connector(UserConnector):
    MappingName = 'JSON-users'
    Settings = {
        'file':             {'order':  1, 'example': "/Users/daniel/Documents/development/Oomnitza/Connector/test_data.json"},
        # 'directory':      {'order': 1,
        #                   'example': "/Users/daniel/Documents/development/Oomnitza/Connector/test_data"},
        'default_role':     {'order':  2, 'default': 25, 'type': int},
        'default_position': {'order':  3, 'default': 'Employee'},
        'sync_field': {'order':  4, 'default': 'USER'},
    }

    def __init__(self, section, settings):
        super(Connector, self).__init__(section, settings)

        self.is_file = True if self.settings.get('file') else False
        if self.is_file:
            self._data_store = os.path.abspath(self.settings.get('file'))
        else:
            self._data_store = os.path.abspath(self.settings.get('directory'))

    def authenticate(self):
        pass

    def do_test_connection(self, options):
        if os.path.isdir(self._data_store):
            return {'result': True, 'error': ''}
        return {
            'result': False,
            'error': '%r is not a directory.' % self._data_store
        }

    def _load_records(self, options):
        if self.is_file:
            yield self._read_json(self._data_store)
        else:
            for filename in glob.glob(os.path.join(
                    self._data_store, '*.json')):
                yield self._read_json(filename)

    def _read_json(self, filename):
        with open(filename, 'rb') as input_file:
            LOGGER.info("Processing input file: %s", filename)
            input_data = json.load(input_file)

            if isinstance(input_data, list):
                for index, user in enumerate(input_data):
                    if not isinstance(user, dict):
                        raise Exception(
                            "List item #%s is not an object!" % index)
                    return user
            elif isinstance(input_data, dict):
                return input_data
            else:
                raise Exception(
                    "File %r does not contain a list or object." % filename)
