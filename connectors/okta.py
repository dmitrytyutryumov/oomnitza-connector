import errno
import json
import logging
import os

from requests import ConnectionError, HTTPError

from lib import TrueValues
from lib.connector import UserConnector

logger = logging.getLogger("connectors/okta")  # pylint:disable=invalid-name


class Connector(UserConnector):
    MappingName = 'Okta'
    Settings = {
        'url':              {'order': 1, 'default': "https://example-admin.okta.com"},
        'api_token':        {'order': 2, 'example': "YOUR Okta API TOKEN"},
        'default_role':     {'order': 3, 'example': 25, 'type': int},
        'default_position': {'order': 4, 'example': 'Employee'},
        'deprovisioned':    {'order': 5, 'default': 'false', 'example': 'false'}
    }

    FieldMappings = {}

    def __init__(self, section, settings):
        super(Connector, self).__init__(section, settings)

    def get_headers(self):
        return {
            'Content-Type': 'application/json',
            'Authorization': 'SSWS %s' % self.settings['api_token']
        }

    def do_test_connection(self, options):
        try:
            url = "{0}/api/v1/users?limit=1".format(self.settings['url'])
            response = self.get(url)
            response.raise_for_status()
            return {'result': True, 'error': ''}
        except ConnectionError as exp:
            return {'result': False, 'error': 'Connection Failed: %s' % exp.message}
        except HTTPError as exp:
            return {'result': False, 'error': 'Connection Failed: %s' % exp.message}

    def dump_data(self, name, data):
        """
        Dump data to files
        """
        if self.settings.get("__save_data__", False):
            try:
                os.makedirs("./saved_data")
            except OSError as exc:
                if exc.errno == errno.EEXIST and os.path.isdir("./saved_data"):
                    pass
                else:
                    raise
            with open("./saved_data/{}.json".format(str(name)), "w") as save_file:
                save_file.write(json.dumps(data))

    def not_deprovisioned_users_generator(self, options):
        """
        Generator returning the users with status != 'DEPROVISIONED'
        
        See the https://developer.okta.com/docs/api/resources/users.html#list-all-users for details
         """
        page = "{0}/api/v1/users?limit={1}".format(self.settings['url'], options.get('limit', 100))
        index = 0
        while page:
            response = self.get(page)
            for user in response.json():
                self.dump_data(index, user)
                index += 1
                yield user

            page = response.links.get('next', {}).get('url', None)

    def deprovisioned_users_generator(self, options):
        """
        Generator returning the users with status == 'DEPROVISIONED'

        See the https://developer.okta.com/docs/api/resources/users.html#list-users-with-a-filter for details
         """
        page = '{0}/api/v1/users?limit={1}&filter=status eq "DEPROVISIONED"'.format(self.settings['url'], options.get('limit', 100))
        index = 0
        while page:
            response = self.get(page)
            for user in response.json():
                self.dump_data(str(index) + '_DEPROVISIONED', user)
                index += 1
                yield user

            page = response.links.get('next', {}).get('url', None)

    def _load_records(self, options):
        for user in self.not_deprovisioned_users_generator(options):
            yield user

        if self.settings.get('deprovisioned') in TrueValues:
            for user in self.deprovisioned_users_generator(options):
                yield user
