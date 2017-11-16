import logging
import pprint
from socket import gaierror

from requests import RequestException

from lib.connector import BaseConnector, AuthenticationError
from lib.error import ConfigError

LOG = logging.getLogger("connectors/oomnitza")


class Connector(BaseConnector):
    Settings = {
        'url':       {'order': 1, 'example': "https://example.bss.com"},
        'api_token': {'order': 2, 'example': "", 'default': ""},
    }
    # no FieldMappings for oomnitza connector
    FieldMappings = {}

    def __init__(self, section, settings):
        super(Connector, self).__init__(section, settings)
        self._test_headers = []
        # self.authenticate()

    def get_field_mappings(self, extra_mappings):
        """ Override base to always return an empty mapping set.
        :param extra_mappings:
        :return: an empty dict()
        """
        return {}

    def get_headers(self):
        if self.settings['api_token']:
            return {
                'Content-Type': 'application/json; charset=utf-8',
                'Authorization': ''.join(
                    ['Bearer ', self.settings['api_token']])
            }

        return {}

    def authenticate(self):
        if not self.settings['api_token']:
                raise ConfigError("Oomnitza section needs either: api_token")

        try:
            url = "{}/api/v2/token".format(self.settings['url'])
            token = self.settings['api_token']
            data = {
                "access_token": token,
                "service_id": 1,
                "expires_at": "2018-12-22T03:12:58.019077+00:00"
            }
            response = self.post(url, data=data)
            assert response.status_code == 201
            return
        except (RequestException, AssertionError) as exp:
            if isinstance(exp.message, basestring):
                raise AuthenticationError(
                    "{} returned {}."
                    .format(self.settings['url'], exp.message))
            if len(exp.message.args) > 2 and isinstance(
                    exp.message.args[1], gaierror):
                msg = "Unable to connect to {} ({})."\
                    .format(self.settings['url'], exp.message.args[1].errno)
                if exp.message.args[1].errno == 8:
                    msg = "Unable to get address for {}."\
                        .format(self.settings['url'])
                raise AuthenticationError(msg)
            raise AuthenticationError(str(exp))

    def upload_users(self, users, options):
        url = "{url}/api/v2/bulk/users?VERSION={VERSION}"\
            .format(**self.settings)
        if 'normal_position' in options:
            url += "&normal_position={}"\
                .format(options['normal_position'])
        url += "&agent_id={}".format(options.get('agent_id', 'Unknown'))

        response = self.post(url, users)
        return response

    def upload_audit(self, computers, options):
        url = "{url}/api/v2/bulk/audit?VERSION={VERSION}"\
            .format(**self.settings)
        response = self.post(url, computers)
        return response

    @staticmethod
    def _test_upload_users(users, options):
        pprint.pprint(users)

    @staticmethod
    def _test_upload_audit(computers, options):
        pprint.pprint(computers)

    def do_test_connection(self, options):
        self.authenticate()
        assert self.settings['api_token'], "Failed to get api_token."

    @classmethod
    def example_ini_settings(cls):
        settings = super(Connector, cls).example_ini_settings()
        return settings[1:]

    # def get_mappings(self, name):
    #     url = "{0}/api/v2/service/{1}/mapping".\
    #         format(self.settings['url'], name)
    #     response = self.get(url)
    #     return response.json()

    def get_location_mappings(self, id_field, label_field):
        try:
            url = "{0}/api/v3/locations".format(self.settings['url'])
            response = self.get(url)
            mappings = {loc[label_field]: loc[id_field] for loc in response.json()
                        if loc.get(id_field, None) and loc.get(label_field, None)}
            LOG.info("Location Map to %s: External Value -> Oomnitza ID", id_field)
            for name in sorted(mappings.keys()):
                LOG.debug("    {} -> {}".format(name, mappings[name]))
            return mappings
        except:
            LOG.exception("Failed to load Locations from Oomnitza.")
            return {}

    def get_settings(self, connector, *keys):
        try:
            url = "{0}/api/v3/settings/{1}/{2}".format(
                self.settings['url'],
                connector,
                '/'.join(keys)
            )
            response = self.get(url)
            return response.json()['value']
        except:
            LOG.exception("Failed to load settings from Oomnitza.")
            raise

    def get_setting(self, key):
        try:
            url = "{0}/api/v3/settings/{1}".format(
                self.settings['url'],
                key
            )
            response = self.get(url)
            return response.json()['value']
        except:
            LOG.exception("Failed to load setting from Oomnitza.")
            raise
