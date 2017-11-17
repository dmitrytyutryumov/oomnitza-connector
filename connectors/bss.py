import hashlib
import json
import logging
import pprint
from socket import gaierror

from requests import RequestException

from lib.connector import BaseConnector, AuthenticationError
from lib.error import ConfigError

LOG = logging.getLogger("connectors/oomnitza")


class Connector(BaseConnector):
    # TODO: put correct url to bss service
    Settings = {
        'url':       {'order': 1, 'example': "https://example.bss.com"},
        'api_token': {'order': 2, 'example': "", 'default': ""},
        'data_size': {'order': 3, 'example': 100000, 'default': 100000},
    }

    def __init__(self, section, settings):
        super(Connector, self).__init__(section, settings)
        self._test_headers = []
        # self.authenticate()

    def get_headers(self):
        if self.settings['api_token']:
            return {
                'Content-Type': 'application/json; charset=utf-8',
                'Authorization': ''.join(
                    ['Bearer ', self.settings['api_token']])
            }

        return {}

    # TODO: config  coorect auth
    def authenticate(self):
        if not self.settings['api_token']:
                raise ConfigError("Oomnitza section needs either: api_token")

        try:
            url = "{}/api/v2/token".format(self.settings['url'])
            token = self.settings['api_token']
            data = {
                "access_token": token,
                "service_name": "LDAP",
                "expires_at": "2019-12-22T03:12:58.019077+00:00"
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

    def upload_data(self, data, options, data_type):
        service_name = options['agent_id']
        run_id = self._get_portion(data, service_name)

        try:
            data_size = int(self.settings['data_size'])
        except ValueError:
            LOG.error('Data size needs to be a number')
            raise ConfigError('Data size needs to be a number')

        if len(data) > data_size:
            start = 0
            finish = data_size
            while start <= len(data):
                if finish > len(data):
                    finish = len(data)
                self._sent_to_bss(data[start:finish], run_id, data_type)
                start += data_size
                finish += data_size
        else:
            self._sent_to_bss(data, run_id, data_type)

        return self._close_portion(run_id)

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

    def _calculate_data_weight(self, data):
        data = [row for row in data]
        data = json.dumps(data, sort_keys=True)
        data_md5 = hashlib.md5(data.encode('utf-8')).hexdigest()
        return data_md5

    def _get_portion(self, data, service_name):
        portion_weight = self._calculate_data_weight(data)
        url = "{}/api/v2/user/{}".format(self.settings['url'], service_name)
        response = self.post(url, {	"weight": portion_weight})
        content = json.loads(response.content)
        return content['run_id']

    def _sent_to_bss(self, data, run_id, data_type):
        url = "{}/api/v2/bulk/{}/{}".format(
            self.settings['url'], data_type, run_id)
        response = self.post(url, data)
        return response

    def _close_portion(self, run_id):
        url = "{}/api/v2/{}/finished" \
            .format(self.settings['url'], run_id)
        response = self.get(url)
        return response
