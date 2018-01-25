import hashlib
import json
import logging

from requests import HTTPError

from lib.connector import BaseConnector
from lib.error import ConfigError

LOG = logging.getLogger("connectors/oomnitza")


class Connector(BaseConnector):
    # TODO: put correct url to dss service
    Settings = {
        'url': {'order': 1, 'example': "https://example.dss.com"},
        'api_token': {'order': 2, 'example': "", 'default': ""},
        'data_size': {'order': 3, 'example': 100000, 'default': 100000},
    }

    def __init__(self, section, settings):
        super(Connector, self).__init__(section, settings)
        self._test_headers = []
        self.url = self.settings['url']
        self.portion_id = None

    def get_headers(self):
        auth_token = self.settings['api_token']
        if auth_token:
            return {
                'Content-Type': 'application/json; charset=utf-8',
                'Authorization2': auth_token
            }

        return {}

    def authenticate(self):
        url = ''.join([self.dss_url, '/api/v2/token'])
        try:
            self.get(url)
            return True
        except HTTPError:
            return False

    def is_authorized(self):
        if not self._session.cookies['session']:
            return self.authenticate()
        return True

    def upload_data(self, data, options):
        record = list(data['data'])

        service_name = data['agent_id']
        data_type = data['data_type']

        service_id = self._get_service_id(service_name, data_type)
        run_id = self._get_portion(record, service_id)

        try:
            data_size = int(self.settings['data_size'])
        except ValueError:
            LOG.error('Data size needs to be a number')
            raise ConfigError('Data size needs to be a number')

        if len(record) > data_size:
            data_lenght = len(record)
            start = 0
            finish = data_size
            while start <= data_lenght:
                if finish > data_lenght:
                    finish = data_lenght

                self._send_to_dss(record[start:finish], run_id)

                start += data_size
                finish += data_size
        else:
            self._send_to_dss(record, run_id)

        LOG.info("Sent %d records to Oomnitza.", len(record))

        return self._close_portion(
            run_id, data['insert_only'], data['update_only'])

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
        data = json.dumps(data, sort_keys=True)
        data_md5 = hashlib.md5(data.encode('utf-8')).hexdigest()
        return data_md5

    def _get_portion(self, data, service_id):
        portion_weight = self._calculate_data_weight(data)
        url = "{}/api/v2/portion".format(self.dss_url)
        data = {
            "weight": portion_weight,
            "service_id": service_id
        }
        try:
            return self.post(url, data).json()['run_id']
        except:
            LOG.error('Can\'t get portion_id')
            return None

    def _send_to_dss(self, data, run_id):
        url = "{}/api/v2/bulk/{}".format(self.dss_url, run_id)
        response = self.post(url, data)
        return response

    def _close_portion(self, run_id, insert_only, update_only):
        data = {
            "insert_only": insert_only,
            "update_only": update_only,
            "run_id": run_id
        }

        url = "{}/api/v2/finished".format(self.dss_url)
        try:
            result = self.post(url, data=data)
            LOG.info("Finished! Records have been sent to Oomnitza")
            return result
        except HTTPError:
            LOG.error('Portion is wrong')

    def _dss_ping(self):
        url = ''.join([self.dss_url, '/api/v2/heartbeat'])
        response = self.get(url)
        if response.status_code == 200:
            data = response.json()
            config = data['config']

    def _get_service_id(self, service_name, data_type, one_time=None):
        url = ''.join([self.dss_url, '/api/v2/service'])
        data = {
            'service_name': service_name,
            'data_type': data_type,
            'one_time': one_time
        }
        try:
            return self.post(url, data=data).json()['service_id']
        except HTTPError:
            LOG.error('Can\'t get service_id')
