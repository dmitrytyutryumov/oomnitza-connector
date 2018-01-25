import errno
import json
import logging
import os
import sys

import requests
from gevent.pool import Pool
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException

from lib import TrueValues
from lib.config import get_dss_url
from utils.data import get_field_value
from .converters import Converter
from .error import ConfigError, AuthenticationError
from .filter import DynamicException
from .httpadapters import AdapterMap, retries
from .version import VERSION

LOG = logging.getLogger("lib/connector")


LastInstalledHandler = None


def run_connector(oomnitza_connector, connector, options):
    global LOG

    try:
        LOG = logging.getLogger(connector['__name__'])

        conn = connector["__connector__"]

        try:
            conn.authenticate()
        except AuthenticationError as exp:
            LOG.error("Authentication failure: %s", exp.message)
            return
        except requests.HTTPError:
            LOG.exception("Error connecting to %s service.",
                          connector['__name__'])
            return

        try:
            conn.perform_sync(oomnitza_connector, options)
        except ConfigError as exp:
            LOG.error(exp.message)
        except requests.HTTPError:
            LOG.exception("Error syncing data for %s service.",
                          connector['__name__'])
    except DynamicException as exp:
        LOG.error("Error running filter for %s: %s", connector['__name__'], exp)
    except:  # pylint:disable=broad-except
        LOG.exception("Unhandled error in run_connector for %s",
                      connector['__name__'])


def stop_connector(connector):
    try:
        conn = connector["__connector__"]
        conn.stop_sync()
    except Exception as ex:
        LOG.exception(str(ex))


class BaseConnector(object):
    Converters = {}
    MappingName = "unnamed"
    OomnitzaBatchSize = 100
    BuiltinSettings = ('ssl_protocol',)

    OomnitzaConnector = None

    CommonSettings = {
        'verify_ssl': {'order': 0, 'default': "True"},
        'cacert_file': {'order': 1, 'default': ""},
        'cacert_dir': {'order': 2, 'default': ""},
        'env_password': {'order': 3, 'default': ""},
        'ssl_protocol': {'order': 4, 'default': ""},
        'use_server_map': {'order': 5, 'default': "True"},
        'only_if_filled': {'order': 6, 'default': ""},
        'dont_overwrite': {'order': 7, 'default': ""},
        'insert_only': {'order': 8, 'default': "False"},
        'update_only': {'order': 9, 'default': "False"},
    }

    def set_dss_log(self, handler, msg, portion_id, session):
        return handler(
            msg, extra={'portion_id': portion_id, 'session': session})

    def __init__(self, section, settings):
        self.processed_records_counter = 0.
        self.sent_records_counter = 0.
        self.section = section
        self.settings = {'VERSION': VERSION}
        self.keep_going = True
        self.__filter__ = None
        self.send_counter = 0
        self._session = None

        for key, value in settings.items():
            if key == '__filter__':
                self.__filter__ = value
            else:
                # first, simple copy for internal __key__ values
                if (key.startswith('__') and key.endswith(
                        '__')) or key in self.BuiltinSettings:
                    self.settings[key] = value
                    continue

                if key in self.Settings:
                    setting = self.Settings[key]
                elif key in self.CommonSettings:
                    setting = self.CommonSettings[key]
                else:
                    # raise ConfigError("Invalid setting %r." % key)
                    LOG.warning(
                        "Invalid setting in %r section: %r." % (section, key))
                    continue

                self.settings[key] = value
        self.dss_url = get_dss_url()

        # loop over settings definitions, setting default values
        for key, setting in self.Settings.items():
            if not self.settings.get(key, None):
                default = setting.get('default', None)
                if default is None:
                    raise RuntimeError("Missing setting value for %s." % key)
                else:
                    self.settings[key] = default

        if section == 'dss' and not BaseConnector.OomnitzaConnector:
            BaseConnector.OomnitzaConnector = self

    @classmethod
    def example_ini_settings(cls):
        """
        Returns the ini settings for this connector with default and example values.
        This is used to generate the INI file.
        :return:
        """
        settings = [('enable', 'False')]
        for key, value in sorted(
                cls.Settings.items(), key=lambda t: t[1]['order']):
            if 'example' in value:
                # settings.append((key, "[{0}]".format(value['example'])))
                settings.append((key, value['example']))
            elif 'default' in value:
                settings.append((key, value['default']))
            else:
                settings.append((key, ''))
        return settings

    def _get_session(self):
        if not self._session:
            self._session = requests.Session()
            protocol = self.settings.get('ssl_protocol', "")
            if protocol:
                LOG.info("Forcing SSL Protocol to: %s", protocol)
                if protocol.lower() in AdapterMap:
                    self._session.mount("https://",
                                        AdapterMap[protocol.lower()](
                                            max_retries=retries))
                else:
                    raise RuntimeError(
                        "Invalid value for ssl_protocol: %r. Valid values are %r.",
                        protocol, list(set(AdapterMap.keys())))
            else:
                self._session.mount("https://",
                                    HTTPAdapter(max_retries=retries))

            self._session.mount("http://", HTTPAdapter(max_retries=retries))
        return self._session

    def get(self, url, headers=None, auth=None):
        """
        Performs a HTTP GET against the passed URL using either the standard or passed headers
        :param url: the full url to retrieve.
        :param headers: optional headers to override the headers from get_headers()
        :return: the response object
        """
        LOG.debug("getting url: %s", url)
        session = self._get_session()

        headers = headers or self.get_headers()
        auth = auth or self.get_auth()
        # LOG.debug("headers: %r", headers)
        response = session.get(url, headers=headers, auth=auth,
                               verify=self.get_verification())

        response.raise_for_status()
        return response

    def post(self, url, data, headers=None, auth=None, post_as_json=True):
        """
        Performs a HTTP GET against the passed URL using either the standard or passed headers
        :param url: the full url to retrieve.
        :param headers: optional headers to override the headers from get_headers()
        :return: the response object
        """
        LOG.debug("posting url: %s", url)
        session = self._get_session()

        headers = headers or self.get_headers()
        auth = auth or self.get_auth()
        if post_as_json:
            data = json.dumps(data)
        response = session.post(url, data=data, headers=headers, auth=auth,
                                verify=self.get_verification())
        response.raise_for_status()
        return response

    def get_verification(self):
        """
        Returns the value of verification.
        :return: True (Path_to_cacert in binary) / False
        """
        verify_ssl = self.settings.get('verify_ssl', True) in TrueValues
        if verify_ssl:
            if getattr(sys, 'frozen', False):
                return os.path.join(
                    getattr(sys, '_MEIPASS', os.path.abspath(".")),
                    'cacert.pem')
            else:
                return True
        else:
            return False

    def get_headers(self):
        """
        Returns the headers to be used by default in get() and post() methods
        :return: headers dict
        """
        return {}

    def get_auth(self):
        return None

    def authenticate(self):
        """
        Perform authentication to target service, if needed. Many APIs don't really support this.
        :return: Nothing
        """
        LOG.debug("%s has no authenticate() method.", self.__class__.__module__)

    def stop_sync(self):
        self.keep_going = False

    def sender(self, oomnitza_connector, options, rec):
        """
        This is data sender that should be executed by greenlet to make network IO operations non-blocking.

        :param oomnitza_connector:
        :param options:
        :param rec:
        :return:
        """

        if not (self.__filter__ is None or self.__filter__(rec)):
            LOG.info("Skipping record %r because it did not pass the filter.",
                     rec)
            return

        self.send_to_dss(oomnitza_connector, rec, options)

    def is_authorized(self):
        """
        Check if authorized
        :return:
        """
        try:
            self.authenticate()
        except AuthenticationError as exp:
            LOG.error("Authentication failed: %r.", exp.message)
            return False
        except requests.exceptions.ConnectionError as exp:
            LOG.exception("Authentication Failed: %r.", exp.message)
            return False

        return True

    def perform_sync(self, oomnitza_connector, options):
        """
        This method controls the sync process. Called from the command line script to do the work.
        :param oomnitza_connector: the Oomnitza API Connector
        :param options: right now, always {}
        :return: boolean success
        """
        if not self.is_authorized():
            return

        limit_records = float(options.get('record_count', 'inf'))

        try:
            pool_size = self.settings['__workers__']

            connection_pool = Pool(size=pool_size)
            records = self._load_records(options=options)
            connection_pool.spawn(
                self.sender, *(oomnitza_connector, options, records))

            connection_pool.join(timeout=60)  # set non-empty timeout to
            # guarantee context switching in case of threading
            return True
        except RequestException as exp:
            raise ConfigError(
                "Error loading records from %s: %s" % (
                    self.MappingName, exp.message))

    def send_to_dss(self, dss_connector, data, options=None):
        """
        Determine which method on the Oomnitza connector to call based on type of data.
        Can call:
            oomnitza_connector.(_test_)upload_assets
            oomnitza_connector.(_test_)upload_users
            oomnitza_connector.(_test_)upload_audit
        :param dss_connector: the Oomnitza connector
        :param data: the data to send (either single object or list)
        :return: the results of the Oomnitza method call
        """
        method = getattr(
            dss_connector,
            "{}upload_data".format(
                self.settings["__testmode__"] and '_test_' or ''
            )
        )
        if self.settings.get("__save_data__", False):
            try:
                try:
                    os.makedirs("./saved_data")
                except OSError as exc:
                    if exc.errno == errno.EEXIST and os.path.isdir(
                            "./saved_data"):
                        pass
                    else:
                        raise

                filename = "./saved_data/oom.payload{0:0>3}.json".format(
                    self.send_counter)
                LOG.info("Saving payload data to %s.", filename)
                with open(filename, 'w') as save_file:
                    self.send_counter += 1
                    json.dump(data, save_file, indent=2)
            except:
                LOG.exception("Error saving data.")

        result = method(data, options)
        if not self.settings["__testmode__"]:
            self.sent_records_counter += 1
        return result

    def test_connection(self, options):
        """
        Here to support GUI Test Connection button.
        :param options: currently always {}
        :return: Nothing
        """
        try:
            return self.do_test_connection(options)
        except Exception as exp:
            LOG.exception(
                "Exception running %s.test_connection()." % self.MappingName)
            return {'result': False,
                    'error': 'Test Connection Failed: %s' % exp.message}

    def do_test_connection(self, options):
        raise NotImplemented

    def _load_records(self, options):
        """
        Performs the record retrieval of the records to be imported.
        :param options: currently always {}
        :return: nothing, but yields records wither singly or in a list
        """
        raise NotImplemented

    def server_handler(self, body, wsgi_env, options):
        """
        Do the server side logic for the certain connector.
        :param wsgi_env: WSGI env dict
        :param body: request bode read from the 
        :param options:
        :return:
        """
        raise NotImplementedError

    @classmethod
    def get_field_value(cls, field, data, default=None):
        """
        Will return the field value out of data.
        Field can contain '.', which will be followed.
        :param field: the field name, can contain '.'
        :param data: the data as a dict, can contain sub-dicts
        :param default: the default value to return if field can't be found
        :return: the field value, or default.
        """
        return get_field_value(data, field, default)

    def get_setting_value(self, setting, default=None):
        """
        Nice helper to get settings.
        :param setting: the setting to return
        :param default: the default to return is the settings is not set.
        :return: the setting value, or default
        """
        return self.settings.get(setting, default)

    @classmethod
    def apply_converter(cls, converter_name, field, record, value):
        params = {}
        if ':' in converter_name:
            converter_name, args = converter_name.split(':', 1)
            for arg in args.split('|'):
                if '=' in arg:
                    k, v = arg.split('=', 1)
                else:
                    k, v = arg, True
                params[k] = v

        return Converter.run_converter(converter_name, field, record, value,
                                       params)


class UserConnector(BaseConnector):
    RecordType = 'users'

    def __init__(self, section, settings):
        super(UserConnector, self).__init__(section, settings)

        if self.settings['default_position'].lower() == 'unused':
            self.normal_position = True
        else:
            self.normal_position = False

    def send_to_dss(self, dss_connector, record, options=None):
        options['agent_id'] = self.MappingName
        if self.normal_position:
            options['normal_position'] = True

        payload = {
            "agent_id": self.MappingName,
            "data_type": self.RecordType,
            "sync_field": self.settings['sync_field'],
            "data": record,
            "insert_only": self.settings.get('insert_only', "False"),
            "update_only": self.settings.get('update_only', "False"),
            "only_if_filled": self.settings.get('only_if_filled', None),
            "dont_overwrite": self.settings.get('dont_overwrite', None),
            "options": options
        }
        return super(UserConnector, self).send_to_dss(dss_connector, payload)


class AuditConnector(BaseConnector):
    RecordType = 'audit'
    OomnitzaBatchSize = 1

    def __init__(self, section, settings):
        super(AuditConnector, self).__init__(section, settings)

    def send_to_dss(self, dss_connector, record, options=None):
        payload = {
            "agent_id": self.MappingName,
            "data_type": self.RecordType,
            "sync_field": self.settings['sync_field'],
            "data": record,
            "insert_only": self.settings.get('insert_only', "False"),
            "update_only": self.settings.get('update_only', "False"),
            "only_if_filled": self.settings.get('only_if_filled', None),
            "dont_overwrite": self.settings.get('dont_overwrite', None),
            "options": options
        }
        return super(AuditConnector, self).send_to_dss(dss_connector, payload)
