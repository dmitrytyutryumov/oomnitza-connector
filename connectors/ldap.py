from __future__ import absolute_import

import logging
import ldap

from lib.connector import UserConnector, AuthenticationError
from lib.ext.ldap import LdapConnection


LOG = logging.getLogger("connectors/ldap_users")  # pylint:disable=invalid-name


class Connector(UserConnector):
    MappingName = 'LDAP'
    Settings = {
        'url':              {'order':  1, 'example': "ldap://ldap.forumsys.com:389"},
        'username':         {'order':  2, 'example': "cn=read-only-admin,dc=example,dc=com"},
        'password':         {'order':  3, 'default': ""},
        'base_dn':          {'order':  4, 'example': "dc=example,dc=com"},
        'group_dn':         {'order':  5, 'default': ""},
        'protocol_version': {'order':  6, 'default': "3"},
        'filter':           {'order':  7, 'default': "(objectClass=*)"},
        'default_role':     {'order':  8, 'example': 25, 'type': int},
        'default_position': {'order':  9, 'example': 'Employee'},
    }

    def __init__(self, section, settings):
        super(Connector, self).__init__(section, settings)
        fields = ()
        self.ldap_connection = LdapConnection(self.settings, fields)

    def authenticate(self):
        self.ldap_connection.authenticate()

    def do_test_connection(self, options):
        try:
            self.authenticate()
            return {'result': True, 'error': ''}
        except AuthenticationError as exp:
            return {'result': False, 'error': 'Connection Failed: %s' % (exp.message)}
        except ldap.SERVER_DOWN as exp:
            return {'result': False, 'error': 'Connection Failed: %s' % (exp.message['desc'])}
        except Exception as exp:
            return {'result': False, 'error': 'Connection Failed: %s' % exp}

    def _load_records(self, options):
        return self.ldap_connection.load_data(options)