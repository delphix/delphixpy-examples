#!/usr/bin/env python
# Corey Brune - Oct 2016
#This class handles the config file and authentication to a VE
#requirements
#pip install docopt delphixpy

"""This module takes the conf file for VE(s) and returns an authentication
   object
"""

import json

from delphixpy.v1_6_0.delphix_engine import DelphixEngine
from delphixpy.v1_6_0.exceptions import RequestError
from delphixpy.v1_6_0.exceptions import JobError
from delphixpy.v1_6_0.exceptions import HttpError

from lib.DlpxException import DlpxException


VERSION = 'v.0.0.001'


class GetSession(object):
    """
    Class to get the configuration and returns an Delphix authentication
    object
    """

    def __init__(self):
        self.server_session = None
        self.dlpx_engines = {}


    def get_config(self, config_file_path='./dxtools.conf'):
        """
        This method reads in the dxtools.conf file

        config_file_path: path to the configuration file.
                          Default: ./dxtools.conf
        """

        config_file_path = config_file_path

        #First test to see that the file is there and we can open it
        try:
            config_file = open(config_file_path).read()

            #Now parse the file contents as json and turn them into a
            #python dictionary, throw an error if it isn't proper json
            config = json.loads(config_file)

        except IOError:
            raise DlpxException('\nERROR: Was unable to open %s  Please '
                                'check the path and permissions, and try '
                                'again.\n' %
                                (config_file_path))

        except (ValueError, TypeError) as e:
            raise DlpxException('\nERROR: Was unable to read %s as json. '
                                'Please check if the file is in a json format'
                                ' and try again.\n %s' %
                                (config_file, e))

        #Create a dictionary of engines (removing the data node from the
        # dxtools.json, for easier parsing)
        for each in config['data']:
            self.dlpx_engines[each['hostname']] = each


    def serversess(self, f_engine_address, f_engine_username,
                   f_engine_password, f_engine_namespace='DOMAIN'):
        """
        Method to setup the session with the Virtualization Engine

        f_engine_address: The Virtualization Engine's address (IP/DNS Name)
        f_engine_username: Username to authenticate
        f_engine_password: User's password
        f_engine_namespace: Namespace to use for this session. Default: DOMAIN
        """

        try:
            if f_engine_password:
                self.server_session = DelphixEngine(f_engine_address,
                                                    f_engine_username,
                                                    f_engine_password,
                                                    f_engine_namespace)
            elif f_engine_password is None:
                self.server_session = DelphixEngine(f_engine_address,
                                                    f_engine_username,
                                                    None, f_engine_namespace)

        except (HttpError, RequestError, JobError) as e:
            raise DlpxException('ERROR: An error occurred while authenticating'
                                ' to %s:\n %s\n' % (f_engine_address, e))
