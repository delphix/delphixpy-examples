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
from delphixpy.v1_6_0.exceptions import RequestError, JobError, HttpError

from lib.DlpxException import DlpxException


VERSION = 'v.0.0.001'


class GetAuthenticated(object):
    """
    Class to get the configuration and returns an Delphix authentication
    object
    """

    def __init__(self, config_file=None, server_session=None,
                 config_file_path='./dxtools.conf', delphix_engines=None,
                 f_engine_username='delphix_admin', f_engine_password=None,
                 f_engine_address=None, config=None):

        self.config_file_path = config_file_path
        self.delphix_engines = delphix_engines
        self.config_file = config_file
        self.server_session = server_session
        self.f_engine_address = f_engine_address
        self.f_engine_username = f_engine_username
        self.f_engine_password = f_engine_password
        self.config_file_path = config_file_path
        self.config = config


    def get_config(self, config_file_path):
        """
        This method reads in the dxtools.conf file
        """

        self.config_file_path = config_file_path

        #First test to see that the file is there and we can open it
        try:
            self.config_file = open(self.config_file_path).read()

        except (IOError):
            raise DlpxException('ERROR: Was unable to open %s  Please check '
                                'the path and permissions, and try again.' %
                                (self.config_file_path))

        #Now parse the file contents as json and turn them into a python
        # dictionary, throw an error if it isn't proper json
        try:
            self.config = json.loads(self.config_file)

        except (ValueError, TypeError) as e:
            raise DlpxException('ERROR: Was unable to read %s as json. Please '
                                'check if the file is in a json format and '
                                'try again.\n %s' % (self.config_file, e))

        #Create a dictionary of engines (removing the data node from the
        # dxtools.json, for easier parsing)
        self.delphix_engines = {}
        for self.each in self.config['data']:
            self.delphix_engines[self.each['hostname']] = self.each

        return self.delphix_engines


    def serversess(self, f_engine_address, f_engine_username,
                   f_engine_password):
        """
        Method to setup the session with the Delphix Engine
        """

        self.f_engine_address = f_engine_address
        self.f_engine_username = f_engine_username
        self.f_engine_password = f_engine_password

        try:
            self.server_session = DelphixEngine(self.f_engine_address,
                                                self.f_engine_username,
                                                self.f_engine_password,
                                                "DOMAIN")
            return self.server_session

        except (HttpError, RequestError, JobError) as e:
            raise DlpxException('ERROR: An error occurred while authenticating'
                                ' to %s:\n %s\n' % (self.f_engine_address, e))
