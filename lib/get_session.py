#!/usr/bin/env python3
# Corey Brune - Oct 2016
# This class handles the config file and authentication to a DDP
# requirements
# pip install docopt delphixpy

"""This module takes the conf file for DDP(s) and returns an authentication
   object
"""

import json
from time import sleep

from delphixpy.v1_10_2.delphix_engine import DelphixEngine
from delphixpy.v1_10_2 import web
from delphixpy.v1_10_2 import exceptions
from delphixpy.v1_10_2 import job_context

from lib import dlpx_exceptions
from .DxLogging import print_debug
from .DxLogging import print_info

VERSION = 'v.0.3.000'


class GetSession:
    """
    Class to read configuration and returns an Delphix session object
    """

    def __init__(self):
        self.server_session = None
        self.dlpx_engines = {}
        self.jobs = {}

    def get_config(self, config_file_path="./dxtools.conf"):
        """
        This method reads in the dxtools.conf file

        :param config_file_path: path to the configuration file.
        :type config_file_path: str
        :return: dict containing engine information
        """
        # First test to see that the file is there and we can open it
        try:
            with open(config_file_path) as config_file:
                config = json.loads(config_file.read())
        except IOError:
            raise dlpx_exceptions.DlpxException(
                f'\nERROR: Was unable to open {config_file_path}. Please '
                f'check the path and permissions, and try again.\n')
        except (ValueError, TypeError, AttributeError) as err:
            raise dlpx_exceptions.DlpxException(
                f'\nERROR: Was unable to read {config_file_path} as json. '
                f'Please check if the file is in a json format and try '
                f'again.\n {err}')
        # Create a dictionary of engines (removing the data node from the
        # dxtools.json, for easier parsing)
        for each in config['data']:
            self.dlpx_engines[each['hostname']] = each

    def dlpx_session(self, f_engine_address, f_engine_username,
                     f_engine_password=None, enable_https=True):
        """
        Method to setup the session with DDP
        :param f_engine_address: The DDP's address (IP/DNS Name)
        :type f_engine_address: str
        :param f_engine_username: Username to authenticate
        :type f_engine_username: str
        :param f_engine_password: User's password
        :type f_engine_password: str
        :param enable_https: Enable or disable HTTPS
        :type enable_https: bool
        :return: delphixpy.v1_10_2.delphix_engine.DelphixEngine object
        """
        f_engine_namespace = 'DOMAIN'
        try:
            self.server_session = DelphixEngine(
                f_engine_address, f_engine_username, f_engine_password,
                f_engine_namespace, enable_https)
            self.server_wait()
        except (exceptions.HttpError, exceptions.RequestError,
                exceptions.JobError) as err:
            raise dlpx_exceptions.DlpxException(
                f'ERROR: An error occurred while authenticating to '
                f'{f_engine_address}:\n {err}\n')

    def job_mode(self, single_thread=True):
        """
        This method tells the jobs to run sync or async, based on the
        single_thread variable
        :param single_thread: Execute application synchronously (True) or
                       async (False)
                       Default: True
        :type single_thread: Bool
        :return: contextlib._GeneratorContextManager
        """
        # Synchronously
        if single_thread:
            return job_context.sync(self.server_session)
        # Or asynchronously
        elif single_thread is False:
            return job_context.async(self.server_session)

    def job_wait(self):
        """
        This job stops all work in the thread/process until jobs are
        completed.
        """
        # Grab all the jos on the server (the last 25, be default)
        all_jobs = web.job.get_all(self.server_session)
        # For each job in the list, check to see if it is running (not ended)
        for job_obj in all_jobs:
            if not (job_obj.job_state in ['CANCELED', 'COMPLETED', 'FAILED']):
                print_debug(f'\nDEBUG: Waiting for {job_obj.reference} '
                            f'(currently: {job_obj.job_state}) to finish '
                            f'running against the container.\n')
                # If so, wait
                job_context.wait(self.server_session, job_obj.reference)

    def server_wait(self):
        """
        This job waits for a successful connection to DDP.
        """
        while True:
            try:
                web.system.get(self.server_session)
                break
            except (exceptions.HttpError, exceptions.RequestError):
                pass
            print_info('Waiting for Delphix DDP to be ready')
            sleep(3)
