#!/usr/bin/env python
# Corey Brune - Oct 2016
# This class handles the config file and authentication to a VE
# requirements
# pip install docopt delphixpy

"""This module takes the conf file for VE(s) and returns an authentication
   object
"""

import json
import ssl
from distutils.version import LooseVersion
from time import sleep

from delphixpy.v1_8_0 import job_context
from delphixpy.v1_8_0.delphix_engine import DelphixEngine
from delphixpy.v1_8_0.exceptions import HttpError
from delphixpy.v1_8_0.exceptions import JobError
from delphixpy.v1_8_0.exceptions import RequestError
from delphixpy.v1_8_0.web import job
from delphixpy.v1_8_0.web import system
from delphixpy.v1_8_0.web.vo import SystemInfo

from .DlpxException import DlpxException
from .DxLogging import print_debug
from .DxLogging import print_info

VERSION = "v.0.2.10"


class GetSession(object):
    """
    Class to get the configuration and returns an Delphix authentication
    object
    """

    def __init__(self):
        self.server_session = None
        self.dlpx_engines = {}
        self.jobs = {}

    def __getitem__(self, key):
        return self.data[key]

    def get_config(self, config_file_path="./dxtools.conf"):
        """
        This method reads in the dxtools.conf file

        config_file_path: path to the configuration file.
                          Default: ./dxtools.conf
        """

        # config_file_path = config_file_path
        # config_file = None

        # First test to see that the file is there and we can open it
        try:
            with open(config_file_path) as config_file:

                # Now parse the file contents as json and turn them into a
                # python dictionary, throw an error if it isn't proper json
                config = json.loads(config_file.read())

        except IOError:
            raise DlpxException(
                "\nERROR: Was unable to open {}. Please "
                "check the path and permissions, and try "
                "again.\n".format(config_file_path)
            )

        except (ValueError, TypeError, AttributeError) as e:
            raise DlpxException(
                "\nERROR: Was unable to read {} as json. "
                "Please check if the file is in a json format"
                " and try again.\n {}".format(config_file_path, e)
            )

        # Create a dictionary of engines (removing the data node from the
        # dxtools.json, for easier parsing)
        for each in config["data"]:
            self.dlpx_engines[each["hostname"]] = each

    def serversess(
        self,
        f_engine_address,
        f_engine_username,
        f_engine_password,
        f_engine_namespace="DOMAIN",
    ):
        """
        Method to setup the session with the Virtualization Engine

        f_engine_address: The Virtualization Engine's address (IP/DNS Name)
        f_engine_username: Username to authenticate
        f_engine_password: User's password
        f_engine_namespace: Namespace to use for this session. Default: DOMAIN
        """

        #        if use_https:
        #            if hasattr(ssl, '_create_unverified_context'):
        #                ssl._create_default_https_context = ssl._create_unverified_context

        try:
            if f_engine_password:
                self.server_session = DelphixEngine(
                    f_engine_address,
                    f_engine_username,
                    f_engine_password,
                    f_engine_namespace,
                )
            elif f_engine_password is None:
                self.server_session = DelphixEngine(
                    f_engine_address, f_engine_username, None, f_engine_namespace
                )

        except (HttpError, RequestError, JobError) as e:
            raise DlpxException(
                "ERROR: An error occurred while authenticating"
                " to {}:\n {}\n".format(f_engine_address, e)
            )

    def job_mode(self, single_thread=True):
        """
        This method tells Delphix how to execute jobs, based on the
        single_thread variable

        single_thread: Execute application synchronously (True) or
                       async (False)
                       Default: True
        """

        # Synchronously (one at a time)
        if single_thread is True:
            print_debug("These jobs will be executed synchronously")
            return job_context.sync(self.server_session)

        # Or asynchronously
        elif single_thread is False:
            print_debug("These jobs will be executed asynchronously")
            # 5.3.5 changed the async method to asyncly, so we need to do a version check
            build_version = system.get(self.server_session).build_version
            if LooseVersion(
                "%s.%s.%s"
                % (build_version.major, build_version.minor, build_version.micro)
            ) < LooseVersion("5.3.5"):
                return job_context.asyncly(self.server_session)
            else:
                return job_context.asyncly(self.server_session)

    def job_wait(self):
        """
        This job stops all work in the thread/process until jobs are completed.

        No arguments
        """
        # Grab all the jos on the server (the last 25, be default)
        all_jobs = job.get_all(self.server_session)

        # For each job in the list, check to see if it is running (not ended)
        for jobobj in all_jobs:
            if not (jobobj.job_state in ["CANCELED", "COMPLETED", "FAILED"]):
                print_debug(
                    "\nDEBUG: Waiting for %s (currently: %s) to "
                    "finish running against the container.\n"
                    % (jobobj.reference, jobobj.job_state)
                )

                # If so, wait
                job_context.wait(self.server_session, jobobj.reference)

    def server_wait(self):
        """
        This job just waits for the Delphix Engine to be up and for a
        succesful connection.

        No arguments
        """
        while True:
            try:
                system.get(self.server_session)
                break
            except:
                pass
            print_info("Waiting for Delphix Engine to be ready")
            sleep(3)
