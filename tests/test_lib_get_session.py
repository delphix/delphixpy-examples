#!/usr/bin/env python3

"""
Unit tests for DPP GetSession
"""

import os
import ssl
import sys
import types
import unittest
from io import StringIO

from delphixpy.v1_10_2.web import database
from lib import get_references
from lib.get_session import GetSession


class GetSessionTests(unittest.TestCase):
    """
    Unit test for GetSession

    Requirements: Customize variables under the setUpClass() method.
    """
    @classmethod
    def setUpClass(cls):
        super(GetSessionTests, cls).setUpClass()
        cls.server_obj = GetSession()
        cls.engine_ip = '172.16.98.44'
        cls.engine_user = 'delphix_admin'
        cls.engine_pass = 'delphix'

    def test_get_config(self):
        print('TEST - Get config')
        self.server_obj.get_config()
        self.assertNotEqual(0, len(self.server_obj.dlpx_ddps))

    def test_server_session(self):
        print('TEST - Server session')
        for engine in self.server_obj.dlpx_ddps.keys():
            ddps_dct = self.server_obj.dlpx_ddps[engine].pop()
            self.server_obj.dlpx_session(ddps_dct['ip_address'],
                                         ddps_dct['username'],
                                         ddps_dct['password'],
                                         ddps_dct['use_https'])

    def test_job_mode_sync(self):
        print('TEST - Job mode sync')
        self.server_obj.dlpx_session(self.engine_ip, self.engine_user,
                                     self.engine_pass, "DOMAIN")
        with self.server_obj.job_mode():
            database.get_all(self.server_obj.server_session)

    def test_job_mode_async(self):
        print('TEST - Job mode async')
        self.server_obj.dlpx_session(self.engine_ip, self.engine_user,
                                     self.engine_pass, "DOMAIN")
        with self.server_obj.job_mode(False):
            database.get_all(self.server_obj.server_session)

    def test_server_wait(self):
        print('TEST - Server wait')
        self.server_obj.dlpx_session(self.engine_ip, self.engine_user,
                                     self.engine_pass, "DOMAIN")
        self.server_obj.server_wait()

# Run the unit tests
if __name__ == "__main__":
    unittest.main(module=__name__, buffer=True)
