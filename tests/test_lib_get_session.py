#!/usr/bin/env python3

"""
Unit tests for DPP Timeflows
"""

import unittest
import types
from io import StringIO
import sys

from delphixpy.v1_10_2 import web

from lib.get_session import GetSession
from lib import get_references


class GetSessionTests(unittest.TestCase):
    """
    XXXXXXXXXXXXXXXXXXXXXXX

    Requirements: XXXXXXXXXXXXXXXXXXXXXXX
    XXXXXXXXXXXXXXXXXXXXXXX
    """
    @classmethod
    def setUpClass(cls):
        super(GetSessionTests, cls).setUpClass()
        cls.server_obj = GetSession()

    def test_get_config(self):
        print('TEST - Get config')
        self.server_obj.get_config()
        self.assertNotEqual(0, len(self.server_obj.dlpx_engines))

    def test_server_session(self):
        print('TEST - Server session')
        for engine in self.server_obj.dlpx_engines.keys():
            self.server_obj.dlpx_session(
               self.server_obj.dlpx_engines[engine]['ip_address'],
               self.server_obj.dlpx_engines[engine]['username'],
               self.server_obj.dlpx_engines[engine]['password'],
               self.server_obj.dlpx_engines[engine]['use_https'])
            self.assertIsNotNone(self.server_obj.dlpx_engines[engine])

    def test_job_mode_sync(self):
        print('TEST - Job mode sync')
        self.server_obj.job_mode()
        with self.server_obj:
            web.database.get_all(self.server_obj)

    def test_job_mode_async(self):
        print('TEST - Job mode async')
        self.server_obj.job_mode(False)
        with self.server_obj:
            web.database.get_all(self.server_obj)

# Run the test case
if __name__ == "__main__":
    unittest.main(module=__name__, buffer=True)
