#!/usr/bin/env python3

"""
Unit tests for DPP run_job
"""

import unittest
import types
import sys
import os
import ssl
from io import StringIO

from delphixpy.v1_10_2 import web

from lib.get_session import GetSession
from lib.dx_timeflow import DxTimeflow
from lib.run_job import run_job


class RunJob(unittest.TestCase):
    """
    Unit tests for the RunJob module

    Requirements: Customize variables under the setUpClass() method.
    """

    @classmethod
    def setUpClass(cls):
        super(RunJob, cls).setUpClass()
        if (not os.environ.get('PYTHONHTTPSVERIFY', '') and
            getattr(ssl, '_create_unverified_context', None)): 
            ssl._create_default_https_context = ssl._create_unverified_context
        cls.server_obj = GetSession()
        cls.server_obj.dlpx_session("172.16.98.44", "delphix_admin",
                                    "delphix", "DOMAIN")
        cls.server_obj.dlpx_ddps["engine_name"] = "test_engine"

    def test_run_job_all(self):
        print('TEST - Run Job All')
        ret_val = run_job(main_func, self.server_obj)
        self.assertIsInstance(ret_val, types.GeneratorType)

    def test_run_job_default_ddp(self):
        print('TEST - Run Job default DDP')
        ret_val = run_job(main_func, self.server_obj, 'default')
        self.assertIsInstance(ret_val, types.GeneratorType)

    def test_run_job_named_ddp(self):
        print('TEST - Run Job named DDP')
        ret_val = run_job(main_func, self.server_obj, 'landshark')
        self.assertIsInstance(ret_val, types.GeneratorType)

def main_func(var):
    import time
    print(f'var passed: {var}')
    time.sleep(5)

# Run the test case
if __name__ == "__main__":
    unittest.main(module=__name__, buffer=True)
