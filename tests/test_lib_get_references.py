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


class GetReferencesTests(unittest.TestCase):
    """
    XXXXXXXXXXXXXXXXXXXXXXX

    Requirements: XXXXXXXXXXXXXXXXXXXXXXX
    XXXXXXXXXXXXXXXXXXXXXXX
    """

    @classmethod
    def setUpClass(cls):
        super(GetReferencesTests, cls).setUpClass()
        cls.server_obj = GetSession()
        cls.server_obj.serversess("172.16.98.44", "delphix_admin",
                                  "delphix", "DOMAIN")
        cls.server_obj.dlpx_engines["engine_name"] = "test_engine"
        cls.zulu_timestamp = '2018-08-24T19:14:14'
        cls.db_name = 'classic'
        cls.db_reference = 'ORACLE_DB_CONTAINER-508'
        cls.env_name = 'Masking Source'
        cls.child_vdb = 'Vdlp_112'
        cls.env_ref = 'UNIX_HOST_ENVIRONMENT-18'
        cls.install_home = '/u01/app/oracle/product/11.2.0.4/dbhome_1'
        cls.sourceconfig_name = 'Vdlpxdb1_112'
        cls.group_name = 'Sources'

    def test_convert_timestamp(self):
        print('TEST - Convert Zulu timezone into DDP timezone')
        local_tz = get_references.convert_timestamp(
                   self.server_obj.server_session, self.zulu_timestamp)
        self.assertIsInstance(local_tz, str)

    def test_get_running_job(self):
        # This test requires a running job, otherwise it will fail
        print('TEST - Get Running Job')
        obj_ref = get_references.get_running_job(
                    self.server_obj.server_session, self.db_reference)
        self.assertIsInstance(obj_ref, str)

    def test_find_obj_by_name(self):
        print('TEST - Find Object by Name')
        obj_ref = get_references.find_obj_by_name(
                    self.server_obj.server_session, web.environment,
                    self.env_name)
        self.assertIsInstance(obj_ref,
                              web.objects.UnixHostEnvironment.UnixHostEnvironment)

    def test_find_source_by_dbname(self):
        print('TEST - Find source by database name')
        src_obj = get_references.find_source_by_dbname(
                   self.server_obj.server_session, self.child_vdb)
        self.assertIsInstance(src_obj,
             web.objects.OracleVirtualSource.OracleVirtualSource)

    def test_find_obj_name(self):
        print('TEST - Find object name from reference')
        obj_name = get_references.find_obj_name(
                   self.server_obj.server_session, web.database,
                   self.db_reference)
        self.assertIsInstance(obj_name, str)

    def test_find_db_repo(self):
        print('TEST - Find database repository')
        db_repo_name = get_references.find_db_repo(
                       self.server_obj.server_session, 'OracleInstall',
                       self.env_ref, self.install_home)
        self.assertIsInstance(db_repo_name,
                              web.objects.OracleInstall.OracleInstall)

    def test_find_sourceconfig(self):
        print('TEST - Find Sourceconfig')
        src_name = get_references.find_sourceconfig(
                   self.server_obj.server_session, self.sourceconfig_name,
                   self.env_ref)
        self.assertIsInstance(src_name,
                              web.objects.OracleSIConfig.OracleSIConfig)

    def test_find_all_databases_by_group_name(self):
        print('TEST - Find all databases by group name')
        obj_lst = get_references.find_all_databases_by_group_name(
                   self.server_obj.server_session, self.group_name)
        self.assertIsInstance(obj_lst, list)

# Run the test case
if __name__ == "__main__":
    unittest.main(module=__name__, buffer=True)
