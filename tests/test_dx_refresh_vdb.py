#!/usr/bin/env python3

"""
Unit tests for VDB refresh
"""

import io
import sys
import unittest

import dx_refresh_vdb

from delphixpy.v1_10_2 import exceptions
from delphixpy.v1_10_2.web import selfservice
from delphixpy.v1_10_2.web import vo
from lib.dx_timeflow import DxTimeflow
from lib.get_session import GetSession

VERSION = "0.0.0.1"


class DxVDBRefresh(unittest.TestCase):
    """
    Refreshes a VDB or a group.

    Requirements:
    """

    @classmethod
    def setUpClass(cls):
        super(DxVDBRefresh, cls).setUpClass()
        cls.server_obj = GetSession()
        cls.server_obj.dlpx_session(
            "172.16.98.44", "delphix_admin", "delphix", "DOMAIN"
        )
        cls.server_obj.dlpx_ddps["engine_name"] = "test_engine"
        cls.database_name = "12cvdb"
        cls.vdb_name = "ss_te"

    def _find_ref(self, f_class, obj_name):
        for obj in f_class.get_all(self.server_obj.server_session):
            if obj.name == obj_name:
                return obj
        raise dlpx_execptions.DlpxObjectNotFound

    def test_refresh_vdb_latest(self):
        print("TEST - Refresh VDB Latest")
        dx_refresh_vdb.refresh_vdb(self.server_obj, self.vdb_name, "LATEST")


#    def test_refresh_all_vdbs_latest(self):
#        print('TEST - Refresh VDB Latest')
#        dx_refresh_vdb.refresh_vdb(self.server_obj, self.vdb_name, 'LATEST')

#    def test_refresh_all_vdbs(self):
#        print('TEST - Refresh all VDBs')
#        create_ref = dx_refresh_vdb.refresh_vdb(
#            self.server_obj, self.template_name, self.database_name)
#        self.assertIsInstance(create_ref, str)
#        ss_template.delete_template(self.server_obj, self.template_name)

#    def test_list_timeflows(self):
#        msg = io.StringIO()
#        sys.stdout = msg
#        ss_template.list_timeflows(self.server_obj)
#        sys.stdout = sys.__stdout__
#        self.assertIn('Name, Reference, Active Branch', msg.getvalue())

#    def test_list_snapshots(self):
#        msg = io.StringIO()
#        sys.stdout = msg
#        ss_template.list_snapshots(self.server_obj)
#        sys.stdout = sys.__stdout__
#        self.assertIn('Name, Reference, Active Branch', msg.getvalue())

# Run the test case
if __name__ == "__main__":
    unittest.main(module=__name__, buffer=True)
