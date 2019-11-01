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
from lib.dx_timeflow import DxTimeflow
from lib.run_job import run_job


class RunJob(unittest.TestCase):
    """
    XXXXXXXXXXXXXXXXXXXXXXX

    Requirements: XXXXXXXXXXXXXXXXXXXXXXX
    XXXXXXXXXXXXXXXXXXXXXXX
    """

    @classmethod
    def setUpClass(cls):
        super(RunJob, cls).setUpClass()
        cls.server_obj = GetSession()
        cls.server_obj.dlpx_session("172.16.98.44", "delphix_admin",
                                    "delphix", "DOMAIN")
        cls.server_obj.dlpx_engines["engine_name"] = "test_engine"

    def test_run_job_all(self):
        print('TEST - Run Job')
        ret_val = run_job(main_func, self.server_obj)
        self.assertIsInstance(ret_val, types.GeneratorType)

#    def test_find_snapshots(self):
#        print('TEST - Find snapshots')
#        snaps = self.tf_obj.find_snapshot(self.snap_name)
#        self.assertIsInstance(snaps, types.GeneratorType)
#        for snap in snaps:
#            self.assertIsInstance(snap, str)
#
#    def test_list_timeflows(self):
#        print('TEST - List Timeflows and locations')
#        tf_timeflow_objs = self.tf_obj.list_timeflows()
#        self.assertIsInstance(tf_timeflows_objs, types.GeneratorType)
#
#    def test_create_bookmark_timestamp(self):
#        print('TEST - Create TF Bookmark by timestamp')
#        bm_name = self.tf_bookmark + "_ts"
#        self.tf_obj.create_bookmark(bm_name, self.dxdb, self.timestamp)
#
#    def test_create_bookmark_location(self):
#       print('TEST - Create TF Bookmark by location')
#       bm_name = self.tf_bookmark + '_loc'
#       self.tf_obj.create_bookmark(bm_name, self.dxdb,
#                                   location=self.location)
#
#    def test_delete_bookmark(self):
#        print('TEST - Deleting TF Bookmark.')
#        bm_name = 'test_tm_create_loc'
#        self.tf_obj.delete_bookmark(bm_name)
#
#    def test_list_tf_bookmarks(self):
#        print('TEST - Get timeflow bookmarks')
#        tf_bookmark_objs = self.tf_obj.list_tf_bookmarks()
#        self.assertIsInstance(tf_bookmark_objs, types.GeneratorType)
#
#    def test_get_timeflow_reference(self):
#        print('TEST - Get timeflow reference')
#        self.tf_obj.get_timeflow_reference(self.dxdb)
#
#    def test_refresh_vdb_tf_bookmark(self):
#        print('TEST - Refresh VDB from TF Bookmark')
#        self.tf_obj.refresh_vdb_tf_bookmark(self.vdb, self.some)
#
#    def test_set_timeflow_point_snapshot_latest_point(self):
#        print('TEST - Set Timeflow Point by Snapshot')
#        vdb_obj = get_references.find_obj_by_name(
#                      self.server_obj.server_session, web.database.database,
#                      self.vdb)
#        tfp_snap = self.tf_obj.set_timeflow_point(vdb_obj, 'snapshot')
#        self.assertIsInstance(tfp_snap,
#                              web.objects.TimeflowPointSemantic.TimeflowPointSemantic)
#
#    def test_set_timeflow_point_snapshot_latest_point(self):
#        print('TEST - Set Timeflow Point by Snapshot')
#        vdb_obj = get_references.find_obj_by_name(
#                      self.server_obj.server_session, web.database.database,
#                      self.vdb)
#        tfp_snap = self.tf_obj.set_timeflow_point(vdb_obj, 'snapshot')
#        self.assertIsInstance(tfp_snap,
#                              web.objects.TimeflowPointSemantic.TimeflowPointSemantic)

def main_func():
    print('test')

# Run the test case
if __name__ == "__main__":
    unittest.main(module=__name__, buffer=True)
