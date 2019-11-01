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
from lib import get_references


class RunJobs(unittest.TestCase):
    """
    XXXXXXXXXXXXXXXXXXXXXXX

    Requirements: XXXXXXXXXXXXXXXXXXXXXXX
    XXXXXXXXXXXXXXXXXXXXXXX
    """

    @classmethod
    def setUpClass(cls):
        super(DxTimeflowTests, cls).setUpClass()
        cls.server_obj = GetSession()
        cls.server_obj.dlpx_session("172.16.98.44", "delphix_admin",
                                    "delphix", "DOMAIN")
        cls.server_obj.dlpx_engines["engine_name"] = "test_engine"
        cls.tf_obj = DxTimeflow(cls.server_obj.server_session)
        cls.dxdb = 'classic'
        cls.snap_name = '@2018-08-24T19:14:38.149Z'
        cls.timestamp = '2018-08-24T19:14:14.000Z'
        cls.tf_name = 'DB_REFRESH@2019-07-16T20:56:42'
        cls.tf_bookmark = 'test_tm_create'
        cls.location = '24114144'
        cls.vdb = '12cvbd'
        cls.some = 'test_tm_create_loc'

    def test_run_job(self):
        print('TEST - Get timeflow reference')
        tf_ref = self.tf_obj.get_timeflow_reference(self.dxdb)
        self.assertIn("TIMEFLOW", tf_ref)

    def test_find_snapshots(self):
        print('TEST - Find snapshots')
        snaps = self.tf_obj.find_snapshot(self.snap_name)
        self.assertIsInstance(snaps, types.GeneratorType)
        for snap in snaps:
            self.assertIsInstance(snap, str)

    def test_list_timeflows(self):
        print('TEST - List Timeflows and locations')
        tf_timeflow_objs = self.tf_obj.list_timeflows()
        self.assertIsInstance(tf_timeflows_objs, types.GeneratorType)

    def test_create_bookmark_timestamp(self):
        print('TEST - Create TF Bookmark by timestamp')
        bm_name = self.tf_bookmark + "_ts"
        self.tf_obj.create_bookmark(bm_name, self.dxdb, self.timestamp)

    def test_create_bookmark_location(self):
       print('TEST - Create TF Bookmark by location')
       bm_name = self.tf_bookmark + '_loc'
       self.tf_obj.create_bookmark(bm_name, self.dxdb,
                                   location=self.location)

    def test_delete_bookmark(self):
        print('TEST - Deleting TF Bookmark.')
        bm_name = 'test_tm_create_loc'
        self.tf_obj.delete_bookmark(bm_name)

    def test_list_tf_bookmarks(self):
        print('TEST - Get timeflow bookmarks')
        tf_bookmark_objs = self.tf_obj.list_tf_bookmarks()
        self.assertIsInstance(tf_bookmark_objs, types.GeneratorType)

    def test_get_timeflow_reference(self):
        print('TEST - Get timeflow reference')
        self.tf_obj.get_timeflow_reference(self.dxdb)

    def test_refresh_vdb_tf_bookmark(self):
        print('TEST - Refresh VDB from TF Bookmark')
        self.tf_obj.refresh_vdb_tf_bookmark(self.vdb, self.some)

    def test_set_timeflow_point_snapshot_latest_point(self):
        print('TEST - Set Timeflow Point by Snapshot')
        vdb_obj = get_references.find_obj_by_name(
                      self.server_obj.server_session, web.database.database,
                      self.vdb)
        tfp_snap = self.tf_obj.set_timeflow_point(vdb_obj, 'snapshot')
        self.assertIsInstance(tfp_snap,
                              web.objects.TimeflowPointSemantic.TimeflowPointSemantic)

    def test_set_timeflow_point_snapshot_latest_point(self):
        print('TEST - Set Timeflow Point by Snapshot')
        vdb_obj = get_references.find_obj_by_name(
                      self.server_obj.server_session, web.database.database,
                      self.vdb)
        tfp_snap = self.tf_obj.set_timeflow_point(vdb_obj, 'snapshot')
        self.assertIsInstance(tfp_snap,
                              web.objects.TimeflowPointSemantic.TimeflowPointSemantic)

    def test_set_timeflow_point_named_snapshot(self):
        print('TEST - Set Timeflow Point by Snapshot')
        vdb_obj = get_references.find_obj_by_name(
                      self.server_obj.server_session, web.database.database,
                      self.vdb)
        tfp_snap = self.tf_obj.set_timeflow_point(vdb_obj, 'snapshot',
                                                  timestamp=self.snap_name)
        self.assertIsInstance(tfp_snap,
                              web.objects.TimeflowPointSnapshot.TimeflowPointSnapshot)

    def test_set_timeflow_point_by_time_timeflow_name(self):
        print('TEST - Set timeflow point by time and timeflow name')
        vdb_obj = get_references.find_obj_by_name(
                      self.server_obj.server_session, web.database.database,
                      self.vdb)
        tfp_snap = self.tf_obj.set_timeflow_point(
                       vdb_obj, 'time', timestamp=self.timestamp,
                       timeflow_name=self.tf_name)
        self.assertIsInstance(
            tfp_snap, web.objects.TimeflowPointTimestamp.TimeflowPointTimestamp)

    def test_set_timeflow_point_time_latest_point(self):
        print('TEST - Set timeflow point by time latest point')
        vdb_obj = get_references.find_obj_by_name(
                      self.server_obj.server_session, web.database.database,
                      self.vdb)
        tfp_snap = self.tf_obj.set_timeflow_point(vdb_obj, 'time')

        print(type(tfp_snap), '\n\n')
        self.assertIsInstance(tfp_snap,
                              web.objects.TimeflowPointSemantic.TimeflowPointSemantic)



# Run the test case
if __name__ == "__main__":
    unittest.main(module=__name__, buffer=True)
