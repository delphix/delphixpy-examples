#!/usr/bin/env python3

"""
Unit tests for DPP Timeflows
"""

import sys
import types
import unittest
from io import StringIO

from delphixpy.v1_10_2.web import database
from delphixpy.v1_10_2.web import objects
from delphixpy.v1_10_2.web import snapshot
from delphixpy.v1_10_2.web import timeflow
from delphixpy.v1_10_2.web import vo
from lib import dlpx_exceptions
from lib import get_references
from lib.dx_timeflow import DxTimeflow
from lib.get_session import GetSession

VERSION = "v.0.3.001"


class DxTimeflowTests(unittest.TestCase):
    """
    Unit tests for Timeflow

    Requirements: customize the settings under the setUpClass method.
    """

    @classmethod
    def setUpClass(cls):
        super(DxTimeflowTests, cls).setUpClass()
        cls.server_obj = GetSession()
        cls.server_obj.dlpx_session("172.16.98.44", "delphix_admin",
                                    "delphix", "DOMAIN")
        cls.server_obj.dlpx_ddps["engine_name"] = "test_engine"
        cls.dxdb = 'classic'
        cls.vdb = '12cvbd'
        cls.tf_bookmark = 'test_tm_create'
        cls.tf_params = vo.TimeflowBookmarkCreateParameters()
        cls.tf_params.timeflow_point = vo.OracleTimeflowPoint()
        cls.tf_obj = DxTimeflow(cls.server_obj.server_session)

    def _find_obj(self, f_class, obj_name):
        for obj in f_class.get_all(self.server_obj.server_session):
            if obj.name == obj_name:
                return obj
        raise dlpx_exceptions.DlpxObjectNotFound(f'Didn\'t find {obj_name}')

    def _find_obj_by_ref(self, f_class, reference):
        return f_class.get(self.server_obj.server_session, reference)
        raise dlpx_exceptions.DlpxObjectNotFound(f'Didn\'t find {reference}')

    def _find_snapshots(self, vdb_ref):
        return snapshot.get_all(self.server_obj.server_session,
                                database=vdb_ref)[0]
        raise dlpx_exceptions.DlpxObjectNotFound(f'Didn\'t find {obj_name}')

    def _create_tf_bookmark(self, bookmark_name):
        self.tf_params.name = bookmark_name
        vdb_obj = self._find_obj(database, self.dxdb)
        snapshot_obj = self._find_snapshots(vdb_obj.reference)
        self.tf_params.timeflow_point.timeflow = vdb_obj.current_timeflow
        self.tf_params.timeflow_point.timestamp = \
            snapshot_obj.latest_change_point.timestamp
        timeflow.bookmark.create(self.server_obj.server_session,
                                 self.tf_params)

    def test_get_timeflow_reference(self):
        print('TEST - Get timeflow reference')
        tf_ref = self.tf_obj.get_timeflow_reference(self.dxdb)
        self.assertIn("TIMEFLOW", tf_ref)

    def test_find_snapshots(self):
        print('TEST - Find snapshots')
        vdb_obj = self._find_obj(database, self.vdb)
        snapshot_obj = self._find_snapshots(vdb_obj.reference)
        self.assertIsInstance(self.tf_obj.find_snapshot(snapshot_obj.name),
                              str)

    def test_list_timeflows(self):
        print('TEST - List Timeflows and locations')
        tf_timeflow_objs = self.tf_obj.list_timeflows()
        self.assertIsInstance(tf_timeflow_objs, types.GeneratorType)

#    def test_create_and_delete_bookmark_by_timestamp(self):
#        print('TEST - Create TF Bookmark by timestamp')
#        bm_name = self.tf_bookmark + "_ts"
#        vdb_obj = self._find_obj(database, self.dxdb)
#        snapshot_obj = self._find_snapshots(vdb_obj.reference)
#        self.tf_obj.create_bookmark(bm_name, self.dxdb,
#                                    snapshot_obj.latest_change_point.timestamp)
#        self.tf_obj.delete_bookmark(bm_name)
#
#    def test_create_and_delete_bookmark_by_location(self):
#        print('TEST - Create TF Bookmark by location')
#        bm_name = self.tf_bookmark + '_loc'
#        vdb_obj = self._find_obj(database, self.dxdb)
#        snapshot_obj = self._find_snapshots(vdb_obj.reference)
#        self.tf_obj.create_bookmark(
#            bm_name, self.dxdb,
#            location=snapshot_obj.latest_change_point.location)
#        self.tf_obj.delete_bookmark(bm_name)

    def test_list_tf_bookmarks(self):
        print('TEST - Get timeflow bookmarks')
        tf_bookmark_objs = self.tf_obj.list_tf_bookmarks()
        self.assertIsInstance(tf_bookmark_objs, types.GeneratorType)

    def test_get_timeflow_reference(self):
        print('TEST - Get timeflow reference')
        tf_ref = self.tf_obj.get_timeflow_reference(self.dxdb)
        self.assertIsInstance(tf_ref, str)

#    def test_refresh_vdb_tf_bookmark(self):
#        print('TEST - Refresh VDB from TF Bookmark')
#        bm_name = 'test_bookmark'
#        self._create_tf_bookmark(bm_name)
#        self.tf_obj.refresh_vdb_tf_bookmark(self.vdb, bm_name)
#        self.tf_obj.delete_bookmark(bm_name)

    def test_set_timeflow_point_snapshot_latest_point(self):
        print('TEST - Set Timeflow Point by Snapshot')
        vdb_obj = get_references.find_obj_by_name(
            self.server_obj.server_session, database.database, self.vdb)
        tfp_snap = self.tf_obj.set_timeflow_point(vdb_obj, 'snapshot')
        self.assertIsInstance(
            tfp_snap, objects.TimeflowPointSemantic.TimeflowPointSemantic)

    def test_set_timeflow_point_snapshot_latest_point(self):
        print('TEST - Set Timeflow Point by Snapshot')
        vdb_obj = get_references.find_obj_by_name(
            self.server_obj.server_session, database, self.vdb)
        tfp_snap = self.tf_obj.set_timeflow_point(vdb_obj, 'snapshot')
        self.assertIsInstance(
            tfp_snap, objects.TimeflowPointSemantic.TimeflowPointSemantic)

#    def test_set_timeflow_point_named_snapshot(self):
#        print('TEST - Set Timeflow Point by Snapshot')
#        vdb_obj = self._find_obj(database, self.vdb)
#        snapshot_obj = self._find_snapshots(vdb_obj.reference)
#        vdb_obj = get_references.find_obj_by_name(
#            self.server_obj.server_session, database, self.vdb)
#        tfp_snap = self.tf_obj.set_timeflow_point(
#            vdb_obj, 'snapshot', timestamp=snapshot_obj.name)
#        self.assertIsInstance(
#            tfp_snap, objects.TimeflowPointSnapshot.TimeflowPointSnapshot)

    def test_set_timeflow_point_by_time_timeflow_name(self):
        print('TEST - Set timeflow point by time and timeflow name')
        vdb_obj = self._find_obj(database, self.vdb)
        snapshot_obj = self._find_snapshots(vdb_obj.reference)
        tf_obj = self._find_obj_by_ref(timeflow, snapshot_obj.timeflow)
        tfp_snap = self.tf_obj.set_timeflow_point(
            vdb_obj, 'time', timestamp=snapshot_obj.name,
            timeflow_name=tf_obj.name)
        self.assertIsInstance(
            tfp_snap, objects.TimeflowPointTimestamp.TimeflowPointTimestamp)

    def test_set_timeflow_point_time_latest_point(self):
        print('TEST - Set timeflow point by time latest point')
        vdb_obj = get_references.find_obj_by_name(
                      self.server_obj.server_session, database.database,
                      self.vdb)
        tfp_snap = self.tf_obj.set_timeflow_point(vdb_obj, 'time')
        self.assertIsInstance(
            tfp_snap, objects.TimeflowPointSemantic.TimeflowPointSemantic)


# Run the test case
if __name__ == "__main__":
    unittest.main(module=__name__, buffer=True)
