#!/usr/bin/env python3

"""
Unit tests for Self Service bookmark
"""

import io
import sys
import unittest

import ss_bookmark

from delphixpy.v1_10_2.web import selfservice
from delphixpy.v1_10_2.web import vo
from lib.dx_timeflow import DxTimeflow
from lib.get_session import GetSession

VERSION = "0.0.0.1"


class SelfServiceBookmarkTests(unittest.TestCase):
    """
    Creates, lists, shares/unshares SS Bookmarks.

    Requirements: data_layout named sstemplate3 exists on the engine.
    Change data_layout to reflect values in your environment.
    """

    @classmethod
    def setUpClass(cls):
        super(SelfServiceBookmarkTests, cls).setUpClass()
        cls.server_obj = GetSession()
        cls.server_obj.dlpx_session(
            "172.16.98.44", "delphix_admin", "delphix", "DOMAIN"
        )
        cls.server_obj.dlpx_ddps["engine_name"] = "test_engine"
        cls.data_layout = "ss_data_pod"
        cls.branch_name = "default"
        cls.bookmark_name = "ss_test_bookmark"

    def _find_ref(self, f_class, obj_name):
        for obj in f_class.get_all(self.server_obj.server_session):
            if obj.name == obj_name:
                return obj

    def test_create_ss_bookmark(self):
        print("TEST - Create SS Bookmark")
        create_ref = ss_bookmark.create_bookmark(
            self.server_obj, self.bookmark_name, self.data_layout
        )
        self.assertIsInstance(create_ref, str)
        selfservice.bookmark.delete(self.server_obj.server_session, create_ref)

    def test_create_ss_bookmark_with_branch(self):
        print("TEST - Create SS Bookmark with branch")
        create_ref = ss_bookmark.create_bookmark(
            self.server_obj, self.bookmark_name, self.data_layout, self.branch_name
        )
        self.assertIsInstance(create_ref, str)
        selfservice.bookmark.delete(self.server_obj.server_session, create_ref)

    def test_create_ss_bookmark_with_tags(self):
        print("TEST - Create SS Bookmark with tags")
        tags = "version 123, break fix, delphix"
        create_ref = ss_bookmark.create_bookmark(
            self.server_obj, self.bookmark_name, self.data_layout, None, tags
        )
        self.assertIsInstance(create_ref, str)
        selfservice.bookmark.delete(self.server_obj.server_session, create_ref)

    def test_create_ss_bookmark_with_description(self):
        print("TEST - Create SS Bookmark with description")
        description = "unit testing - ss bookmark"
        create_ref = ss_bookmark.create_bookmark(
            self.server_obj,
            self.bookmark_name,
            self.data_layout,
            None,
            None,
            description,
        )
        self.assertIsInstance(create_ref, str)
        selfservice.bookmark.delete(self.server_obj.server_session, create_ref)

    def test_list_ss_bookmarks(self):
        msg = io.StringIO()
        sys.stdout = msg
        ss_bookmark.list_bookmarks(self.server_obj)
        sys.stdout = sys.__stdout__
        self.assertIn("Name, Reference, Branch", msg.getvalue())

    def test_unshare_ss_bookmark(self):
        msg = io.StringIO()
        sys.stdout = msg
        create_params = vo.JSBookmarkCreateParameters()
        create_params.bookmark = vo.JSBookmark()
        create_params.timeline_point_parameters = vo.JSTimelinePointLatestTimeInput()
        data_layout_obj = self._find_ref(selfservice.container, self.data_layout)
        create_params.bookmark.branch = data_layout_obj.active_branch
        create_params.bookmark.name = self.bookmark_name
        create_params.timeline_point_parameters.source_data_layout = (
            data_layout_obj.reference
        )
        create_ref = selfservice.bookmark.create(
            self.server_obj.server_session, create_params
        )
        ss_bookmark.share_bookmark(self.server_obj, self.bookmark_name)
        ss_bookmark.unshare_bookmark(self.server_obj, self.bookmark_name)
        sys.stdout = sys.__stdout__
        self.assertIn(f"{self.bookmark_name} was unshared", msg.getvalue())
        selfservice.bookmark.delete(self.server_obj.server_session, create_ref)

    def test_share_ss_bookmark(self):
        msg = io.StringIO()
        sys.stdout = msg
        create_params = vo.JSBookmarkCreateParameters()
        create_params.bookmark = vo.JSBookmark()
        create_params.timeline_point_parameters = vo.JSTimelinePointLatestTimeInput()
        data_layout_obj = self._find_ref(selfservice.template, self.data_layout)
        create_params.bookmark.branch = data_layout_obj.active_branch
        create_params.bookmark.name = self.bookmark_name
        create_params.timeline_point_parameters.source_data_layout = (
            data_layout_obj.reference
        )
        create_ref = selfservice.bookmark.create(
            self.server_obj.server_session, create_params
        )
        ss_bookmark.share_bookmark(self.server_obj, self.bookmark_name)
        sys.stdout = sys.__stdout__
        self.assertIn(f"{self.bookmark_name} was shared", msg.getvalue())
        selfservice.bookmark.delete(self.server_obj.server_session, create_ref)


# Run the test case
if __name__ == "__main__":
    unittest.main(module=__name__, buffer=True)
