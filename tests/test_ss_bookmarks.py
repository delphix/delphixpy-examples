#!/usr/bin/env python

"""
Unit tests for Jet Stream delphixpy
"""

import sys
import unittest

import js_bookmark

from lib.GetSession import GetSession

VERSION = "0.0.0.1"


class JetStreamBookmarkTests(unittest.TestCase):
    """
    Creates, lists, shares/unshares JS Bookmarks.

    Requirements: data_layout named jstemplate3 exists on the engine.
    Change data_layout to reflect values in your environment.
    """

    @classmethod
    def setUpClass(cls):
        super(JetStreamBookmarkTests, cls).setUpClass()
        cls.server_obj = GetSession()
        cls.server_obj.serversess(
            "172.16.169.146", "delphix_admin", "delphix", "DOMAIN"
        )
        cls.server_obj.dlpx_engines["engine_name"] = "test_engine"
        cls.data_layout = "jscontainer"
        cls.branch_name = "default"
        cls.bookmark_name = "js_test_bookmark"
        js_bookmark.create_bookmark(
            cls.server_obj, cls.bookmark_name, cls.data_layout, cls.branch_name
        )

    def test_unshares_js_bookmark(self):
        js_bookmark.unshare_bookmark(self.server_obj, self.bookmark_name)
        self.assertIn(
            "{} was unshared".format(self.bookmark_name), sys.stdout.getvalue().strip()
        )

    def test_shares_js_bookmark(self):
        js_bookmark.share_bookmark(self.server_obj, self.bookmark_name)
        self.assertIn(
            "{} was shared".format(self.bookmark_name), sys.stdout.getvalue().strip()
        )

    def test_lists_js_bookmarks(self):
        js_bookmark.list_bookmarks(self.server_obj)
        self.assertIn(
            "Name, Reference, Branch".format(self.bookmark_name),
            sys.stdout.getvalue().strip(),
        )

    @classmethod
    def tearDownClass(cls):
        super(JetStreamBookmarkTests, cls).tearDownClass()
        cls.server_obj = GetSession()
        cls.server_obj.serversess(
            "172.16.169.146", "delphix_admin", "delphix", "DOMAIN"
        )
        cls.bookmark_name = "js_test_bookmark"
        js_bookmark.delete_bookmark(cls.server_obj, cls.bookmark_name)


# Run the test case
if __name__ == "__main__":
    unittest.main(module=__name__, buffer=True)
