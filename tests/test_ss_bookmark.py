#!/usr/bin/env python

"""
Unit tests for Self Service bookmark
"""

import unittest
import sys

import ss_bookmark
from lib.get_session import GetSession
from lib.dx_timeflow import DxTimeflow

VERSION = '0.0.0.1'

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
        cls.server_obj.dlpx_session('172.16.98.44', 'delphix_admin',
                                    'delphix', 'DOMAIN')
        cls.server_obj.dlpx_engines['engine_name'] = 'test_engine'
        cls.data_layout = 'jscontainer'
        cls.branch_name = 'default'
        cls.bookmark_name = 'ss_test_bookmark'

    def test_create_ss_bookmark(self):
        print('TEST - Create SS Bookmark')
        ss_bookmark.create_bookmark(self.server_obj, self.bookmark_name,
                                    self.data_layout, self.branch_name)

    def test_unshares_ss_bookmark(self):
        ss_bookmark.unshare_bookmark(self.server_obj, self.bookmark_name)
        self.assertIn('{} was unshared'.format(self.bookmark_name),
                      sys.stdout.getvalue().strip())

    def test_shares_ss_bookmark(self):
        ss_bookmark.share_bookmark(self.server_obj, self.bookmark_name)
        self.assertIn('{} was shared'.format(self.bookmark_name),
                      sys.stdout.getvalue().strip())

    def test_lists_ss_bookmarks(self):
        ss_bookmark.list_bookmarks(self.server_obj)
        self.assertIn('Name, Reference, Branch'.format(self.bookmark_name),
                          sys.stdout.getvalue().strip())

    @classmethod
    def tearDownClass(cls):
        super(JetStreamBookmarkTests, cls).tearDownClass()
        cls.server_obj = GetSession()
        cls.server_obj.serversess('172.16.98.44', 'delphix_admin',
                                  'delphix', 'DOMAIN')
        cls.bookmark_name = 'ss_test_bookmark'
        ss_bookmark.delete_bookmark(cls.server_obj, cls.bookmark_name)


# Run the test case
if __name__ == '__main__':
    unittest.main(module=__name__, buffer=True)
