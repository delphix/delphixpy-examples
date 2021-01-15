#!/usr/bin/env python

"""
Unit tests for Jet Stream delphixpy
"""

import sys
import unittest

import js_bookmark
import js_container
import js_template

from lib.GetSession import GetSession


class JetStreamContainerTests(unittest.TestCase):
    """
    Creates, lists, adds/removes users to JS Containers.

    Requirements: Parent VDB named jst3, child VDB named jst3_cld and a
    user named jsuser.
    Change template_db, database_name and owner_name to reflect values in
    your environment.
    """

    @classmethod
    def setUpClass(cls):
        super(JetStreamContainerTests, cls).setUpClass()
        cls.server_obj = GetSession()
        cls.server_obj.serversess(
            "172.16.169.146", "delphix_admin", "delphix", "DOMAIN"
        )
        cls.server_obj.dlpx_engines["engine_name"] = "test_engine"
        cls.container_name = "js_test_container0001"
        cls.database_name = "jst3_cld"
        cls.template_db = "jst3"
        cls.bookmark_name = "js_test_bookmark0001"
        cls.template_name = "js_test_template0001"

        js_template.create_template(cls.server_obj, cls.template_name, cls.template_db)
        js_container.create_container(
            cls.server_obj, cls.template_name, cls.container_name, cls.database_name
        )
        js_bookmark.create_bookmark(
            cls.server_obj, cls.bookmark_name, cls.template_name
        )

    def test_adds_removes_js_user(self):
        owner_name = "jsuser"
        js_container.add_owner(self.server_obj, owner_name, self.container_name)
        self.assertIn(owner_name, sys.stdout.getvalue().strip())

        js_container.remove_owner(self.server_obj, owner_name, self.container_name)
        self.assertIn(owner_name, sys.stdout.getvalue().strip())

    def test_lists_js_containers(self):
        js_container.list_containers(self.server_obj)
        self.assertIn(self.container_name, sys.stdout.getvalue().strip())

    def test_lists_hierarchy_js_containers(self):
        js_container.list_hierarchy(self.server_obj, self.container_name)
        self.assertIn(self.database_name, sys.stdout.getvalue().strip())

    def test_refreshes_js_containers(self):
        js_container.refresh_container(self.server_obj, self.container_name)
        self.assertIn(self.container_name, sys.stdout.getvalue().strip())

    def test_restore_js_container_to_bookmark(self):
        js_container.restore_container(
            self.server_obj, self.container_name, self.bookmark_name
        )
        self.assertIn(self.container_name, sys.stdout.getvalue().strip())

    @classmethod
    def tearDownClass(cls):
        super(JetStreamContainerTests, cls).tearDownClass()
        cls.server_obj = GetSession()
        cls.container_name = "js_test_container0001"
        cls.server_obj.serversess(
            "172.16.169.146", "delphix_admin", "delphix", "DOMAIN"
        )
        cls.container_name = "js_test_container0001"
        cls.template_name = "js_test_template0001"
        js_container.delete_container(cls.server_obj, cls.container_name, True)
        js_template.delete_template(cls.server_obj, cls.template_name)


# Run the test case
if __name__ == "__main__":
    unittest.main(buffer=True)
