#!/usr/bin/env python

"""
Unit tests for Jet Stream delphixpy
"""

import sys
import unittest

import js_branch
import js_container
import js_template

from lib.GetSession import GetSession


class JetStreamBranchTests(unittest.TestCase):
    """
    Creates, activates, lists destroys JS Branches

    Requirements: Parent VDB named jst3, and child VDB named jst3_cld.
    Change template_db and database_name to reflect values in your environment.
    """

    @classmethod
    def setUpClass(cls):
        super(JetStreamBranchTests, cls).setUpClass()
        cls.server_obj = GetSession()
        cls.server_obj.serversess(
            "172.16.169.146", "delphix_admin", "delphix", "DOMAIN"
        )
        cls.server_obj.dlpx_engines["engine_name"] = "test_engine"
        cls.container_name = "js_test_container0001"
        cls.branch_name = "js_test_branch0001"
        cls.template_name = "js_test_template0001"
        cls.template_db = "jst3"
        cls.database_name = "jst3_cld"
        js_template.create_template(cls.server_obj, cls.template_name, cls.template_db)
        js_container.create_container(
            cls.server_obj, cls.template_name, cls.container_name, cls.database_name
        )
        js_branch.create_branch(
            cls.server_obj, cls.branch_name, cls.template_name, cls.container_name
        )

    def test_activate_js_branch(self):
        original_branch = "default"
        js_branch.activate_branch(self.server_obj, original_branch)
        self.assertIn(original_branch, sys.stdout.getvalue().strip())

    def test_lists_js_branches(self):
        js_branch.list_branches(self.server_obj)
        self.assertIn(
            "Branch Name, Data Layout".format(self.branch_name),
            sys.stdout.getvalue().strip(),
        )

    @classmethod
    def tearDownClass(cls):
        super(JetStreamBranchTests, cls).tearDownClass()
        cls.server_obj = GetSession()
        cls.server_obj.serversess(
            "172.16.169.146", "delphix_admin", "delphix", "DOMAIN"
        )
        cls.branch_name = "js_test_branch0001"
        cls.container_name = "js_test_container0001"
        cls.template_name = "js_test_template0001"
        js_branch.delete_branch(cls.server_obj, cls.branch_name)
        js_container.delete_container(cls.server_obj, cls.container_name, True)
        js_template.delete_template(cls.server_obj, cls.template_name)


# Run the test case
if __name__ == "__main__":
    unittest.main(buffer=True)
