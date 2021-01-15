#!/usr/bin/env python

"""
Unit tests for DVE authorizations
"""

import sys
import unittest

import dx_authorization

from lib.GetSession import GetSession


class DxAuthorizationTests(unittest.TestCase):
    """
    Creates, activates, lists destroys Delphix Authorizations

    Requirements: VDB named dx_vdb, group named Untitled, and user named jsuser.
    Change target_vdb, group and user to reflect values in your environment.
    """

    @classmethod
    def setUpClass(cls):
        super(DxAuthorizationTests, cls).setUpClass()
        cls.server_obj = GetSession()
        cls.server_obj.serversess(
            "172.16.169.146", "delphix_admin", "delphix", "DOMAIN"
        )
        cls.server_obj.dlpx_engines["engine_name"] = "test_engine"
        cls.user = "jsuser"
        cls.target_vdb = "dx_vdb"
        cls.group = "Untitled"
        cls.target_type_db = "database"
        cls.target_type_group = "group"
        cls.role_data = "Data"
        cls.role_read = "Read"
        cls.role_owner = "OWNER"

    def test_create_authorization_group(self):
        dx_authorization.create_authorization(
            self.server_obj,
            self.role_data,
            self.target_type_group,
            self.group,
            self.user,
        )
        self.assertIn("created for {}".format(self.user), sys.stdout.getvalue().strip())

    def test_create_authorization_database(self):
        dx_authorization.create_authorization(
            self.server_obj,
            self.role_data,
            self.target_type_db,
            self.target_vdb,
            self.user,
        )
        self.assertIn("created for {}".format(self.user), sys.stdout.getvalue().strip())

    def test_lists_dx_authorizations(self):
        dx_authorization.list_authorization(self.server_obj)
        self.assertIn("sysadmin", sys.stdout.getvalue().strip())

    @classmethod
    def tearDownClass(cls):
        super(DxAuthorizationTests, cls).tearDownClass()
        cls.server_obj = GetSession()
        cls.server_obj.serversess(
            "172.16.169.146", "delphix_admin", "delphix", "DOMAIN"
        )
        cls.user = "jsuser"
        cls.target_vdb = "dx_vdb"
        cls.group = "Untitled"
        cls.target_type_db = "database"
        cls.target_type_group = "group"
        cls.role_data = "Data"
        cls.role_read = "Read"
        cls.role_owner = "OWNER"
        dx_authorization.delete_authorization(
            cls.server_obj, cls.role_data, cls.target_type_db, cls.target_vdb, cls.user
        )
        dx_authorization.delete_authorization(
            cls.server_obj, cls.role_data, cls.target_type_group, cls.group, cls.user
        )


# Run the test case
if __name__ == "__main__":
    unittest.main(buffer=True)
