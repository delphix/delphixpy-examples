#!/usr/bin/env python

"""
Unit tests for DVE operations
"""

import sys
import unittest

import dx_operations

from lib.GetSession import GetSession


class DxOperationsTests(unittest.TestCase):
    """
    Creates, activates, lists destroys Delphix Authorizations

    Requirements: VDB named dx_vdb.
    Change target_vdb to reflect values in your environment.
    """

    @classmethod
    def setUpClass(cls):
        super(DxOperationsTests, cls).setUpClass()
        cls.server_obj = GetSession()
        cls.server_obj.serversess(
            "172.16.169.146", "delphix_admin", "delphix", "DOMAIN"
        )
        cls.server_obj.dlpx_engines["engine_name"] = "test_engine"
        cls.target_vdb = "dx_vdb"

    def test_operation_functionality(self):
        operations = ["stop", "start", "disable", "enable"]
        for op in operations:
            dx_operations.dx_obj_operation(self.server_obj, self.target_vdb, op)
            self.assertIn(
                "{} was successfully".format(op), sys.stdout.getvalue().strip()
            )

    def test_lists_dx_authorizations(self):
        dx_operations.list_databases(self.server_obj)
        self.assertIn(self.target_vdb, sys.stdout.getvalue().strip())


# Run the test case
if __name__ == "__main__":
    unittest.main(module=__name__, buffer=True)
