#!/usr/bin/env python

"""
Unit tests for Jet Stream delphixpy
"""

import sys
import unittest

import js_template

from lib.GetSession import GetSession

VERSION = "0.0.0.015"


class JetStreamTemplateTests(unittest.TestCase):
    """
    Creates, lists, deletes JS Templates

    Requirements: Parent VDB named jst3.
    Change database_name to reflect values in your environment.
    """

    @classmethod
    def setUpClass(cls):
        super(JetStreamTemplateTests, cls).setUpClass()
        cls.server_obj = GetSession()
        cls.server_obj.serversess(
            "172.16.169.146", "delphix_admin", "delphix", "DOMAIN"
        )
        cls.server_obj.dlpx_engines["engine_name"] = "test_engine"
        cls.database_name = "jst3"
        cls.template_name = "js_test_template0001"
        js_template.create_template(
            cls.server_obj, cls.template_name, cls.database_name
        )

    def test_lists_js_templates(self):
        js_template.list_templates(self.server_obj)
        self.assertIn(self.template_name, sys.stdout.getvalue().strip())

    @classmethod
    def tearDownClass(cls):
        super(JetStreamTemplateTests, cls).tearDownClass()
        cls.server_obj = GetSession()
        cls.server_obj.serversess(
            "172.16.169.146", "delphix_admin", "delphix", "DOMAIN"
        )
        cls.template_name = "js_test_template0001"
        js_template.delete_template(cls.server_obj, cls.template_name)


# Run the test case
if __name__ == "__main__":
    unittest.main(module=__name__, buffer=True)
