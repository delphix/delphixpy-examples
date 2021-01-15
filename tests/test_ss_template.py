#!/usr/bin/env python3

"""
Unit tests for Self Service template
"""

import io
import sys
import unittest

import ss_template

from delphixpy.v1_10_2 import exceptions
from delphixpy.v1_10_2.web import selfservice
from delphixpy.v1_10_2.web import vo
from lib.dx_timeflow import DxTimeflow
from lib.get_session import GetSession

VERSION = "0.0.0.1"


class SelfServiceTemplateTests(unittest.TestCase):
    """
    Creates, deletes, lists, restores SS Templates.

    Requirements: data_template named ss_data_template exists on the engine.
    Change data_template to reflect values in your environment.
    """

    @classmethod
    def setUpClass(cls):
        super(SelfServiceTemplateTests, cls).setUpClass()
        cls.server_obj = GetSession()
        cls.server_obj.dlpx_session(
            "172.16.98.44", "delphix_admin", "delphix", "DOMAIN"
        )
        cls.server_obj.dlpx_ddps["engine_name"] = "test_engine"
        cls.template_name = "ss_data_pod"
        cls.database_name = "ss_tmpl"
        cls.owner_name = "dev"
        cls.bookmark_name = "ss_bookmark"

    def _find_ref(self, f_class, obj_name):
        for obj in f_class.get_all(self.server_obj.server_session):
            if obj.name == obj_name:
                return obj
        raise dlpx_execptions.DlpxObjectNotFound

    def test_create_and_delete_ss_template(self):
        print("TEST - Create and Delete a SS Template")
        create_ref = ss_template.create_template(
            self.server_obj, self.template_name, self.database_name
        )
        self.assertIsInstance(create_ref, str)
        ss_template.delete_template(self.server_obj, self.template_name)

    def test_list_templates(self):
        msg = io.StringIO()
        sys.stdout = msg
        ss_template.list_templates(self.server_obj)
        sys.stdout = sys.__stdout__
        self.assertIn("Name, Reference, Active Branch", msg.getvalue())


# Run the test case
if __name__ == "__main__":
    unittest.main(module=__name__, buffer=True)
