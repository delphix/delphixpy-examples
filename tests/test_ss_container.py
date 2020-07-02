#!/usr/bin/env python3

"""
Unit tests for Self Service container
"""

import unittest
import sys
import io

from delphixpy.v1_10_2.web import vo
from delphixpy.v1_10_2 import exceptions
from delphixpy.v1_10_2.web import selfservice

import ss_container
from lib.get_session import GetSession
from lib.dx_timeflow import DxTimeflow

VERSION = '0.0.0.1'

class SelfServiceContainerTests(unittest.TestCase):
    """
    Creates, deletes, lists, restores SS Containers.

    Requirements: data_template named ss_data_template exists on the engine.
    Change data_template to reflect values in your environment.
    """

    @classmethod
    def setUpClass(cls):
        super(SelfServiceContainerTests, cls).setUpClass()
        cls.server_obj = GetSession()
        cls.server_obj.dlpx_session('172.16.98.44', 'delphix_admin',
                                    'delphix', 'DOMAIN')
        cls.server_obj.dlpx_ddps['engine_name'] = 'test_engine'
        cls.data_template = 'ss_data_template'
        cls.container_name = 'ss_data_pod'
        cls.database_name = 'ss_te'
        cls.owner_name = 'dev'
        cls.bookmark_name = 'ss_bookmark'

    def _find_ref(self, f_class, obj_name):
        for obj in f_class.get_all(self.server_obj.server_session):
            if obj.name == obj_name:
                return obj
        raise dlpx_execptions.DlpxObjectNotFound

    def _create_container(self):
        return ss_container.create_container(
            self.server_obj, self.data_template, self.container_name,
            self.database_name)

    def _delete_container(self, create_ref):
        delete_params = vo.JSDataContainerDeleteParameters()
        delete_params.delete_data_sources = False
        selfservice.container.delete(self.server_obj.server_session,
                                     create_ref, delete_params)

    def test_create_add_remove_users_ss_container(self):
        print('TEST - Create, add/remove users in an  SS Container')
        create_ref = ss_container.create_container(
            self.server_obj, self.data_template, self.container_name,
            self.database_name)
        self.assertIsInstance(create_ref, str)
        ss_container.add_owner(self.server_obj, self.owner_name,
                               self.container_name)
        ss_container.remove_owner(self.server_obj, self.owner_name,
                               self.container_name)
        self._delete_container(create_ref)

    def test_refresh_container(self):
        print('TEST - Refresh Container')
        create_ref = ss_container.create_container(
            self.server_obj, self.data_template, self.container_name,
            self.database_name)
        self.assertIsInstance(create_ref, str)
        ss_container.refresh_container(self.server_obj, self.container_name)
        self._delete_container(create_ref)

    def test_restore_and_reset_container(self):
        create_ref = self._create_container()
        data_layout_obj = self._find_ref(selfservice.container,
                                         self.container_name)
        ss_bookmark_params = vo.JSBookmarkCreateParameters()
        ss_bookmark_params.bookmark = vo.JSBookmark()
        ss_bookmark_params.bookmark.name = self.bookmark_name
        ss_bookmark_params.bookmark.branch = data_layout_obj.active_branch
        ss_bookmark_params.timeline_point_parameters = \
            vo.JSTimelinePointLatestTimeInput()
        ss_bookmark_params.timeline_point_parameters.source_data_layout = \
            data_layout_obj.reference
        ss_bookmark_ref = selfservice.bookmark.create(
            self.server_obj.server_session, ss_bookmark_params)
        ss_container.restore_container(self.server_obj, self.container_name,
                                       self.bookmark_name)
        ss_container.reset_container(self.server_obj, self.container_name)
        self._delete_container(create_ref)

    def test_list_containers(self):
        msg = io.StringIO()
        sys.stdout = msg
        ss_container.list_containers(self.server_obj)
        sys.stdout = sys.__stdout__
        self.assertIn('Name, Active Branch, Owner', msg.getvalue()) 

    def test_list_hierarchy_containers(self):
        create_ref = self._create_container()
        msg = io.StringIO()
        sys.stdout = msg
        ss_container.list_hierarchy(self.server_obj, self.container_name)
        sys.stdout = sys.__stdout__
        self.assertIn('Related VDBs:', msg.getvalue()) 
        self._delete_container(create_ref)

# Run the test case
if __name__ == '__main__':
    unittest.main(module=__name__, buffer=True)
