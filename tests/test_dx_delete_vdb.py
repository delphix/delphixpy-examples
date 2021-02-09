#
# Test routine for dx_delete_vdb
# This test can be run in the following modes:
# 1: Single engine mode, by providing --d <enginename>
# 2: Multi engine mode by providing --d all
# 3: Default engine mode by not providing a --d argument.
#
"""
Tests dx_delete_vdb
Example:
    python3 test_dx_delete_vdb.py
"""

VERSION = "v.0.0.001"

import unittest

import dx_delete_vdb

import lib.run_job
from lib.dlpx_exceptions import DlpxObjectNotFound
from lib.get_session import GetSession


class DxDeleteVDBTest(unittest.TestCase):
    def test_something(self):
        self.assertEqual(False, False)

    @classmethod
    def setUpClass(cls):
        super(DxDeleteVDBTest, cls).setUpClass()
        config_path = "../config/dxtools.conf"
        cls.engine_name = "myve"
        cls.vdb_name = "testvdb"
        cls.force = False
        cls.single_thread = False
        cls.dx_session_obj = GetSession()
        cls.dx_session_obj.get_config(config_path)
        cls.engine = None

    # Test to delete a vdb that does not exist in specified engine
    def test_delete_nonexisting_vdb(self):
        self.engine = self.dx_session_obj.dlpx_ddps[self.engine_name]
        self.dx_session_obj.dlpx_session(
            self.engine["ip_address"], self.engine["username"], self.engine["password"]
        )
        with self.assertRaises(DlpxObjectNotFound) as ex:
            dx_delete_vdb.delete_vdb(self.dx_session_obj, self.vdb_name, self.force)
        self.assertEqual(type(ex.exception), DlpxObjectNotFound)

    # Deletes vdb by name across all engines
    # The vdb should exist on one or more engines.
    def test_delete_vdb_allengines(self):
        self.vdb_name = "vOraCRM_BRKFIX"
        jobs = []
        jobs_success = True
        for eo in self.dx_session_obj.dlpx_ddps:
            self.engine = self.dx_session_obj.dlpx_ddps[eo]
            self.dx_session_obj.dlpx_session(
                self.engine["ip_address"],
                self.engine["username"],
                self.engine["password"],
            )
            try:
                jobid = dx_delete_vdb.delete_vdb(
                    self.dx_session_obj, self.vdb_name, self.force
                )
                jobs.append(jobid)
            except DlpxObjectNotFound as e:
                pass
            except Exception as e:
                jobs_success = False
                break

            for job in jobs:
                jobstate = lib.run_job.find_job_state_by_jobid(
                    eo, self.dx_session_obj, job, 10
                )
                if jobstate == "FAILED":
                    jobs_success = False
                    break
        self.assertEqual(jobs_success, True)


if __name__ == "__main__":
    unittest.main()
