"""
List, create, destroy and refresh Delphix timeflows
"""

import re
import sys

from delphixpy.v1_10_2 import exceptions
from delphixpy.v1_10_2 import web
from delphixpy.v1_10_2 import job_context
from delphixpy.v1_10_2.web.timeflow import bookmark
from delphixpy.v1_10_2.web import vo

import lib

VERSION = "v.0.3.000"


class DxTimeflow:
    """Shared methods for timeflows
    :param engine: A Delphix DDP session object
    :type engine: delphixpy.v1_10_2.delphix_engine.DelphixEngine
    """

    def __init__(self, engine):
        super().__init__()
        self._engine = engine

    def get_timeflow_reference(self, db_name):
        """
        :param db_name: The database name to retrieve current_timeflow
        :type db_name: str
        :return: current_timeflow reference for db_name
        """

        db_lst = web.database.get_all(self._engine)
        for db_obj in db_lst:
            if db_obj.name == db_name:
                return db_obj.current_timeflow
        raise lib.dlpx_exceptions.DlpxException(f'Timeflow reference not '
                                                f'found for {db_name}.')

    def list_timeflows(self):
        """
        Retrieve all timeflows for a given engine
        :return: generator containing
        delphixpy.v1_10_2.web.objects.OracleTimeflow.OracleTimeflow objects
        """
        all_timeflows = web.timeflow.get_all(self._engine)
        for tf_obj in all_timeflows:
            try:
                tf_obj.name = lib.get_references.find_obj_name(
                    self._engine, web.database, tf_obj.container)
                yield tf_obj
            except TypeError as err:
                raise lib.dlpx_exceptions.DlpxException(
                    f'Listing Timeflows encountered an error:\n{err}')
            except (exceptions.RequestError, exceptions.JobError,
                    exceptions.HttpError) as err:
                raise lib.dlpx_exceptions.DlpxException(err)

    def create_bookmark(self, bookmark_name, db_name, timestamp=None,
                        location=None):
        """
        Create a timeflow bookmark

        :param bookmark_name: Bookmark's name
        :type bookmark_name: str
        :param db_name: The database name to create the bookmark
        :type bookmark_name: str
        :param timestamp: Timestamp for the bookmark.
        :type timestamp: str Required format is (UTC/Zulu):
                         YYYY-MM-DDTHH:MM:SS.000Z
        :param location: Location which is referenced by the bookmark
        """
        tf_create_params = vo.TimeflowBookmarkCreateParameters()
        tf_ref = self.get_timeflow_reference(db_name)
        if re.search('ORAC', tf_ref, re.IGNORECASE):
            tf_create_params.timeflow_point = vo.OracleTimeflowPoint()
        elif re.search('MSSql', tf_ref, re.IGNORECASE):
            tf_create_params.timeflow_point = vo.MSSqlTimeflowPoint()
        elif re.search('ASE', tf_ref, re.IGNORECASE):
            tf_create_params.timeflow_point = vo.ASETimeflowPoint()
        tf_create_params.name = bookmark_name
        tf_create_params.timeflow_point.timeflow = tf_ref
        if timestamp is not None:
            tf_create_params.timeflow_point.timestamp = timestamp
        else:
            tf_create_params.timeflow_point.location = location
        try:
            bookmark.bookmark.create(self._engine, tf_create_params)
        except exceptions.RequestError as err:
            raise lib.dlpx_exceptions.DlpxException(err.error)
        except (exceptions.JobError, exceptions.HttpError):
            lib.dx_logging.print_exception(
                f'Fatal exception caught while creating the Timeflow '
                f'Bookmark:\n{sys.exc_info()[0]}\n')

    def delete_bookmark(self, bookmark_name):
        """
        Delete a Timeflow bookmark
        :param bookmark_name: name of the TF bookmark to delete
        :param bookmark_name: str
        """
        tf_bookmark = next(lib.get_references.find_obj_by_name(
            self._engine, bookmark, bookmark_name))
        try:
            bookmark.bookmark.delete(self._engine, tf_bookmark.reference)
        except exceptions.RequestError as err:
            raise lib.dlpx_exceptions.DlpxException(err.error)
        except (exceptions.JobError, exceptions.HttpError):
            raise lib.dlpx_exceptions.DlpxException(
                f'Fatal exception caught while creating the Timeflow '
                f'Bookmark:\n{sys.exc_info()[0]}\n')

    def list_tf_bookmarks(self):
        """
        Return all Timeflow Bookmarks
        :return: generator containing v1_10_2.web.vo.TimeflowBookmark objects
        """
        all_bookmarks = bookmark.bookmark.get_all(self._engine)
        for tfbm_obj in all_bookmarks:
            try:
                if tfbm_obj.timestamp is None:
                    tfbm_obj.timestamp = None
                else:
                    tfbm_obj.timestamp = \
                        lib.get_references.convert_timestamp(
                            self._engine, tfbm_obj.timestamp[:-5])
                yield tfbm_obj
            except TypeError:
                raise lib.dlpx_exceptions.DlpxException(f'No timestamp found '
                                                        f'for {tfbm_obj.name}')
            except exceptions.RequestError as err:
                dlpx_err = err.error
                raise lib.dlpx_exceptions.DlpxException(dlpx_err.action)

    def find_snapshot(self, snap_name):
        """
        Method to find a snapshot by name
        :param snap_name: Name of the snapshot
        :type snap_name: str
        :return: generator :py:class:`v1_10_2.web.vo.TimeflowSnapshot`
        """
        snapshots = web.snapshot.get_all(self._engine)
        for snapshot_obj in snapshots:
            if str(snapshot_obj.name).startswith(snap_name):
                yield snapshot_obj.name
            elif str(snapshot_obj.latest_change_point.timestamp).startswith(
                    snap_name):
                yield snapshot_obj

    def set_timeflow_point(self, container_obj, timestamp_type,
                           timestamp='LATEST', timeflow_name=None):
        """
        Returns the reference of the timestamp specified.
        :param container_obj: Delphix object containing the
                              snapshot/timeflow to be provisioned
        :type container_obj:
            :py:class:`delphixpy.v1_10_2.web.objects.Container.Container`
            object
        :param timestamp_type: Type of timestamp - SNAPSHOT or TIME
        :type timestamp_type: str
        :param timestamp: Name of timestamp/snapshot. Default: Latest
        :type timestamp: str
        :param timeflow_name: Name of the timeflow
        :type timeflow_name: TimeflowPointTimestamp
        :return: one of the following types depending on timeflow required
                 TimeflowPointParameters
                 TimeflowPointSnapshot
                 TimeflowPointSemantic
        """
        timeflow_point_parameters = None
        if timestamp_type.upper() == 'SNAPSHOT':
            if timestamp.upper() == 'LATEST':
                timeflow_point_parameters = vo.TimeflowPointSemantic()
                timeflow_point_parameters.container = container_obj.reference
                timeflow_point_parameters.location = 'LATEST_SNAPSHOT'
            elif timestamp.startswith('@'):
                snapshot_obj = lib.get_references.find_obj_by_name(
                    self._engine, web.snapshot, timestamp)
                if snapshot_obj:
                    timeflow_point_parameters = vo.TimeflowPointSnapshot()
                    timeflow_point_parameters.snapshot = snapshot_obj.reference
                else:
                    raise lib.dlpx_exceptions.DlpxException(
                        f'ERROR: Was unable to use the specified snapshot '
                        f'{timestamp} for database {container_obj.name}')
            elif timestamp:
                snapshot_obj = self.find_snapshot(container_obj.reference)
                if snapshot_obj:
                    timeflow_point_parameters = vo.TimeflowPointTimestamp()
                    timeflow_point_parameters.timeflow = snapshot_obj.timeflow
                    timeflow_point_parameters.timestamp = \
                        snapshot_obj.latest_change_point.timestamp
                elif snapshot_obj is None:
                    raise lib.dlpx_exceptions.DlpxException(
                        f'Unable to find a suitable time for {timestamp}'
                        f' for database {container_obj.name}')
        elif timestamp_type.upper() == 'TIME':
            if timestamp.upper() == 'LATEST':
                timeflow_point_parameters = vo.TimeflowPointSemantic()
                timeflow_point_parameters.container = container_obj.reference
                timeflow_point_parameters.location = 'LATEST_POINT'
            elif timestamp:
                timeflow_point_parameters = vo.TimeflowPointTimestamp()
                timeflow_obj = lib.get_references.find_obj_by_name(
                    self._engine, web.timeflow, timeflow_name)
                timeflow_point_parameters.timeflow = timeflow_obj.reference
                timeflow_point_parameters.timestamp = timestamp
        else:
            raise lib.dlpx_exceptions.DlpxObjectNotFound(
                f'Timestamp type {timestamp_type} not found for VDB '
                f'{container_obj}. Valid types are snapshot or time.')
        return timeflow_point_parameters

    def refresh_vdb_tf_bookmark(self, vdb_name, tf_bookmark_name):
        """
        Refreshes a VDB from a Timeflow Bookmark
        :param vdb_name: Name of the VDB
        :type vdb_name: str
        :param tf_bookmark_name: Name of the Timeflow Bookmark
        :type tf_bookmark_name: str
        :return: str reference to the refresh job
        """
        try:
            vdb_obj = next(
                lib.get_references.find_obj_by_name(self._engine,
                                                    web.database, vdb_name))
            tf_bookmark_obj = next(lib.get_references.find_obj_by_name(
                self._engine, web.timeflow.bookmark, tf_bookmark_name))
        except StopIteration as err:
            raise lib.dlpx_exceptions.DlpxObjectNotFound(err)
        if 'ORACLE' in vdb_obj.reference:
            tf_params = vo.OracleRefreshParameters()
        else:
            tf_params = vo.RefreshParameters()
        tf_params.timeflow_point_parameters = vo.TimeflowPointBookmark()
        tf_params.timeflow_point_parameters.bookmark = \
            tf_bookmark_obj.reference
        try:
            with job_context.async(self._engine):
                web.database.refresh(self._engine, vdb_obj.reference,
                                     tf_params)
                return self._engine.last_job
        except exceptions.RequestError as err:
            raise lib.dlpx_exceptions.DlpxException(err.error.action)
        except (exceptions.JobError, exceptions.HttpError) as err:
            lib.dx_logging.print_exception(f'Exception caught during refresh:'
                                           f'\n{sys.exc_info()[0]}')
            raise lib.dlpx_exceptions.DlpxException(err.error)
