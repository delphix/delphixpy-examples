"""
List, create, destroy and refresh Delphix timeflows
"""
# TODO:
#    implement debug flag

import re
import sys

from delphixpy.v1_6_0.exceptions import HttpError, JobError, RequestError
from delphixpy.v1_6_0.web import database
from delphixpy.v1_6_0.web import timeflow
from delphixpy.v1_6_0.web import snapshot
from delphixpy.v1_6_0 import job_context
from delphixpy.v1_6_0.web.timeflow import bookmark
from delphixpy.v1_6_0.web.vo import OracleRefreshParameters
from delphixpy.v1_6_0.web.vo import OracleTimeflowPoint
from delphixpy.v1_6_0.web.vo import RefreshParameters
from delphixpy.v1_6_0.web.vo import TimeflowPointLocation
from delphixpy.v1_6_0.web.vo import MSSqlTimeflowPoint
from delphixpy.v1_6_0.web.vo import TimeflowPointTimestamp
from delphixpy.v1_6_0.web.vo import TimeflowPointSemantic

from DlpxException import DlpxException
from GetReferences import get_obj_reference
from GetReferences import convert_timestamp
from GetReferences import find_obj_by_name

VERSION = 'v.0.1.000'

class DxTimeflow(object):
    """Shared methods for timeflows """

    def __init__(self, engine):
        self.engine = engine


    def get_timeflow_reference(self, db_name):
        """
        Return current_timeflow for the db_name

        db_name: The database name to retrieve current_timeflow
        """

        self.db_lst = database.get_all(self.engine)

        for self.db_obj in self.db_lst:
            if self.db_obj.name == db_name:
                return self.db_obj.current_timeflow

        raise DlpxException('Timeflow reference not found for %s' % db_name)


    def list_timeflows(self):
        """
        Retrieve and print all timeflows for a given engine
        """

        self.all_timeflows = timeflow.get_all(self.engine)

        print 'DB Name, Timeflow Name, Timestamp'
        for self.tfbm_lst in self.all_timeflows:

            try:
                self.db_name = get_obj_reference(self.engine, database,
                                                 self.tfbm_lst.container)

                print '%s, %s, %s\n' % (str(self.db_name),
                                        str(self.tfbm_lst.name),
                                        str(self.tfbm_lst.parent_point.timestamp))

            except AttributeError:
                print '%s, %s\n' % (str(self.tfbm_lst.name), str(self.db_name))

            except TypeError as e:
                raise DlpxException('Listing Timeflows encountered an error'
                                    ':\n%s' % (e))

            except RequestError as e:
                self.dlpx_err = e.message
                raise DlpxException(self.dlpx_err.action)

            except (JobError, HttpError) as e:
                raise DlpxException(e)


    def create_bookmark(self, bookmark_name, db_name, timestamp=None,
                        location=None):
        """
        Create a timeflow bookmark

        bookmark_name: Bookmark's name
        db_name: The database name to re
        timestamp: Timestamp for the bookmark.
               Required format is (UTC/Zulu): YYYY-MM-DDTHH:MM:SS.000Z
        location: Location of the bookmark
        """

        self.tf_ref = self.get_timeflow_reference(db_name)

        if re.search('ORAC', self.tf_ref, re.IGNORECASE):
            self.bookmark_type = 'OracleTimeflowPoint'
            self.otfp = OracleTimeflowPoint()
        elif re.search('MSSql', self.tf_ref, re.IGNORECASE):
            self.bookmark_type = 'MSSqlTimeflowPoint'
            self.otfp = MSSqlTimeflowPoint()

        self.otfp.type = self.bookmark_type
        self.otfp.timeflow = self.tf_ref

        if timestamp is not None:
            self.otfp.timestamp = timestamp
        else:
            self.otfp.location = location

        self.tf_create_params = TimeflowBookmarkCreateParameters()
        self.tf_create_params.name = bookmark_name
        self.tf_create_params.timeflow_point = self.otfp

        try:
            print 'Bookmark %s successfully created with reference %s' % (
                bookmark.bookmark.create(self.engine, self.tf_create_params))

        except RequestError as e:
            raise DlpxException(e.message)

        except (JobError, HttpError):
            print 'Fatal exception caught while creating the Timeflow Bookmark'
            raise DlpxException(sys.exc_info()[0])


    def get_bookmarks(self, parsable=False):
        """
        Print all Timeflow Bookmarks

        parsable (optional): Flag to print output in a parsable format.
        """

        self.all_bookmarks = bookmark.bookmark.get_all(self.engine)

        if parsable is False:
            print('\nBookmark name\tReference\tTimestamp\t'
                  'Location\tTimeflow\n')

        elif parsable is True:
            print 'Bookmark name,Reference,Timestamp,Location,Timeflow'

        for self.tfbm_lst in self.all_bookmarks:
            try:
                if self.tfbm_lst.timestamp is None:
                    self.converted_timestamp = None

                else:
                    self.converted_timestamp = \
                        convert_timestamp(self.engine,
                                          self.tfbm_lst.timestamp[:-5])

                if parsable is False:
                    print '%s %s %s %s %s' % (self.tfbm_lst.name,
                                              self.tfbm_lst.reference,
                                              str(self.converted_timestamp),
                                              self.tfbm_lst.location,
                                              self.tfbm_lst.timeflow)
                elif parsable is True:
                    print '%s,%s,%s,%s,%s' % (self.tfbm_lst.name,
                                              self.tfbm_lst.reference,
                                              str(self.converted_timestamp),
                                              self.tfbm_lst.location,
                                              self.tfbm_lst.timeflow)

            except TypeError:
                print 'No timestamp found for %s' % self.tfbm_lst.name

            except RequestError as e:
                self.dlpx_err = e.message
                raise DlpxException(self.dlpx_err.action)


    def find_snapshot(self, database_ref, timestamp, snap_name=None, 
                      snap_time=None):
        """
        Method to find a snapshot by name

        database_obj: database reference for the snapshot lookup
        snap_name: name of the snapshot. Default: None
        snap_time: time of the snapshot. Default: None
        """

        snapshots = snapshot.get_all(self.engine, database=database_ref)

        matches = []
        for snapshot_obj in snapshots:
            if (str(snapshot_obj.name).startswith(timestamp) and
               snap_name is not None):

                matches.append(snapshot_obj)

            elif (str(snapshot_obj.latest_change_point.timestamp).startswith(timestamp)
                  and snap_time is not None):

                matches.append(snapshot_obj)

        if len(matches) == 1:
            return matches[0]

        elif len(matches) > 1:
            raise DlpxException('%s: The name specified was not specific '
                                'enough. More than one match found.\n' % (
                                self.engine.address))

        elif len(matches) < 1:
            raise DlpxException('%s: No matches found for the time '
                                'specified.\n' % (self.engine.address))


    def set_timeflow_point(self, container_obj, timestamp_type,
                           timestamp='LATEST', timeflow_name=None):
        """
        This method returns the reference of the timestamp specified.
        container_obj: Delphix object containing the snapshot/timeflow to be
                       provisioned.
        timestamp_type: Type of timestamp - SNAPSHOT or TIME
        timestamp: Name of timestamp/snapshot. Default: Latest
        """

        if timestamp_type.upper() == "SNAPSHOT":
            if timestamp.upper() == "LATEST":
                timeflow_point_parameters = TimeflowPointSemantic()
                timeflow_point_parameters.location = "LATEST_SNAPSHOT"

            elif timestamp.startswith("@"):
                snapshot_obj = self.find_snapshot(container_obj.reference,
                                                  timestamp, snap_name=True)

                if snapshot_obj:
                    timeflow_point_parameters=TimeflowPointLocation()
                    timeflow_point_parameters.timeflow = snapshot_obj.timeflow
                    timeflow_point_parameters.location = \
                                   snapshot_obj.latest_change_point.location

                else:
                    raise DlpxException('ERROR: Was unable to use the '
                                        'specified snapshot %s for database %s'
                                        '.\n' % (timestamp, container_obj.name))

            elif timestamp:
                snapshot_obj = self.find_snapshot(container_obj.reference, 
                                                  timestamp, snap_time=True)

                if snapshot_obj:
                    timeflow_point_parameters=TimeflowPointTimestamp()
                    timeflow_point_parameters.timeflow = snapshot_obj.timeflow
                    timeflow_point_parameters.timestamp = \
                                   snapshot_obj.latest_change_point.timestamp

                elif snapshot_obj is None:
                    raise DlpxException('Was unable to find a suitable time'
                                        '  for %s for database %s' %
                                        (timestamp, container_obj.name))

        elif timestamp_type.upper() == "TIME":

#### Assert timeflow_name is not None
            if timestamp.upper() == "LATEST":
                timeflow_point_parameters = TimeflowPointSemantic()
                timeflow_point_parameters.location = "LATEST_POINT"

            elif timestamp:
                timeflow_point_parameters = TimeflowPointTimestamp()
                timeflow_point_parameters.type = 'TimeflowPointTimestamp'
                timeflow_obj = find_obj_by_name(self.engine, timeflow, 
                                                timeflow_name)

                timeflow_point_parameters.timeflow = timeflow_obj.reference
                timeflow_point_parameters.timestamp = timestamp
                return timeflow_point_parameters
        else:
            raise DlpxException('%s is not a valid timestamp_type. Exiting'
                                '\n' % (timestamp_type))

        timeflow_point_parameters.container = container_obj.reference
        return timeflow_point_parameters


    def refresh_container(self, parent_bookmark_ref, db_type, child_db_ref):
        """
        Refreshes a container

        parent_bookmark_ref: The parent's bookmark reference.
        db_type: The database type
        child_db_ref: The child database reference
        """

        if db_type == 'Oracle':
            self.tf_params = OracleRefreshParameters()
        else:
            self.tf_params = RefreshParameters()

        self.tf_params.timeflow_point_parameters = {'type':
                                                    'TimeflowPointBookmark',
                                                    'bookmark':
                                                    parent_bookmark_ref}

        try:
            with job_context.async(self.engine):
                self.db_ret_val = database.refresh(self.engine,
                                                   child_db_ref,
                                                   self.tf_params)
            return self.db_ret_val

        except RequestError as e:
            self.dlpx_err = e.message
            raise DlpxException(self.dlpx_err.action)

        except (JobError, HttpError):
            print 'Fatal exception caught during refresh.'
            raise DlpxException(sys.exc_info()[0])
