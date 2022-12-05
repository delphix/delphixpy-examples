"""
List, create, destroy and refresh Delphix timeflows
"""
from __future__ import print_function

import re
import sys

from delphixpy.v1_8_0 import job_context
from delphixpy.v1_8_0.exceptions import HttpError
from delphixpy.v1_8_0.exceptions import JobError
from delphixpy.v1_8_0.exceptions import RequestError
from delphixpy.v1_8_0.web import database
from delphixpy.v1_8_0.web import snapshot
from delphixpy.v1_8_0.web import timeflow
from delphixpy.v1_8_0.web.timeflow import bookmark
from delphixpy.v1_8_0.web.vo import MSSqlTimeflowPoint
from delphixpy.v1_8_0.web.vo import OracleRefreshParameters
from delphixpy.v1_8_0.web.vo import OracleTimeflowPoint
from delphixpy.v1_8_0.web.vo import RefreshParameters
from delphixpy.v1_8_0.web.vo import TimeflowPointLocation
from delphixpy.v1_8_0.web.vo import TimeflowPointSemantic
from delphixpy.v1_8_0.web.vo import TimeflowPointTimestamp

from .DlpxException import DlpxException
from .DxLogging import print_exception
from .GetReferences import convert_timestamp
from .GetReferences import find_obj_by_name
from .GetReferences import get_obj_reference

# TODO:
#    implement debug flag


VERSION = "v.0.2.003"


class DxTimeflow(object):
    """Shared methods for timeflows"""

    def __init__(self, engine):
        self.engine = engine

    def get_timeflow_reference(self, db_name):
        """
        Return current_timeflow for the db_name

        db_name: The database name to retrieve current_timeflow
        """

        db_lst = database.get_all(self.engine)

        for db_obj in db_lst:
            if db_obj.name == db_name:
                return db_obj.current_timeflow

        raise DlpxException("Timeflow reference not found for {}".format(db_name))

    def list_timeflows(self):
        """
        Retrieve and print all timeflows for a given engine
        """

        all_timeflows = timeflow.get_all(self.engine)

        print("DB Name, Timeflow Name, Timestamp")
        for tfbm_lst in all_timeflows:

            try:
                db_name = get_obj_reference(self.engine, database, tfbm_lst.container)

                print(
                    "{}, {}, {}\n".format(
                        str(db_name),
                        str(tfbm_lst.name),
                        str(tfbm_lst.parent_point.timestamp),
                    )
                )

            except AttributeError:
                print("{}, {}\n".format(str(tfbm_lst.name), str(db_name)))

            except TypeError as e:
                raise DlpxException(
                    "Listing Timeflows encountered an error" ":\n{}".format((e))
                )

            except RequestError as e:
                dlpx_err = e.message
                raise DlpxException(dlpx_err.action)

            except (JobError, HttpError) as e:
                raise DlpxException(e)

    def create_bookmark(self, bookmark_name, db_name, timestamp=None, location=None):
        """
        Create a timeflow bookmark

        bookmark_name: Bookmark's name
        db_name: The database name to re
        timestamp: Timestamp for the bookmark.
               Required format is (UTC/Zulu): YYYY-MM-DDTHH:MM:SS.000Z
        location: Location of the bookmark
        """

        global bookmark_type
        tf_ref = self.get_timeflow_reference(db_name)

        if re.search("ORAC", tf_ref, re.IGNORECASE):
            bookmark_type = "OracleTimeflowPoint"
            otfp = OracleTimeflowPoint()
        elif re.search("MSSql", tf_ref, re.IGNORECASE):
            bookmark_type = "MSSqlTimeflowPoint"
            otfp = MSSqlTimeflowPoint()

        otfp.type = bookmark_type
        otfp.timeflow = tf_ref

        if timestamp is not None:
            otfp.timestamp = timestamp
        else:
            otfp.location = location

        tf_create_params = TimeflowBookmarkCreateParameters()
        tf_create_params.name = bookmark_name
        tf_create_params.timeflow_point = otfp

        try:
            print(
                "Bookmark {} successfully created with reference {}".format(
                    bookmark.bookmark.create(self.engine, tf_create_params)
                )
            )

        except RequestError as e:
            raise DlpxException(e.message)

        except (JobError, HttpError):
            print_exception(
                "Fatal exception caught while creating the"
                "Timeflow Bookmark:\n{}\n".format(sys.exc_info()[0])
            )

    def get_bookmarks(self, parsable=False):
        """
        Print all Timeflow Bookmarks

        parsable (optional): Flag to print output in a parsable format.
        """

        all_bookmarks = bookmark.bookmark.get_all(self.engine)

        if parsable is False:
            print("\nBookmark name\tReference\tTimestamp\t" "Location\tTimeflow\n")

        elif parsable is True:
            print("Bookmark name,Reference,Timestamp,Location,Timeflow")

        for tfbm_lst in all_bookmarks:
            try:
                if tfbm_lst.timestamp is None:
                    converted_timestamp = None

                else:
                    converted_timestamp = convert_timestamp(
                        self.engine, tfbm_lst.timestamp[:-5]
                    )

                if parsable is False:
                    print(
                        "{} {} {} {} {}".format(
                            tfbm_lst.name,
                            tfbm_lst.reference,
                            str(converted_timestamp),
                            tfbm_lst.location,
                            tfbm_lst.timeflow,
                        )
                    )
                elif parsable is True:
                    print(
                        "{},{},{},{},{}".format(
                            tfbm_lst.name,
                            tfbm_lst.reference,
                            str(converted_timestamp),
                            tfbm_lst.location,
                            tfbm_lst.timeflow,
                        )
                    )

            except TypeError:
                print("No timestamp found for {}".format(tfbm_lst.name))

            except RequestError as e:
                dlpx_err = e.message
                raise DlpxException(dlpx_err.action)

    def find_snapshot(self, database_ref, timestamp, snap_name=None, snap_time=None):
        """
        Method to find a snapshot by name

        database_obj: database reference for the snapshot lookup
        snap_name: name of the snapshot. Default: None
        snap_time: time of the snapshot. Default: None
        """

        snapshots = snapshot.get_all(self.engine, database=database_ref)

        matches = []
        for snapshot_obj in snapshots:
            if str(snapshot_obj.name).startswith(timestamp) and snap_name is not None:

                matches.append(snapshot_obj)

            elif (
                str(snapshot_obj.latest_change_point.timestamp).startswith(timestamp)
                and snap_time is not None
            ):

                matches.append(snapshot_obj)

        if len(matches) == 1:
            return matches[0]

        elif len(matches) > 1:
            raise DlpxException(
                "{}: The name specified was not specific "
                "enough. More than one match found.\n".format(self.engine.address)
            )

        elif len(matches) < 1:
            raise DlpxException(
                "{}: No matches found for the time "
                "specified.\n".format(self.engine.address)
            )

    def set_timeflow_point(
        self, container_obj, timestamp_type, timestamp="LATEST", timeflow_name=None
    ):
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
                timeflow_point_parameters.container = container_obj.reference
                timeflow_point_parameters.location = "LATEST_SNAPSHOT"

            elif timestamp.startswith("@"):
                snapshot_obj = self.find_snapshot(
                    container_obj.reference, timestamp, snap_name=True
                )

                if snapshot_obj:
                    timeflow_point_parameters = TimeflowPointLocation()
                    timeflow_point_parameters.timeflow = snapshot_obj.timeflow
                    timeflow_point_parameters.location = (
                        snapshot_obj.latest_change_point.location
                    )

                else:
                    raise DlpxException(
                        "ERROR: Was unable to use the "
                        "specified snapshot {}"
                        "for database {}".format(timestamp, container_obj.name)
                    )

            elif timestamp:
                snapshot_obj = self.find_snapshot(
                    container_obj.reference, timestamp, snap_time=True
                )

                if snapshot_obj:
                    timeflow_point_parameters = TimeflowPointTimestamp()
                    timeflow_point_parameters.timeflow = snapshot_obj.timeflow
                    timeflow_point_parameters.timestamp = (
                        snapshot_obj.latest_change_point.timestamp
                    )

                elif snapshot_obj is None:
                    print_exception(
                        "Was unable to find a suitable time"
                        "  for {} for database {}".format(
                            (timestamp, container_obj.name)
                        )
                    )

        elif timestamp_type.upper() == "TIME":
            if timestamp.upper() == "LATEST":
                timeflow_point_parameters = TimeflowPointSemantic()
                timeflow_point_parameters.location = "LATEST_POINT"

            elif timestamp:
                timeflow_point_parameters = TimeflowPointTimestamp()
                timeflow_point_parameters.type = "TimeflowPointTimestamp"
                timeflow_obj = find_obj_by_name(self.engine, timeflow, timeflow_name)

                timeflow_point_parameters.timeflow = timeflow_obj.reference
                timeflow_point_parameters.timestamp = timestamp
                return timeflow_point_parameters
        else:
            raise DlpxException(
                "{} is not a valid timestamp_type. Exiting" "\n".format(timestamp_type)
            )

        timeflow_point_parameters.container = container_obj.reference
        return timeflow_point_parameters

    def refresh_container(self, parent_bookmark_ref, db_type, child_db_ref):
        """
        Refreshes a container

        parent_bookmark_ref: The parent's bookmark reference.
        db_type: The database type
        child_db_ref: The child database reference
        """

        if db_type == "Oracle":
            tf_params = OracleRefreshParameters()
        else:
            tf_params = RefreshParameters()

        tf_params.timeflow_point_parameters = {
            "type": "TimeflowPointBookmark",
            "bookmark": parent_bookmark_ref,
        }

        try:
            with job_context.asyncly(self.engine):
                db_ret_val = database.refresh(self.engine, child_db_ref, tf_params)
            return db_ret_val

        except RequestError as e:
            dlpx_err = e.message
            raise DlpxException(dlpx_err.action)

        except (JobError, HttpError) as e:
            print_exception(
                "Exception caught during refresh:\n{}".format(sys.exc_info()[0])
            )
