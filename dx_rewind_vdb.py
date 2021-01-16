#!/usr/bin/env python3
# Corey Brune - Sep 2016
# This script performs a rewind of a vdb
# requirements
# pip install --upgrade setuptools pip docopt delphixpy.v1_8_0

# The below doc follows the POSIX compliant standards and allows us to use
# this doc to also define ARGUMENTS for the script.

"""Rewinds a vdb
Usage:
  dx_rewind_vdb.py (--vdb <name> [--timestamp_type <type>] \
  [--timestamp <timepoint_semantic>])
  [--bookmark <type> --engine <identifier> --all --parallel <n>] \
  [--poll <n> --config <path_to_file> --logdir <path_to_file>]
  dx_rewind_vdb.py -h | --help | -v | --version

Rewinds a Delphix VDB
Examples:
    Rollback to latest snapshot using defaults:
      dx_rewind_vdb.py --vdb testVdbUF
    Rollback using a specific timestamp:
      dx_rewind_vdb.py --vdb testVdbUF --timestamp_type snapshot \
      --timestamp 2016-11-15T11:30:17.857Z


Options:
  --vdb <name>              Name of VDB to rewind
  --type <database_type>    Type of database: oracle, mssql, ase, vfiles
  --timestamp_type <type>   The type of timestamp being used for the reqwind.
                            Acceptable Values: TIME, SNAPSHOT
                            [default: SNAPSHOT]
  --all                       Run against all engines.
  --timestamp <timepoint_semantic>
                            The Delphix semantic for the point in time on
                            the source to rewind the VDB.
                            Formats:
                            latest point in time or snapshot: LATEST
                            point in time: "YYYY-MM-DD HH24:MI:SS"
                            snapshot name: "@YYYY-MM-DDTHH24:MI:SS.ZZZ"
                            snapshot time from GUI: "YYYY-MM-DD HH24:MI"
                            [default: LATEST]
  --engine <type>           Alt Identifier of Delphix engine in dxtools.conf.
  --debug                   Enable debug logging
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./dxtools.conf]
  --logdir <path_to_file>    The path to the logfile you want to use.
                            [default: ./dx_rewind_vdb.log]
  -h --help                 Show this screen.
  -v --version              Show version.
"""

import sys
import time
from os.path import basename

import docopt

from delphixpy.v1_8_0.web import vo
from delphixpy.v1_10_2 import exceptions
from delphixpy.v1_10_2.web import database
from lib import dlpx_exceptions
from lib import dx_logging
from lib import dx_timeflow
from lib import get_references
from lib import get_session
from lib import run_job
from lib.run_async import run_async

VERSION = "v.0.3.003"


def rewind_database(dlpx_obj, vdb_name, timestamp, timestamp_type="SNAPSHOT"):
    """
    Performs the rewind (rollback) of a VDB
    :param dlpx_obj: Virtualization Engine session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param vdb_name: VDB to be rewound
    :type vdb_name: str
    :param timestamp: Point in time to rewind the VDB
    :type timestamp: str
    :param timestamp_type: The type of timestamp being used for the rewind
    :type timestamp_type: str
    """
    engine_name = dlpx_obj.dlpx_ddps["engine_name"]
    dx_timeflow_obj = dx_timeflow.DxTimeflow(dlpx_obj.server_session)
    container_obj = get_references.find_obj_by_name(
        dlpx_obj.server_session, database, vdb_name
    )
    # Sanity check to make sure our container object has a reference
    if container_obj.reference:
        try:
            if container_obj.runtime.enabled == "ENABLED":
                dx_logging.print_info(
                    f"INFO: {engine_name} Rewinding "
                    f"{container_obj.name} to {timestamp}\n"
                )
            elif container_obj.virtual is not True or container_obj.staging is True:
                raise dlpx_exceptions.DlpxException(
                    f"{container_obj.name} in engine {engine_name} is not "
                    f"a virtual object. Skipping.\n"
                )
        # This exception is raised if rewinding a vFiles VDB since
        # AppDataContainer does not have virtual, staging or enabled attributes
        except AttributeError:
            pass
        # If the vdb is a Oracle type, we need to use a OracleRollbackParameters
        if str(container_obj.reference).startswith("ORACLE"):
            rewind_params = vo.OracleRollbackParameters()
        else:
            rewind_params = vo.RollbackParameters()
        rewind_params.timeflow_point_parameters = dx_timeflow_obj.set_timeflow_point(
            container_obj, timestamp_type, timestamp
        )
        try:
            # Rewind the VDB
            database.rollback(
                dlpx_obj.server_session, container_obj.reference, rewind_params
            )
            dlpx_obj.jobs[engine_name] = dlpx_obj.server_session.last_job
        except (
            exceptions.RequestError,
            exceptions.HttpError,
            exceptions.JobError,
        ) as err:
            raise dlpx_exceptions.DlpxException(
                f"ERROR: {engine_name} encountered an error on "
                f"{container_obj.name} during the rewind process:\n{err}"
            )
    # Don't do anything if the database is disabled
    else:
        dx_logging.print_info(
            f"{engine_name}: {container_obj.name} is not " f"enabled. Skipping sync."
        )


@run_async
def main_workflow(engine, dlpx_obj, single_thread):
    """
    This function is where we create our main workflow.
    Use the @run_async decorator to run this function asynchronously.
    The @run_async decorator allows us to run against multiple Delphix Engine
    simultaneously
    :param engine: Dictionary of engines
    :type engine: dictionary
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param single_thread: True - run single threaded, False - run multi-thread
    :type single_thread: bool
    """
    try:
        # Setup the connection to the Delphix DDP
        dlpx_obj.dlpx_session(
            engine["ip_address"], engine["username"], engine["password"]
        )
    except dlpx_exceptions.DlpxException as err:
        dx_logging.print_exception(
            f"ERROR: dx_rewind_vdb encountered an error authenticating to "
            f'{engine["hostname"]} {ARGUMENTS["--target"]}:\n{err}\n'
        )
    thingstodo = ["thingstodo"]
    try:
        with dlpx_obj.job_mode(single_thread):
            while dlpx_obj.jobs or thingstodo:
                if thingstodo:
                    rewind_database(
                        dlpx_obj,
                        ARGUMENTS["--vdb"],
                        ARGUMENTS["--timestamp"],
                        ARGUMENTS["--timestamp_type"],
                    )
                    thingstodo.pop()
                    run_job.find_job_state(engine, dlpx_obj)
    except (
        dlpx_exceptions.DlpxException,
        dlpx_exceptions.DlpxObjectNotFound,
        exceptions.RequestError,
        exceptions.JobError,
        exceptions.HttpError,
    ) as err:
        dx_logging.print_exception(
            f"Error in dx_rewind_vdb:" f'{engine["ip_address"]}\n{err}'
        )


def main():
    """
    main function - creates session and runs jobs
    """
    time_start = time.time()
    try:
        dx_session_obj = get_session.GetSession()
        dx_logging.logging_est(ARGUMENTS["--logdir"])
        config_file_path = ARGUMENTS["--config"]
        single_thread = ARGUMENTS["--single_thread"]
        engine = ARGUMENTS["--engine"]
        dx_session_obj.get_config(config_file_path)
        # This is the function that will handle processing main_workflow for
        # all the servers.
        for each in run_job.run_job(
            main_workflow, dx_session_obj, engine, single_thread
        ):
            # join them back together so that we wait for all threads to
            # complete
            each.join()
        elapsed_minutes = run_job.time_elapsed(time_start)
        dx_logging.print_info(
            f"script took {elapsed_minutes} minutes to " f"get this far."
        )
    # Here we handle what we do when the unexpected happens
    except SystemExit as err:
        # This is what we use to handle our sys.exit(#)
        sys.exit(err)

    except dlpx_exceptions.DlpxException as err:
        # We use this exception handler when an error occurs in a function
        # call.
        dx_logging.print_exception(
            f"ERROR: Please check the ERROR message " f"below:\n {err.error}"
        )
        sys.exit(2)

    except exceptions.HttpError as err:
        # We use this exception handler when our connection to Delphix fails
        dx_logging.print_exception(
            f"ERROR: Connection failed to the Delphix DDP. Please check "
            f"the ERROR message below:\n{err.status}"
        )
        sys.exit(2)

    except exceptions.JobError as err:
        # We use this exception handler when a job fails in Delphix so that we
        # have actionable data
        elapsed_minutes = run_job.time_elapsed(time_start)
        dx_logging.print_exception(
            f"A job failed in the Delphix Engine:\n{err.job}."
            f"{basename(__file__)} took {elapsed_minutes} minutes to get "
            f"this far"
        )
        sys.exit(3)

    except KeyboardInterrupt:
        # We use this exception handler to gracefully handle ctrl+c exits
        dx_logging.print_debug("You sent a CTRL+C to interrupt the process")
        elapsed_minutes = run_job.time_elapsed(time_start)
        dx_logging.print_info(
            f"{basename(__file__)} took {elapsed_minutes} " f"minutes to get this far."
        )


if __name__ == "__main__":
    # Grab our ARGUMENTS from the doc at the top of the script
    ARGUMENTS = docopt.docopt(__doc__, version=basename(__file__) + " " + VERSION)
    # Feed our ARGUMENTS to the main function, and off we go!
    main()
