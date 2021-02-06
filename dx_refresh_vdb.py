#!/usr/bin/env python
# Adam Bowen - Apr 2016
# This script refreshes a vdb
# Updated by Corey Brune Oct 2016
# requirements
# pip install --upgrade setuptools pip docopt delphixpy.v1_8_0

# The below doc follows the POSIX compliant standards and allows us to use
# this doc to also define our arguments for the script. This thing is brilliant.
"""Refresh a vdb
Usage:
  dx_refresh_vdb.py (--vdb <name> | --dsource <name> | --all_vdbs [--group_name <name>]| --host <name> | --list_timeflows | --list_snapshots)
                   [--timestamp_type <type>]
                   [--timestamp <timepoint_semantic> --timeflow <timeflow>]
                   [-d <identifier> | --engine <identifier> | --all]
                   [--debug] [--parallel <n>] [--poll <n>]
                   [--config <path_to_file>] [--logdir <path_to_file>]
  dx_refresh_vdb.py -h | --help | -v | --version
Refresh a Delphix VDB
Examples:
  dx_refresh_vdb.py --vdb "aseTest" --group_name "Analytics"
  dx_refresh_vdb.py --dsource "dlpxdb1"
  dx_refresh_vdb.py --all_vdbs --host LINUXSOURCE --parallel 4 --debug -d landsharkengine
  dx_refresh_vdb.py --all_vdbs --group_name "Analytics" --all
Options:
  --vdb <name>             Name of the object you are refreshing.
  --all_vdbs                Refresh all VDBs that meet the filter criteria.
  --dsource <name>          Name of dsource in Delphix to execute against.
  --group_name <name>       Name of the group to execute against.
  --list_timeflows          List all timeflows
  --list_snapshots          List all snapshots
  --host <name>             Name of environment in Delphix to execute against.
  --timestamp_type <type>   The type of timestamp you are specifying.
                            Acceptable Values: TIME, SNAPSHOT
                            [default: SNAPSHOT]
  --timestamp <timepoint_semantic>
                            The Delphix semantic for the point in time on
                            the source from which you want to refresh your VDB.
                            Formats:
                            latest point in time or snapshot: LATEST
                            point in time: "YYYY-MM-DD HH24:MI:SS"
                            snapshot name: "@YYYY-MM-DDTHH24:MI:SS.ZZZ"
                            snapshot time from GUI: "YYYY-MM-DD HH24:MI"
                            [default: LATEST]
  --timeflow <name>         Name of the timeflow to refresh a VDB
  -d <identifier>           Identifier of Delphix engine in dxtools.conf.
  --engine <type>           Alt Identifier of Delphix engine in dxtools.conf.
  --all                     Run against all engines.
  --debug                   Enable debug logging
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./dxtools.conf]
  --logdir <path_to_file>   The path to the logfile you want to use.
                            [default: ./dx_refresh_db.log]
  -h --help                 Show this screen.
  -v --version              Show version.
"""
from __future__ import print_function

import sys
import traceback
from os.path import basename
from time import sleep
from time import time

from docopt import docopt

from delphixpy.v1_8_0.exceptions import HttpError
from delphixpy.v1_8_0.exceptions import JobError
from delphixpy.v1_8_0.exceptions import RequestError
from delphixpy.v1_8_0.web import database
from delphixpy.v1_8_0.web import environment
from delphixpy.v1_8_0.web import group
from delphixpy.v1_8_0.web import job
from delphixpy.v1_8_0.web import source
from delphixpy.v1_8_0.web import timeflow
from delphixpy.v1_8_0.web.snapshot import snapshot
from delphixpy.v1_8_0.web.vo import OracleRefreshParameters
from delphixpy.v1_8_0.web.vo import RefreshParameters
from delphixpy.v1_8_0.web.vo import TimeflowPointLocation
from delphixpy.v1_8_0.web.vo import TimeflowPointSemantic
from delphixpy.v1_8_0.web.vo import TimeflowPointTimestamp
from lib.DlpxException import DlpxException
from lib.DxLogging import logging_est
from lib.DxLogging import print_debug
from lib.DxLogging import print_exception
from lib.DxLogging import print_info
from lib.DxTimeflow import DxTimeflow
from lib.GetReferences import find_obj_by_name
from lib.GetReferences import find_source_by_dbname
from lib.GetSession import GetSession

VERSION = "v.0.3.004"


def refresh_database(vdb_name, timestamp, timestamp_type="SNAPSHOT"):
    """
    This function actually performs the refresh
    engine:
    dlpx_obj: Virtualization Engine session object
    vdb_name: VDB to be refreshed
    """

    # Sanity check to make sure our source object has a reference
    dx_timeflow_obj = DxTimeflow(dx_session_obj.server_session)
    container_obj = find_obj_by_name(dx_session_obj.server_session, database, vdb_name)
    source_obj = find_source_by_dbname(
        dx_session_obj.server_session, database, vdb_name
    )

    # Sanity check to make sure our container object has a reference
    if container_obj.reference:
        try:
            if container_obj.virtual is not True:
                raise DlpxException(
                    "{} is not a virtual object. "
                    "Skipping.\n".format(container_obj.name)
                )
            elif container_obj.staging is True:
                raise DlpxException(
                    "{} is a virtual object. " "Skipping.\n".format(container_obj.name)
                )
            elif container_obj.runtime.enabled == "ENABLED":
                print_info(
                    "\nINFO: Refrshing {} to {}\n".format(container_obj.name, timestamp)
                )

        # This exception is raised if rewinding a vFiles VDB
        # since AppDataContainer does not have virtual, staging or
        # enabled attributes.
        except AttributeError:
            pass

    if source_obj.reference:
        # We can only refresh VDB's
        if source_obj.virtual != True:
            print_info(
                "\nINFO: {} is not a virtual object. Skipping.\n".format(
                    container_obj.name
                )
            )

        # Ensure this source is not a staging database. We can't act upon those.
        elif source_obj.staging == True:
            print_info(
                "\nINFO: {} is a staging database. Skipping.\n".format(
                    container_obj.name
                )
            )

        # Ensure the source is enabled. We can't refresh disabled databases.
        elif source_obj.runtime.enabled == "ENABLED":
            source_db = database.get(
                dx_session_obj.server_session, container_obj.provision_container
            )
            if not source_db:
                print_error(
                    "\nERROR: Was unable to retrieve the source container for {} \n".format(
                        container_obj.name
                    )
                )
            print_info(
                "\nINFO: Refreshing {} from {}\n".format(
                    container_obj.name, source_db.name
                )
            )

            # If the vdb is a Oracle type, we need to use a
            # OracleRefreshParameters
            """
            rewind_params = RollbackParameters()
            rewind_params.timeflow_point_parameters = dx_timeflow_obj.set_timeflow_point(
                container_obj, timestamp_type, timestamp
            )
            print_debug('{}: {}'.format(engine_name, str(rewind_params)))
            """
            if str(container_obj.reference).startswith("ORACLE"):
                refresh_params = OracleRefreshParameters()
            else:
                refresh_params = RefreshParameters()

            try:
                refresh_params.timeflow_point_parameters = (
                    dx_timeflow_obj.set_timeflow_point(
                        source_db, timestamp_type, timestamp
                    )
                )
                print_info("\nINFO: Refresh prams {}\n".format(refresh_params))

                # Sync it
                database.refresh(
                    dx_session_obj.server_session,
                    container_obj.reference,
                    refresh_params,
                )
                dx_session_obj.jobs[
                    dx_session_obj.server_session.address
                ] = dx_session_obj.server_session.last_job

            except RequestError as e:
                print(
                    "\nERROR: Could not set timeflow point:\n%s\n" % (e.message.action)
                )
                sys.exit(1)

            except DlpxException as e:
                print("ERROR: Could not set timeflow point:\n%s\n" % (e.message))
                sys.exit(1)

        # Don't do anything if the database is disabled
        else:
            print_info(
                "\nINFO: {} is not enabled. Skipping sync.\n".format(container_obj.name)
            )


def run_async(func):
    """
    http://code.activestate.com/recipes/576684-simple-threading-decorator/
    run_async(func)
        function decorator, intended to make "func" run in a separate
        thread (asynchronously).
        Returns the created Thread object
        E.g.:
        @run_async
        def task1():
            do_something
        @run_async
        def task2():
            do_something_too
        t1 = task1()
        t2 = task2()
        ...
        t1.join()
        t2.join()
    """
    from threading import Thread
    from functools import wraps

    @wraps(func)
    def async_func(*args, **kwargs):
        func_hl = Thread(target=func, args=args, kwargs=kwargs)
        func_hl.start()
        return func_hl

    return async_func


@run_async
def main_workflow(engine):
    """
    This function actually runs the jobs.
    Use the @run_async decorator to run this function asynchronously.
    This allows us to run against multiple Delphix Engine simultaneously

    engine: Dictionary of engines
    """
    jobs = {}

    try:
        # Setup the connection to the Delphix Engine
        dx_session_obj.serversess(
            engine["ip_address"], engine["username"], engine["password"]
        )

    except DlpxException as e:
        print_exception(
            "\nERROR: Engine {} encountered an error while"
            "{}:\n{}\n".format(engine["hostname"], arguments["--target"], e)
        )
        sys.exit(1)

    thingstodo = ["thingtodo"]
    with dx_session_obj.job_mode(single_thread):
        while len(dx_session_obj.jobs) > 0 or len(thingstodo) > 0:
            if len(thingstodo) > 0:
                refresh_database(
                    arguments["--vdb"],
                    arguments["--timestamp"],
                    arguments["--timestamp_type"],
                )
                thingstodo.pop()

            # get all the jobs, then inspect them
            i = 0
            for j in dx_session_obj.jobs.keys():
                job_obj = job.get(dx_session_obj.server_session, dx_session_obj.jobs[j])
                print_debug(job_obj)
                print_info(
                    "{}: Operations: {}".format(engine["hostname"], job_obj.job_state)
                )
                if job_obj.job_state in ["CANCELED", "COMPLETED", "FAILED"]:
                    # If the job is in a non-running state, remove it from the
                    # running jobs list.
                    del dx_session_obj.jobs[j]
                elif job_obj.job_state in "RUNNING":
                    # If the job is in a running state, increment the running
                    # job count.
                    i += 1

                print_info("{}: {:d} jobs running.".format(engine["hostname"], i))

            # If we have running jobs, pause before repeating the checks.
            if len(dx_session_obj.jobs) > 0:
                sleep(float(arguments["--poll"]))


def run_job():
    """
    This function runs the main_workflow aynchronously against all the servers
    specified
    """
    # Create an empty list to store threads we create.
    threads = []
    engine = None

    # If the --all argument was given, run against every engine in dxtools.conf
    if arguments["--all"]:
        print_info("Executing against all Delphix Engines in the dxtools.conf")

        try:
            # For each server in the dxtools.conf...
            for delphix_engine in dx_session_obj.dlpx_engines:
                engine = dx_session_obj[delphix_engine]
                # Create a new thread and add it to the list.
                threads.append(main_workflow(engine))

        except DlpxException as e:
            print("Error encountered in run_job():\n{}".format(e))
            sys.exit(1)

    else:
        # Else if the --engine argument was given, test to see if the engine
        # exists in dxtools.conf
        if arguments["--engine"]:
            try:
                engine = dx_session_obj.dlpx_engines[arguments["--engine"]]
                print_info(
                    "Executing against Delphix Engine: {}\n".format(
                        (arguments["--engine"])
                    )
                )

            except (DlpxException, RequestError, KeyError) as e:
                raise DlpxException(
                    "\nERROR: Delphix Engine {} cannot be "
                    "found in {}. Please check your value "
                    "and try again. Exiting.\n".format(
                        arguments["--engine"], config_file_path
                    )
                )

        else:
            # Else search for a default engine in the dxtools.conf
            for delphix_engine in dx_session_obj.dlpx_engines:
                if dx_session_obj.dlpx_engines[delphix_engine]["default"] == "true":

                    engine = dx_session_obj.dlpx_engines[delphix_engine]
                    print_info(
                        "Executing against the default Delphix Engine "
                        "in the dxtools.conf: {}".format(
                            dx_session_obj.dlpx_engines[delphix_engine]["hostname"]
                        )
                    )
                break

            if engine == None:
                raise DlpxException("\nERROR: No default engine found. Exiting")

    # run the job against the engine
    threads.append(main_workflow(engine))

    # For each thread in the list...
    for each in threads:
        # join them back together so that we wait for all threads to complete
        # before moving on
        each.join()


def time_elapsed():
    """
    This function calculates the time elapsed since the beginning of the script.
    Call this anywhere you want to note the progress in terms of time
    """
    # elapsed_minutes = round((time() - time_start)/60, +1)
    # return elapsed_minutes
    return round((time() - time_start) / 60, +1)


def main(arguments):
    # We want to be able to call on these variables anywhere in the script.
    global single_thread
    global usebackup
    global time_start
    global config_file_path
    global dx_session_obj
    global debug

    if arguments["--debug"]:
        debug = True

    try:
        dx_session_obj = GetSession()
        logging_est(arguments["--logdir"])
        print_debug(arguments)
        time_start = time()
        engine = None
        single_thread = False
        config_file_path = arguments["--config"]
        # Parse the dxtools.conf and put it into a dictionary
        dx_session_obj.get_config(config_file_path)

        # This is the function that will handle processing main_workflow for
        # all the servers.
        run_job()

        # elapsed_minutes = time_elapsed()
        print_info("script took {:.2f} minutes to get this far.".format(time_elapsed()))

    # Here we handle what we do when the unexpected happens
    except SystemExit as e:
        """
        This is what we use to handle our sys.exit(#)
        """
        sys.exit(e)

    except HttpError as e:
        """
        We use this exception handler when our connection to Delphix fails
        """
        print_exception(
            "Connection failed to the Delphix Engine"
            "Please check the ERROR message:\n{}\n"
        ).format(e)
        sys.exit(1)

    except JobError as e:
        """
        We use this exception handler when a job fails in Delphix so that
        we have actionable data
        """
        elapsed_minutes = time_elapsed()
        print_exception("A job failed in the Delphix Engine")
        print_info(
            "{} took {:.2f} minutes to get this far:\n{}\n".format(
                basename(__file__), elapsed_minutes, e
            )
        )
        sys.exit(3)

    except KeyboardInterrupt:
        """
        We use this exception handler to gracefully handle ctrl+c exits
        """
        print_debug("You sent a CTRL+C to interrupt the process")
        elapsed_minutes = time_elapsed()
        print_info(
            "{} took {:.2f} minutes to get this far\n".format(
                basename(__file__), elapsed_minutes
            )
        )

    except:
        """
        Everything else gets caught here
        """
        print_exception(sys.exc_info()[0])
        elapsed_minutes = time_elapsed()
        print_info(
            "{} took {:.2f} minutes to get this far\n".format(
                basename(__file__), elapsed_minutes
            )
        )
        sys.exit(1)


if __name__ == "__main__":
    # Grab our arguments from the doc at the top of the script
    arguments = docopt(__doc__, version=basename(__file__) + " " + VERSION)
    # Feed our arguments to the main function, and off we go!
    main(arguments)
