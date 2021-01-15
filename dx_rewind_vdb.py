#!/usr/bin/env python
# Corey Brune - Sep 2016
# This script performs a rewind of a vdb
# requirements
# pip install --upgrade setuptools pip docopt delphixpy.v1_8_0

# The below doc follows the POSIX compliant standards and allows us to use
# this doc to also define our arguments for the script.

"""Rewinds a vdb
Usage:
  dx_rewind_vdb.py (--vdb <name> [--timestamp_type <type>] [--timestamp <timepoint_semantic>])
                   [--bookmark <type>] 
                   [ --engine <identifier> --all]
                   [--debug] [--parallel <n>] [--poll <n>]
                   [--config <path_to_file>] [--logdir <path_to_file>]
  dx_rewind_vdb.py -h | --help | -v | --version

Rewinds a Delphix VDB
Examples:
    Rollback to latest snapshot using defaults:
      dx_rewind_vdb.py --vdb testVdbUF
    Rollback using a specific timestamp:
      dx_rewind_vdb.py --vdb testVdbUF --timestamp_type snapshot --timestamp 2016-11-15T11:30:17.857Z
  

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

VERSION = "v.0.2.016"


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
from delphixpy.v1_8_0.web import job
from delphixpy.v1_8_0.web.vo import OracleRollbackParameters
from delphixpy.v1_8_0.web.vo import RollbackParameters
from lib.DlpxException import DlpxException
from lib.DxLogging import logging_est
from lib.DxLogging import print_debug
from lib.DxLogging import print_exception
from lib.DxLogging import print_info
from lib.DxTimeflow import DxTimeflow
from lib.GetReferences import find_obj_by_name
from lib.GetSession import GetSession


def rewind_database(dlpx_obj, vdb_name, timestamp, timestamp_type="SNAPSHOT"):
    """
    This function performs the rewind (rollback)

    dlpx_obj: Virtualization Engine session object
    vdb_name: VDB to be rewound
    timestamp: Point in time to rewind the VDB
    timestamp_type: The type of timestamp being used for the rewind
    """

    engine_name = dlpx_obj.dlpx_engines.keys()[0]
    dx_timeflow_obj = DxTimeflow(dlpx_obj.server_session)
    container_obj = find_obj_by_name(dlpx_obj.server_session, database, vdb_name)
    # Sanity check to make sure our container object has a reference
    if container_obj.reference:
        try:
            if container_obj.virtual is not True:
                raise DlpxException(
                    "{} in engine {} is not a virtual object. "
                    "Skipping.\n".format(container_obj.name, engine_name)
                )
            elif container_obj.staging is True:
                raise DlpxException(
                    "{} in engine {} is a virtual object. "
                    "Skipping.\n".format(container_obj.name, engine_name)
                )
            elif container_obj.runtime.enabled == "ENABLED":
                print_info(
                    "\nINFO: {} Rewinding {} to {}\n".format(
                        engine_name, container_obj.name, timestamp
                    )
                )

        # This exception is raised if rewinding a vFiles VDB
        # since AppDataContainer does not have virtual, staging or
        # enabled attributes.
        except AttributeError:
            pass

        print_debug("{}: Type: {}".format(engine_name, container_obj.type))

        # If the vdb is a Oracle type, we need to use a OracleRollbackParameters
        if str(container_obj.reference).startswith("ORACLE"):
            rewind_params = OracleRollbackParameters()
        else:
            rewind_params = RollbackParameters()
        rewind_params.timeflow_point_parameters = dx_timeflow_obj.set_timeflow_point(
            container_obj, timestamp_type, timestamp
        )
        print_debug("{}: {}".format(engine_name, str(rewind_params)))
        try:
            # Rewind the VDB
            database.rollback(
                dlpx_obj.server_session, container_obj.reference, rewind_params
            )
            dlpx_obj.jobs[engine_name] = dlpx_obj.server_session.last_job
            print_info("VDB {} was rolled back.".format(container_obj.name))
        except (RequestError, HttpError, JobError) as e:
            print_exception(
                "ERROR: {} encountered an error on {}"
                " during the rewind process:\n{}".format(
                    engine_name, container_obj.name, e
                )
            )
    # Don't do anything if the database is disabled
    else:
        print_info(
            "{}: {} is not enabled. Skipping sync.".format(
                engine_name, container_obj.name
            )
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
def main_workflow(engine, dlpx_obj):
    """
    This function is where we create our main workflow.
    Use the @run_async decorator to run this function asynchronously.
    The @run_async decorator allows us to run against multiple Delphix Engine
    simultaneously

    :param engine: Dictionary of engines
    :type engine: dictionary
    :param dlpx_obj: Virtualization Engine session object
    :type dlpx_obj: lib.GetSession.GetSession
    """

    try:
        # Setup the connection to the Delphix Engine
        dlpx_obj.serversess(
            engine["ip_address"], engine["username"], engine["password"]
        )
    except DlpxException as e:
        print_exception(
            "ERROR: Engine {} encountered an error while"
            "rewinding {}:\n{}\n".format(engine["hostname"], arguments["--target"], e)
        )

    thingstodo = ["thingtodo"]
    try:
        with dlpx_obj.job_mode(single_thread):
            while len(dlpx_obj.jobs) > 0 or len(thingstodo) > 0:
                if len(thingstodo) > 0:
                    rewind_database(
                        dlpx_obj,
                        arguments["--vdb"],
                        arguments["--timestamp"],
                        arguments["--timestamp_type"],
                    )
                    thingstodo.pop()

                # get all the jobs, then inspect them
                i = 0
                for j in dlpx_obj.jobs.keys():
                    job_obj = job.get(dlpx_obj.server_session, dlpx_obj.jobs[j])
                    print_debug(job_obj)
                    print_info(
                        "{}: Refresh of {}: {}".format(
                            engine["hostname"], arguments["--vdb"], job_obj.job_state
                        )
                    )
                    if job_obj.job_state in ["CANCELED", "COMPLETED", "FAILED"]:
                        # If the job is in a non-running state, remove it
                        # from the running jobs list.
                        del dlpx_obj.jobs[j]
                    elif job_obj.job_state in "RUNNING":
                        # If the job is in a running state, increment the
                        # running job count.
                        i += 1
                    print_info("{}: {:d} jobs running.".format(engine["hostname"], i))
                    # If we have running jobs, pause before repeating the
                    # checks.
                    if len(dlpx_obj.jobs) > 0:
                        sleep(float(arguments["--poll"]))
    except (DlpxException, RequestError, JobError, HttpError) as e:
        print_exception("Error in dx_rewind_vdb: {}\n{}".format(engine["hostname"], e))
        sys.exit(1)


def time_elapsed(time_start):
    """
    This function calculates the time elapsed since the beginning of the script.
    Call this anywhere you want to note the progress in terms of time

    time_start: float containing start time of the script.
    """
    return round((time() - time_start) / 60, +1)


def run_job(dlpx_obj, config_file_path):
    """
    This function runs the main_workflow aynchronously against all the
    servers specified

    :param dlpx_obj: Virtualization Engine session object
    :type dlpx_obj: lib.GetSession.GetSession
    :param config_file_path: string containing path to configuration file.
    :type config_file_path: str
    """

    # Create an empty list to store threads we create.
    threads = []
    engine = None

    # If the --all argument was given, run against every engine in dxtools.conf
    if arguments["--all"]:
        print_info("Executing against all Delphix Engines in the dxtools.conf")
        try:
            # For each server in the dxtools.conf...
            for delphix_engine in dlpx_obj.dlpx_engines:
                engine = dlpx_obj.dlpx_engines[delphix_engine]
                # Create a new thread and add it to the list.
                threads.append(main_workflow(engine, dlpx_obj))
        except DlpxException as e:
            print_exception("Error encountered in run_job():\n{}".format(e))
            sys.exit(1)

    elif arguments["--all"] is False:
        # Else if the --engine argument was given, test to see if the engine
        # exists in dxtools.conf
        if arguments["--engine"]:
            try:
                engine = dlpx_obj.dlpx_engines[arguments["--engine"]]
                print_info(
                    "Executing against Delphix Engine: {}\n".format(
                        arguments["--engine"]
                    )
                )
            except (DlpxException, RequestError, KeyError):
                raise DlpxException(
                    "\nERROR: Delphix Engine {} cannot be "
                    "found in {}. Please check your value and"
                    " try again. Exiting.\n".format(
                        arguments["--engine"], config_file_path
                    )
                )
        else:
            # Else search for a default engine in the dxtools.conf
            for delphix_engine in dlpx_obj.dlpx_engines:
                if dlpx_obj.dlpx_engines[delphix_engine]["default"] == "true":
                    engine = dlpx_obj.dlpx_engines[delphix_engine]
                    print_info(
                        "Executing against the default Delphix Engine "
                        "in the dxtools.conf: {}".format(
                            dlpx_obj.dlpx_engines[delphix_engine]["hostname"]
                        )
                    )
                break

            if engine is None:
                raise DlpxException("\nERROR: No default engine found. Exiting")

        # run the job against the engine
        threads.append(main_workflow(engine, dlpx_obj))

    # For each thread in the list...
    for each in threads:
        # join them back together so that we wait for all threads to complete
        # before moving on
        each.join()


def main():
    # We want to be able to call on these variables anywhere in the script.
    global single_thread
    global debug

    time_start = time()
    single_thread = False

    try:
        dx_session_obj = GetSession()
        logging_est(arguments["--logdir"])
        print_debug(arguments)
        config_file_path = arguments["--config"]
        # Parse the dxtools.conf and put it into a dictionary
        dx_session_obj.get_config(config_file_path)

        # This is the function that will handle processing main_workflow for
        # all the servers.
        run_job(dx_session_obj, config_file_path)

        elapsed_minutes = time_elapsed(time_start)
        print_info(
            "script took {:.2f} minutes to get this far.".format(elapsed_minutes)
        )

    # Here we handle what we do when the unexpected happens
    except SystemExit as e:
        # This is what we use to handle our sys.exit(#)
        sys.exit(e)

    except DlpxException as e:
        # We use this exception handler when an error occurs in a function call.
        print_exception(
            "ERROR: Please check the ERROR message below:\n" "{}".format(e.message)
        )
        sys.exit(2)

    except HttpError as e:
        # We use this exception handler when our connection to Delphix fails
        print_exception(
            "ERROR: Connection failed to the Delphix Engine. Please"
            "check the ERROR message below:\n{}".format(e.message)
        )
        sys.exit(2)

    except JobError as e:
        # We use this exception handler when a job fails in Delphix so that we
        # have actionable data
        print_exception("A job failed in the Delphix Engine:\n{}".format(e.job))
        elapsed_minutes = time_elapsed(time_start)
        print_exception(
            "{} took {:.2f} minutes to get this far".format(
                basename(__file__), elapsed_minutes
            )
        )
        sys.exit(3)

    except KeyboardInterrupt:
        # We use this exception handler to gracefully handle ctrl+c exits
        print_debug("You sent a CTRL+C to interrupt the process")
        elapsed_minutes = time_elapsed(time_start)
        print_info(
            "{} took {:.2f} minutes to get this far".format(
                basename(__file__), elapsed_minutes
            )
        )
    except:
        # Everything else gets caught here
        print_exception("{}\n{}".format(sys.exc_info()[0], traceback.format_exc()))
        elapsed_minutes = time_elapsed(time_start)
        print_info(
            "{} took {:.2f} minutes to get this far".format(
                basename(__file__), elapsed_minutes
            )
        )
        sys.exit(1)


if __name__ == "__main__":
    # Grab our arguments from the doc at the top of the script
    arguments = docopt(__doc__, version=basename(__file__) + " " + VERSION)

    # Feed our arguments to the main function, and off we go!
    main()
