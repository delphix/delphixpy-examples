#!/usr/bin/env python
# Corey Brune - Oct 2016
# This script starts or stops a VDB
# requirements
# pip install docopt delphixpy

# The below doc follows the POSIX compliant standards and allows us to use
# this doc to also define our arguments for the script.
"""List all VDBs or Start, stop, enable, disable a VDB
Usage:
  dx_operations_vdb.py (--vdb <name> [--stop | --start | --enable | --disable] | --list | --all_dbs <name>)
                  [-d <identifier> | --engine <identifier> | --all]
                  [--force] [--debug] [--parallel <n>] [--poll <n>]
                  [--config <path_to_file>] [--logdir <path_to_file>]
  dx_operations_vdb.py -h | --help | -v | --version
List all VDBs, start, stop, enable, disable a VDB

Examples:
  dx_operations_vdb.py --engine landsharkengine --vdb testvdb --stop
  dx_operations_vdb.py --vdb testvdb --start
  dx_operations_vdb.py --all_dbs enable
  dx_operations_vdb.py --all_dbs disable
  dx_operations_vdb.py --list

Options:
  --vdb <name>              Name of the VDB to stop or start
  --start                   Stop the VDB
  --stop                    Stop the VDB
  --all_dbs <name>          Enable or disable all dSources and VDBs
  --list                    List all databases from an engine
  --enable                  Enable the VDB
  --disable                 Disable the VDB
  -d <identifier>           Identifier of Delphix engine in dxtools.conf.
  --engine <type>           Alt Identifier of Delphix engine in dxtools.conf.
  --all                     Run against all engines.
  --force                   Do not clean up target in VDB disable operations
  --debug                   Enable debug logging
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./dxtools.conf]
  --logdir <path_to_file>    The path to the logfile you want to use.
                            [default: ./dx_operations_vdb.log]
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
from delphixpy.v1_8_0.web import job
from delphixpy.v1_8_0.web import source
from delphixpy.v1_8_0.web.capacity import consumer
from delphixpy.v1_8_0.web.source import source
from delphixpy.v1_8_0.web.vo import SourceDisableParameters
from lib.DlpxException import DlpxException
from lib.DxLogging import logging_est
from lib.DxLogging import print_debug
from lib.DxLogging import print_exception
from lib.DxLogging import print_info
from lib.GetReferences import find_all_objects
from lib.GetReferences import find_obj_by_name
from lib.GetReferences import find_obj_list
from lib.GetSession import GetSession

VERSION = "v.0.3.018"


def dx_obj_operation(dlpx_obj, vdb_name, operation):
    """
    Function to start, stop, enable or disable a VDB

    :param dlpx_obj: Virtualization Engine session object
    :type dlpx_obj: lib.GetSession.GetSession
    :param vdb_name: Name of the object to stop/start/enable/disable
    :type vdb_name: str
    :param operation: enable or disable dSources and VDBs
    :type operation: str
    """

    print_debug("Searching for {} reference.\n".format(vdb_name))
    engine_name = dlpx_obj.dlpx_engines.keys()[0]
    vdb_obj = find_obj_by_name(dlpx_obj.server_session, source, vdb_name)
    try:
        if vdb_obj:
            if operation == "start":
                source.start(dlpx_obj.server_session, vdb_obj.reference)
            elif operation == "stop":
                source.stop(dlpx_obj.server_session, vdb_obj.reference)
            elif operation == "enable":
                source.enable(dlpx_obj.server_session, vdb_obj.reference)
            elif operation == "disable":
                source.disable(dlpx_obj.server_session, vdb_obj.reference)
            elif operation == "force_disable":
                disable_params = SourceDisableParameters()
                disable_params.attempt_cleanup = False
                source.disable(
                    dlpx_obj.server_session, vdb_obj.reference, disable_params
                )
            dlpx_obj.jobs[engine_name] = dlpx_obj.server_session.last_job
    except (RequestError, HttpError, JobError, AttributeError) as e:
        print_exception(
            "An error occurred while performing {} on {}:\n"
            "{}".format(operation, vdb_name, e)
        )
    print("{} was successfully performed on {}.".format(operation, vdb_name))


def all_databases(dlpx_obj, operation):
    """
    Enable or disable all dSources and VDBs on an engine

    :param dlpx_obj: Virtualization Engine session object
    :type dlpx_obj: lib.GetSession.GetSession
    :param operation: enable or disable dSources and VDBs
    :type operation: str
    """

    for db in database.get_all(dlpx_obj.server_session):
        try:
            dx_obj_operation(dlpx_obj, db.name, operation)
        except (RequestError, HttpError, JobError):
            pass
        print("{} {}\n".format(operation, db.name))
        sleep(2)


def list_databases(dlpx_obj):
    """
    Function to list all databases and stats for an engine

    :param dlpx_obj: Virtualization Engine session object
    :type dlpx_obj: lib.GetSession.GetSession
    """

    source_stats_lst = find_all_objects(dlpx_obj.server_session, source)
    is_dSource = None
    try:
        for db_stats in find_all_objects(dlpx_obj.server_session, consumer):
            source_stats = find_obj_list(source_stats_lst, db_stats.name)
            if source_stats is not None:
                if source_stats.virtual is False:
                    db_size = source_stats.runtime.database_size / 1024 / 1024 / 1024
                    print(
                        "name: {}, provision container: dSource, disk usage: "
                        "{:.2f}GB, Size of Snapshots: {:.2f}GB, "
                        "dSource Size: {:.2f}GB, Log Size: {:.2f}MB,"
                        "Enabled: {}, Status: {}".format(
                            str(db_stats.name),
                            db_stats.breakdown.active_space / 1024 / 1024 / 1024,
                            db_stats.breakdown.sync_space / 1024 / 1024 / 1024,
                            source_stats.runtime.database_size / 1024 / 1024 / 1024,
                            db_stats.breakdown.log_space / 1024 / 1024,
                            source_stats.runtime.enabled,
                            source_stats.runtime.status,
                        )
                    )
                elif source_stats.virtual is True:
                    print(
                        "name: {}, provision container: {}, disk usage: "
                        "{:.2f}GB, Size of Snapshots: {:.2f}GB, "
                        "Log Size: {:.2f}MB, Enabled: {}, "
                        "Status: {}".format(
                            str(db_stats.name),
                            db_stats.parent,
                            db_stats.breakdown.active_space / 1024 / 1024 / 1024,
                            db_stats.breakdown.sync_space / 1024 / 1024 / 1024,
                            db_stats.breakdown.log_space / 1024 / 1024,
                            source_stats.runtime.enabled,
                            source_stats.runtime.status,
                        )
                    )
            elif source_stats is None:
                print(
                    "name: {},provision container: {},database disk "
                    "usage: {:.2f} GB,Size of Snapshots: {:.2f} GB,"
                    "Could not find source information. This could be a "
                    "result of an unlinked object".format(
                        str(db_stats.name),
                        str(db_stats.parent),
                        db_stats.breakdown.active_space / 1024 / 1024 / 1024,
                        db_stats.breakdown.sync_space / 1024 / 1024 / 1024,
                    )
                )
    except (RequestError, JobError, AttributeError, DlpxException) as err:
        print("An error occurred while listing databases: {}".format(err))


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
            "{}:\n{}\n".format(engine["hostname"], arguments["--target"], e)
        )
        sys.exit(1)

    thingstodo = ["thingtodo"]
    try:
        with dlpx_obj.job_mode(single_thread):
            while len(dlpx_obj.jobs) > 0 or len(thingstodo) > 0:
                if len(thingstodo) > 0:
                    if arguments["--start"]:
                        dx_obj_operation(dlpx_obj, arguments["--vdb"], "start")
                    elif arguments["--stop"]:
                        dx_obj_operation(dlpx_obj, arguments["--vdb"], "stop")
                    elif arguments["--enable"]:
                        dx_obj_operation(dlpx_obj, arguments["--vdb"], "enable")
                    elif arguments["--disable"]:
                        if arguments["--force"]:
                            dx_obj_operation(
                                dlpx_obj, arguments["--vdb"], "force_disable"
                            )
                        else:
                            dx_obj_operation(dlpx_obj, arguments["--vdb"], "disable")
                    elif arguments["--list"]:
                        list_databases(dlpx_obj)
                    elif arguments["--all_dbs"]:
                        all_databases(dlpx_obj, arguments["--all_dbs"])
                    thingstodo.pop()
                # get all the jobs, then inspect them
                i = 0
                for j in dlpx_obj.jobs.keys():
                    job_obj = job.get(dlpx_obj.server_session, dlpx_obj.jobs[j])
                    print_debug(job_obj)
                    print_info(
                        "{}: Running JS Bookmark: {}".format(
                            engine["hostname"], job_obj.job_state
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
        print_exception("Error in js_bookmark: {}\n{}".format(engine["hostname"], e))
        sys.exit(1)


def time_elapsed(time_start):
    """
    This function calculates the time elapsed since the beginning of the script.
    Call this anywhere you want to note the progress in terms of time

    :param time_start:  start time of the script.
    :type time_start: float
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
