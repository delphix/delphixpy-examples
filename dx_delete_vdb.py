#!/usr/bin/env python
# Adam Bowen - Apr 2016
# This script deletes a vdb
# requirements
# pip install docopt delphixpy

# The below doc follows the POSIX compliant standards and allows us to use
# this doc to also define our arguments for the script. This thing is brilliant.
"""Delete a VDB

Usage:
  dx_delete_db.py (--group <name> [--name <name>] | --all_dbs )
                  [-d <identifier> | --engine <identifier> | --all]
                  [--usebackup] [--debug] [--parallel <n>] [--poll <n>]
                  [--config <path_to_file>] [--logdir <path_to_file>]
  dx_delete_db.py (--host <name> [--group <name>] [--object_type <type>]
                  | --object_type <name> [--group <name>] [--host <type>] )
                  [-d <identifier> | --engine <identifier> | --all]
                  [--usebackup] [--debug] [--parallel <n>] [--poll <n>]
                  [--config <path_to_file>] [--logdir <path_to_file>]
  dx_delete_db.py -h | --help | -v | --version

Delete a VDB

Examples:
  dx_delete_db.py --group "Sources" --object_type dsource --usebackup
  dx_delete_db.py --name "Employee Oracle 11G DB"
  dx_delete_db.py --host LINUXSOURCE --parallel 2 --usebackup
  dx_delete_db.py --host LINUXSOURCE --parallel 4 --usebackup --debug -d landsharkengine



Options:
  -d <identifier>           Identifier of Delphix engine in dxtools.conf.
  --engine <type>           Alt Identifier of Delphix engine in dxtools.conf.
  --all                     Run against all engines.
  --all_dbs                 Run against all database objects
  --name <name>             Name of object in Delphix to execute against.
  --group <name>            Name of group in Delphix to execute against.
  --host <name>             Name of environment in Delphix to execute against.
  --object_type <obj_type>  dsource or vdb.
  --usebackup               Snapshot using "Most Recent backup".
                            Available for MSSQL and ASE only.
  --debug                   Enable debug logging
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./dxtools.conf]
  --logdir <path_to_file>    The path to the logfile you want to use.
                            [default: ./dx_snapshot_db.log]
  -h --help                 Show this screen.
  -v --version              Show version.

"""
from __future__ import print_function

import json
import logging
import signal
import sys
import time
import traceback
from multiprocessing import Process
from os.path import basename
from time import sleep
from time import time

from docopt import docopt

from delphixpy.v1_6_0 import job_context
from delphixpy.v1_6_0.delphix_engine import DelphixEngine
from delphixpy.v1_6_0.exceptions import HttpError
from delphixpy.v1_6_0.exceptions import JobError
from delphixpy.v1_6_0.web import database
from delphixpy.v1_6_0.web import environment
from delphixpy.v1_6_0.web import group
from delphixpy.v1_6_0.web import job
from delphixpy.v1_6_0.web import source
from delphixpy.v1_6_0.web import user
from delphixpy.v1_6_0.web.vo import ASELatestBackupSyncParameters
from delphixpy.v1_6_0.web.vo import ASENewBackupSyncParameters
from delphixpy.v1_6_0.web.vo import ASESpecificBackupSyncParameters
from delphixpy.v1_6_0.web.vo import MSSqlSyncParameters

VERSION = "v.0.0.001"


def find_obj_by_name(engine, server, f_class, obj_name):
    """
    Function to find objects by name and object class, and return object's reference as a string
    You might use this function to find objects like groups.
    """
    print_debug(
        engine["hostname"]
        + ": Searching objects in the "
        + f_class.__name__
        + ' class\n   for one named "'
        + obj_name
        + '"'
    )
    obj_ref = ""

    all_objs = f_class.get_all(server)
    for obj in all_objs:
        if obj.name == obj_name:
            print_debug(engine["hostname"] + ": Found a match " + str(obj.reference))
            return obj


def find_all_databases_by_group_name(
    engine, server, group_name, exclude_js_container=False
):
    """
    Easy way to quickly find databases by group name
    """

    # First search groups for the name specified and return its reference
    group_obj = find_obj_by_name(engine, server, group, group_name)
    if group_obj:
        databases = database.get_all(
            server,
            group=group_obj.reference,
            no_js_container_data_source=exclude_js_container,
        )
        return databases


def find_database_by_name_and_group_name(engine, server, group_name, database_name):

    databases = find_all_databases_by_group_name(engine, server, group_name)

    for each in databases:
        if each.name == database_name:
            print_debug(engine["hostname"] + ": Found a match " + str(each.reference))
            return each
    print_info('Unable to find "' + database_name + '" in ' + group_name)


def find_source_by_database(engine, server, database_obj):
    # The source tells us if the database is enabled/disables, virtual, vdb/dSource, or is a staging database.
    source_obj = source.get_all(server, database=database_obj.reference)
    # We'll just do a little sanity check here to ensure we only have a 1:1 result.
    if len(source_obj) == 0:
        print_error(
            engine["hostname"]
            + ": Did not find a source for "
            + database_obj.name
            + ". Exiting"
        )
        sys.exit(1)
    elif len(source_obj) > 1:
        print_error(
            engine["hostname"]
            + ": More than one source returned for "
            + database_obj.name
            + ". Exiting"
        )
        print_error(source_obj)
        sys.exit(1)
    return source_obj


def get_config(config_file_path):
    """
    This function reads in the dxtools.conf file
    """
    # First test to see that the file is there and we can open it
    try:
        config_file = open(config_file_path).read()
    except:
        print_error(
            "Was unable to open "
            + config_file_path
            + ". Please check the path and permissions, then try again."
        )
        sys.exit(1)
    # Now parse the file contents as json and turn them into a python dictionary, throw an error if it isn't proper json
    try:
        config = json.loads(config_file)
    except:
        print_error(
            "Was unable to read "
            + config_file_path
            + " as json. Please check file in a json formatter and try again."
        )
        sys.exit(1)
    # Create a dictionary of engines (removing the data node from the dxtools.json, for easier parsing)
    delphix_engines = {}
    for each in config["data"]:
        delphix_engines[each["hostname"]] = each
    print_debug(delphix_engines)
    return delphix_engines


def logging_est(logfile_path):
    """
    Establish Logging
    """
    global debug
    logging.basicConfig(
        filename=logfile_path,
        format="%(levelname)s:%(asctime)s:%(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    print_info("Welcome to " + basename(__file__) + ", version " + VERSION)
    global logger
    debug = arguments["--debug"]
    logger = logging.getLogger()
    if debug == True:
        logger.setLevel(10)
        print_info("Debug Logging is enabled.")


def job_mode(server):
    """
    This function tells Delphix how to execute jobs, based on the single_thread variable at the beginning of the file
    """
    # Synchronously (one at a time)
    if single_thread == True:
        job_m = job_context.sync(server)
        print_debug("These jobs will be executed synchronously")
    # Or asynchronously
    else:
        job_m = job_context.asyncly(server)
        print_debug("These jobs will be executed asynchronously")
    return job_m


def job_wait():
    """
    This job stops all work in the thread/process until jobs are completed.
    """
    # Grab all the jos on the server (the last 25, be default)
    all_jobs = job.get_all(server)
    # For each job in the list, check to see if it is running (not ended)
    for jobobj in all_jobs:
        if not (jobobj.job_state in ["CANCELED", "COMPLETED", "FAILED"]):
            print_debug(
                "Waiting for "
                + jobobj.reference
                + " (currently: "
                + jobobj.job_state
                + ") to finish running against the container"
            )
            # If so, wait
            job_context.wait(server, jobobj.reference)


def on_exit(sig, func=None):
    """
    This function helps us end cleanly and with exit codes
    """
    print_info("Shutdown Command Received")
    print_info("Shutting down " + basename(__file__))
    sys.exit(0)


def print_debug(print_obj):
    """
    Call this function with a log message to prefix the message with DEBUG
    """
    try:
        if debug == True:
            print("DEBUG: " + str(print_obj))
            logging.debug(str(print_obj))
    except:
        pass


def print_error(print_obj):
    """
    Call this function with a log message to prefix the message with ERROR
    """
    print("ERROR: " + str(print_obj))
    logging.error(str(print_obj))


def print_info(print_obj):
    """
    Call this function with a log message to prefix the message with INFO
    """
    print("INFO: " + str(print_obj))
    logging.info(str(print_obj))


def print_warning(print_obj):
    """
    Call this function with a log message to prefix the message with WARNING
    """
    print("WARNING: " + str(print_obj))
    logging.warning(str(print_obj))


def serversess(f_engine_address, f_engine_username, f_engine_password):
    """
    Function to setup the session with the Delphix Engine
    """
    server_session = DelphixEngine(
        f_engine_address, f_engine_username, f_engine_password, "DOMAIN"
    )
    return server_session


def set_exit_handler(func):
    """
    This function helps us set the correct exit code
    """
    signal.signal(signal.SIGTERM, func)


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
    This function is where the main workflow resides.
    Use the @run_async decorator to run this function asynchronously.
    This allows us to run against multiple Delphix Engine simultaneously
    """

    # Pull out the values from the dictionary for this engine
    engine_address = engine["ip_address"]
    engine_username = engine["username"]
    engine_password = engine["password"]
    # Establish these variables as empty for use later
    databases = []
    environment_obj = None
    source_objs = None
    jobs = {}

    # Setup the connection to the Delphix Engine
    server = serversess(engine_address, engine_username, engine_password)

    # If an environment/server was specified
    if host_name:
        print_debug(engine["hostname"] + ": Getting environment for " + host_name)
        # Get the environment object by the hostname
        environment_obj = find_obj_by_name(engine, server, environment, host_name)
        if environment_obj != None:
            # Get all the sources running on the server
            env_source_objs = source.get_all(
                server, environment=environment_obj.reference
            )
            # If the server doesn't have any objects, exit.
            if env_source_objs == None:
                print_error(host_name + "does not have any objects. Exiting")
                sys.exit(1)
            # If we are only filtering by the server, then put those objects in the main list for processing
            if not (arguments["--group"] and database_name):
                source_objs = env_source_objs
                all_dbs = database.get_all(server, no_js_container_data_source=False)
                databases = []
                for source_obj in source_objs:
                    if source_obj.staging == False and source_obj.virtual == True:
                        database_obj = database.get(server, source_obj.container)
                        if database_obj in all_dbs:
                            databases.append(database_obj)
        else:
            print_error(
                engine["hostname"]
                + ":No environment found for "
                + host_name
                + ". Exiting"
            )
            sys.exit(1)
    # If we specified a specific database by name....
    if arguments["--name"]:
        # Get the database object from the name
        database_obj = find_database_by_name_and_group_name(
            engine, server, arguments["--group"], arguments["--name"]
        )
        if database_obj:
            databases.append(database_obj)
    # Else if we specified a group to filter by....
    elif arguments["--group"]:
        print_debug(
            engine["hostname"] + ":Getting databases in group " + arguments["--group"]
        )
        # Get all the database objects in a group.
        databases = find_all_databases_by_group_name(
            engine, server, arguments["--group"]
        )
    # Else, if we said all vdbs ...
    elif arguments["--all_dbs"] and not arguments["--host"]:
        # Grab all databases
        databases = database.get_all(server, no_js_container_data_source=False)
    elif arguments["--object_type"] and not arguments["--host"]:
        databases = database.get_all(server)
    if not databases or len(databases) == 0:
        print_error("No databases found with the criterion specified")
        return
    # reset the running job count before we begin
    i = 0
    with job_mode(server):
        # While there are still running jobs or databases still to process....
        while len(jobs) > 0 or len(databases) > 0:
            # While there are databases still to process and we are still under
            # the max simultaneous jobs threshold (if specified)
            while len(databases) > 0 and (
                arguments["--parallel"] == None or i < int(arguments["--parallel"])
            ):
                # Give us the next database in the list, and remove it from the list
                database_obj = databases.pop()
                # Get the source of the database.
                # The source tells us if the database is enabled/disables, virtual, vdb/dSource, or is a staging database.
                source_obj = find_source_by_database(engine, server, database_obj)
                # If we applied the environment/server filter AND group filter, find the intersecting matches
                if environment_obj != None and (arguments["--group"]):
                    match = False
                    for env_source_obj in env_source_objs:
                        if source_obj[0].reference in env_source_obj.reference:
                            match = True
                            break
                    if match == False:
                        print_error(
                            engine["hostname"]
                            + ": "
                            + database_obj.name
                            + " does not exist on "
                            + host_name
                            + ". Exiting"
                        )
                        return
                # Snapshot the database
                delete_job = delete_database(
                    engine,
                    server,
                    jobs,
                    source_obj[0],
                    database_obj,
                    arguments["--object_type"],
                )
                # If delete_job has any value, then we know that a job was initiated.
                if delete_job:
                    # increment the running job count
                    i += 1
            # Check to see if we are running at max parallel processes, and report if so.
            if arguments["--parallel"] != None and i >= int(arguments["--parallel"]):
                print_info(engine["hostname"] + ": Max jobs reached (" + str(i) + ")")
            # reset the running jobs counter, as we are about to update the count from the jobs report.
            i = update_jobs_dictionary(engine, server, jobs)
            print_info(
                engine["hostname"]
                + ": "
                + str(i)
                + " jobs running. "
                + str(len(databases))
                + " jobs waiting to run"
            )
            # If we have running jobs, pause before repeating the checks.
            if len(jobs) > 0:
                sleep(float(arguments["--poll"]))


def run_job(engine):
    """
    This function runs the main_workflow aynchronously against all the servers specified
    """
    # Create an empty list to store threads we create.
    threads = []
    # If the --all argument was given, run against every engine in dxtools.conf
    if arguments["--all"]:
        print_info("Executing against all Delphix Engines in the dxtools.conf")
        # For each server in the dxtools.conf...
        for delphix_engine in dxtools_objects:
            engine = dxtools_objects[delphix_engine]
            # Create a new thread and add it to the list.
            threads.append(main_workflow(engine))
    else:
        # Else if the --engine argument was given, test to see if the engine exists in dxtools.conf
        if arguments["--engine"]:
            try:
                engine = dxtools_objects[arguments["--engine"]]
                print_info("Executing against Delphix Engine: " + arguments["--engine"])
            except:
                print_error(
                    'Delphix Engine "'
                    + arguments["--engine"]
                    + '" cannot be found in '
                    + config_file_path
                )
                print_error("Please check your value and try again. Exiting")
                sys.exit(1)
        # Else if the -d argument was given, test to see if the engine exists in dxtools.conf
        elif arguments["-d"]:
            try:
                engine = dxtools_objects[arguments["-d"]]
                print_info("Executing against Delphix Engine: " + arguments["-d"])
            except:
                print_error(
                    'Delphix Engine "'
                    + arguments["-d"]
                    + '" cannot be found in '
                    + config_file_path
                )
                print_error("Please check your value and try again. Exiting")
                sys.exit(1)
        else:
            # Else search for a default engine in the dxtools.conf
            for delphix_engine in dxtools_objects:
                if dxtools_objects[delphix_engine]["default"] == "true":
                    engine = dxtools_objects[delphix_engine]
                    print_info(
                        "Executing against the default Delphix Engine in the dxtools.conf: "
                        + dxtools_objects[delphix_engine]["hostname"]
                    )
                    break
            if engine == None:
                print_error("No default engine found. Exiting")
                sys.exit(1)
        # run the job against the engine
        threads.append(main_workflow(engine))

    # For each thread in the list...
    for each in threads:
        # join them back together so that we wait for all threads to complete before moving on
        each.join()


def delete_database(engine, server, jobs, source_obj, container_obj, obj_type=None):
    """
    This function
    FYI - Snapshot is also called sync
    """
    # Sanity check to make sure our source object has a reference
    if source_obj.reference != None:
        # If we specified the --object_type flag, ensure this source is a match. Skip, if not.
        if obj_type != None and (
            (obj_type.lower() == "vdb" and source_obj.virtual != True)
            or (obj_type.lower() == "dsource" and source_obj.virtual != False)
        ):
            print_warning(
                engine["hostname"]
                + ": "
                + container_obj.name
                + " is not a "
                + obj_type.lower()
                + ". Skipping sync"
            )
        # Ensure this source is not a staging database. We can't act upon those.
        elif source_obj.staging == True:
            print_warning(
                engine["hostname"]
                + ": "
                + container_obj.name
                + " is a staging database. Skipping."
            )
        # Ensure the source is enabled. We can't snapshot disabled databases.
        else:
            print_info(engine["hostname"] + ": Deleting " + container_obj.name)
            print_debug(engine["hostname"] + ": Type: " + source_obj.type)
            print_debug(engine["hostname"] + ": " + source_obj.type)
            # Delete it
            database.delete(server, container_obj.reference)
            # Add the job into the jobs dictionary so we can track its progress
            jobs[container_obj] = server.last_job
            # return the job object to the calling statement so that we can tell if a job was created or not (will return None, if no job)
            return server.last_job


def time_elapsed():
    """
    This function calculates the time elapsed since the beginning of the script.
    Call this anywhere you want to note the progress in terms of time
    """
    elapsed_minutes = round((time() - time_start) / 60, +1)
    return elapsed_minutes


def update_jobs_dictionary(engine, server, jobs):
    """
    This function checks each job in the dictionary and updates its status or removes it if the job is complete.
    Return the number of jobs still running.
    """
    # Establish the running jobs counter, as we are about to update the count from the jobs report.
    i = 0
    # get all the jobs, then inspect them
    for j in jobs.keys():
        job_obj = job.get(server, jobs[j])
        print_debug(engine["hostname"] + ": " + str(job_obj))
        print_info(engine["hostname"] + ": " + j.name + ": " + job_obj.job_state)

        if job_obj.job_state in ["CANCELED", "COMPLETED", "FAILED"]:
            # If the job is in a non-running state, remove it from the running jobs list.
            del jobs[j]
        else:
            # If the job is in a running state, increment the running job count.
            i += 1
    return i


def main(argv):
    # We want to be able to call on these variables anywhere in the script.
    global single_thread
    global usebackup
    global time_start
    global host_name
    global database_name
    global config_file_path
    global dxtools_objects

    try:
        # Declare globals that will be used throughout the script.
        logging_est(arguments["--logdir"])
        print_debug(arguments)
        time_start = time()
        engine = None
        single_thread = False
        usebackup = arguments["--usebackup"]
        database_name = arguments["--name"]
        host_name = arguments["--host"]
        config_file_path = arguments["--config"]
        # Parse the dxtools.conf and put it into a dictionary
        dxtools_objects = get_config(config_file_path)

        # This is the function that will handle processing main_workflow for all the servers.
        run_job(engine)

        elapsed_minutes = time_elapsed()
        print_info("script took " + str(elapsed_minutes) + " minutes to get this far.")

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
        print_error("Connection failed to the Delphix Engine")
        print_error("Please check the ERROR message below")
        print_error(e.message)
        sys.exit(2)
    except JobError as e:
        """
        We use this exception handler when a job fails in Delphix so that we have actionable data
        """
        print_error("A job failed in the Delphix Engine")
        print_error(e.job)
        elapsed_minutes = time_elapsed()
        print_info(
            basename(__file__)
            + " took "
            + str(elapsed_minutes)
            + " minutes to get this far."
        )
        sys.exit(3)
    except KeyboardInterrupt:
        """
        We use this exception handler to gracefully handle ctrl+c exits
        """
        print_debug("You sent a CTRL+C to interrupt the process")
        elapsed_minutes = time_elapsed()
        print_info(
            basename(__file__)
            + " took "
            + str(elapsed_minutes)
            + " minutes to get this far."
        )
    except:
        """
        Everything else gets caught here
        """
        print_error(sys.exc_info()[0])
        print_error(traceback.format_exc())
        elapsed_minutes = time_elapsed()
        print_info(
            basename(__file__)
            + " took "
            + str(elapsed_minutes)
            + " minutes to get this far."
        )
        sys.exit(1)


if __name__ == "__main__":
    # Grab our arguments from the doc at the top of the script
    arguments = docopt(__doc__, version=basename(__file__) + " " + VERSION)
    # I added this below condition to account for my --name | or AT LEAST ONE OF --group  --host --object_type
    # I couldn't quite sort it out with docopt. Maybe I'm just dense today.
    # Anyway, if none of the four options are given, print the __doc__ and exit.
    if (
        not (arguments["--name"])
        and not (arguments["--group"])
        and not (arguments["--host"])
        and not (arguments["--object_type"])
        and not (arguments["--all_dbs"])
    ):
        print(__doc__)
        sys.exit()
    # Feed our arguments to the main function, and off we go!
    print(arguments)
    main(arguments)
