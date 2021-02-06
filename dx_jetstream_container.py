#!/usr/bin/env python
# Adam Bowen - Jun 2016
# dx_jetstream_container.py
# Use this file as a starter for your python scripts, if you like
# requirements
# pip install docopt delphixpy

# The below doc follows the POSIX compliant standards and allows us to use
# this doc to also define our arguments for the script. This thing is brilliant.
"""Perform routine operations on Jetstream containers

Usage:
  dx_jetstream_container.py --template <name> (--container <name> | --all_containers )
                  --operation <name> [-d <identifier> | --engine <identifier> | --all]
                  [--bookmark_name <name>] [--bookmark_tags <tags>] [--bookmark_shared <bool>]
                  [--debug] [--parallel <n>] [--poll <n>]
                  [--config <path_to_file>] [--logdir <path_to_file>]
  dx_jetstream_container.py -h | --help | -v | --version

Perform routine operations on a Jetstream Container

Examples:
  dx_jetstream_container.py --operation refresh --template "Masked SugarCRM Application" --container "Sugar Automated Testing Container"
  dx_jetstream_container.py --operation reset --template "Masked SugarCRM Application" --all_containers
  dx_jetstream_container.py --template "Masked SugarCRM Application" --container "Sugar Automated Testing Container" --operation bookmark --bookmark_name "Testing" --bookmark_tags "one,two,three" --bookmark_shared true

Options:
  -d <identifier>           Identifier of Delphix engine in dxtools.conf.
  --engine <type>           Alt Identifier of Delphix engine in dxtools.conf.
  --all                     Run against all engines.
  --all_containers          Run against all jetstream containers
  --template <name>         Name of Jetstream template to execute against.
  --container <name>        Name of Jetstream container to execute against.
  --operation <name>        Name of the operation to execute
                            Can be one of:
                            start, stop, recover, refresh, reset, bookmark
  --bookmark_name <name>    Name of the bookmark to create
                            (only valid with "--operation bookmark")
  --bookmark_tags <tags>    Comma-delimited list to tag the bookmark
                            (only valid with "--operation bookmark")
  --bookmark_shared <bool>  Share bookmark: true/false
                            [default: false]
  --host <name>             Name of environment in Delphix to execute against.
  --debug                   Enable debug logging
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./dxtools.conf]
  --logdir <path_to_file>    The path to the logfile you want to use.
                            [default: ./dx_jetstream_container_refresh.log]
  -h --help                 Show this screen.
  -v --version              Show version.

"""
from __future__ import print_function

import json
import logging
import signal
import sys
import threading
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
from delphixpy.v1_6_0.web import jetstream
from delphixpy.v1_6_0.web import job
from delphixpy.v1_6_0.web.vo import JSBookmark
from delphixpy.v1_6_0.web.vo import JSBookmarkCreateParameters
from delphixpy.v1_6_0.web.vo import JSTimelinePointLatestTimeInput

VERSION = "v.0.0.005"


# from delphixpy.v1_6_0.web.vo import


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
    # from threading import Thread
    from functools import wraps

    @wraps(func)
    def async_func(*args, **kwargs):
        func_hl = threading.Thread(target=func, args=args, kwargs=kwargs)
        func_hl.start()
        return func_hl

    return async_func


@run_async
def container_bookmark(
    engine, server, container_obj, bookmark_name, bookmark_shared, tags
):
    """This function bookmarks the current branch on the container"""
    # But first, let's make sure it is in a CONSISTENT state
    container_recover(engine, server, container_obj)
    # Next let's make sure it is started
    container_start(engine, server, container_obj)
    # Prepare the bookmark creation parameters
    bookmark_create_params = JSBookmarkCreateParameters()
    bookmark_create_params.bookmark = JSBookmark()
    bookmark_create_params.bookmark.name = bookmark_name
    bookmark_create_params.bookmark.branch = container_obj.active_branch
    bookmark_create_params.bookmark.shared = bookmark_shared
    bookmark_create_params.bookmark.tags = tags
    bookmark_create_params.timeline_point_parameters = JSTimelinePointLatestTimeInput()
    bookmark_create_params.timeline_point_parameters.source_data_layout = (
        container_obj.reference
    )

    jetstream.bookmark.create(server, bookmark_create_params)


def container_recover(engine, server, container_obj):
    """This function recovers a container that is in an "INCONSISTENT" state"""
    if container_obj.state == "INCONSISTENT":
        # if not recover it
        job_obj = jetstream.container.recover(server, container_obj.reference)
        # wait for the recovery action to finish
        job_context.wait(server, job_obj.reference)
        # get the updated object with the new state
        container_obj = jetstream.container.get(server, container_obj.reference)
        return container_obj


@run_async
def container_recover_async(engine, server, container_obj):
    """This function recovers all specified containers asynchronously"""
    container_recover(engine, server, container_obj)


@run_async
def container_refresh(engine, server, container_obj):
    """This function refreshes a container"""
    # But first, let's make sure it is in a CONSISTENT state
    container_recover(engine, server, container_obj)
    # Next let's make sure it is started
    container_start(engine, server, container_obj)
    # Now let's refresh it.
    refresh_job = jetstream.container.refresh(server, container_obj.reference)


@run_async
def container_reset(engine, server, container_obj):
    """This function resets a container"""
    # But first, let's make sure it is in a CONSISTENT state
    container_recover(engine, server, container_obj)
    # Next let's make sure it is started
    container_start(engine, server, container_obj)
    # Now let's refresh it.
    reset_job = jetstream.container.reset(server, container_obj.reference)


def container_start(engine, server, container_obj):
    """This function starts/enables a container that is in an "OFFLINE" state"""
    if container_obj.state == "OFFLINE":
        # if not, enable it
        jetstream.container.enable(server, container_obj.reference)


@run_async
def container_start_async(engine, server, container_obj):
    """This function starts all specified containers asynchronously"""
    container_start(engine, server, container_obj)


def container_stop(engine, server, container_obj):
    """This function starts/enables a container that is in an "OFFLINE" state"""
    if container_obj.state == "ONLINE":
        # if not, enable it
        jetstream.container.disable(server, container_obj.reference)


@run_async
def container_stop_async(engine, server, container_obj):
    """This function starts all specified containers asynchronously"""
    container_stop(engine, server, container_obj)


def find_container_by_name_and_template_name(
    engine, server, container_name, template_name
):
    template_obj = find_obj_by_name(engine, server, jetstream.template, template_name)

    containers = jetstream.container.get_all(server, template=template_obj.reference)

    for each in containers:
        if each.name == container_name:
            print_debug(engine["hostname"] + ": Found a match " + str(each.reference))
            return each
    print_info('Unable to find "' + container_name + '" in ' + template_name)


def find_all_containers_by_template_name(engine, server, template_name):
    template_obj = find_obj_by_name(engine, server, jetstream.template, template_name)

    containers = jetstream.container.get_all(server, template=template_obj.reference)
    if containers:
        for each in containers:
            print_debug(engine["hostname"] + ": Found a match " + str(each.reference))
        return containers
    print_info('Unable to find "' + container_name + '" in ' + template_name)


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


def job_wait(server):
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
    containers = []
    jobs = {}

    # Setup the connection to the Delphix Engine
    server = serversess(engine_address, engine_username, engine_password)

    # If we specified a specific database by name....
    if arguments["--container"]:
        # Get the container object from the name
        container_obj = find_container_by_name_and_template_name(
            engine, server, arguments["--container"], arguments["--template"]
        )
        if container_obj:
            containers.append(container_obj)
    # Else, if we said all containers ...
    elif arguments["--all_containers"]:
        # Grab all containers in the template
        containers = find_all_containers_by_template_name(
            engine, server, arguments["--template"]
        )
    if not containers or len(containers) == 0:
        print_error("No containers found with the criterion specified")
        return
    # reset the running job count before we begin
    i = 0
    container_threads = []
    # While there are still running jobs or containers still to process....
    while i > 0 or len(containers) > 0:
        # While there are containers still to process and we are still under
        # the max simultaneous jobs threshold (if specified)
        while len(containers) > 0 and (
            arguments["--parallel"] == None or i < int(arguments["--parallel"])
        ):
            # Give us the next database in the list, and remove it from the list
            container_obj = containers.pop()
            # what do we want to do?
            if arguments["--operation"] == "refresh":
                # refresh the container
                container_threads.append(
                    container_refresh(engine, server, container_obj)
                )
            elif arguments["--operation"] == "reset":
                container_threads.append(container_reset(engine, server, container_obj))
            elif arguments["--operation"] == "start":
                container_threads.append(
                    container_start_async(engine, server, container_obj)
                )
            elif arguments["--operation"] == "stop":
                container_threads.append(
                    container_stop_async(engine, server, container_obj)
                )
            elif arguments["--operation"] == "recover":
                container_threads.append(
                    container_recover_async(engine, server, container_obj)
                )
            elif arguments["--operation"] == "bookmark":
                if arguments["--bookmark_tags"]:
                    tags = arguments["--bookmark_tags"].split(",")
                else:
                    tags = []
                if arguments["--bookmark_shared"]:
                    if str(arguments["--bookmark_shared"]).lower() == "true":
                        bookmark_shared = True
                    elif str(arguments["--bookmark_shared"]).lower() == "false":
                        bookmark_shared = False
                    else:
                        print_error(
                            'Invalid argument "'
                            + str(arguments["--bookmark_shared"]).lower()
                            + '"  for --bookmark_shared'
                        )
                        print_error(
                            "--bookmark_shared only takes a value of true/false."
                        )
                        print_error("Exiting")
                        sys.exit(1)
                else:
                    bookmark_shared = False
                container_threads.append(
                    container_bookmark(
                        engine,
                        server,
                        container_obj,
                        arguments["--bookmark_name"],
                        bookmark_shared,
                        tags,
                    )
                )
            # For each thread in the list...
            i = len(container_threads)
        # Check to see if we are running at max parallel processes, and report if so.
        if arguments["--parallel"] != None and i >= int(arguments["--parallel"]):
            print_info(engine["hostname"] + ": Max jobs reached (" + str(i) + ")")
        # reset the running jobs counter, as we are about to update the count from the jobs report.
        i = 0
        for t in container_threads:
            if t.isAlive():
                i += 1
        print_info(
            engine["hostname"]
            + ": "
            + str(i)
            + " jobs running. "
            + str(len(containers))
            + " jobs waiting to run"
        )
        # If we have running jobs, pause before repeating the checks.
        if i > 0:
            sleep(float(arguments["--poll"]))
    print("made it out")
    # For each thread in the list...
    for each in container_threads:
        # join them back together so that we wait for all threads to complete before moving on
        each.join()


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
    global config_file_path
    global dxtools_objects

    try:
        # Declare globals that will be used throughout the script.
        logging_est(arguments["--logdir"])
        print_debug(arguments)
        time_start = time()
        engine = None
        single_thread = False

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

    # Feed our arguments to the main function, and off we go!
    print(arguments)
    main(arguments)
