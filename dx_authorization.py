#!/usr/bin/env python
# Corey Brune - Oct 2016
# Creates an authorization object
# requirements
# pip install docopt delphixpy

# The below doc follows the POSIX compliant standards and allows us to use
# this doc to also define our arguments for the script.
"""List, create or remove authorizations for a Virtualization Engine
Usage:
  dx_authorization.py (--create --role <name> --target_type <name> --target <name> --user <name> | --list | --delete --role <name> --target_type <name> --target <name> --user <name>)
                  [--engine <identifier> | --all]
                  [--debug] [--parallel <n>] [--poll <n>]
                  [--config <path_to_file>] [--logdir <path_to_file>]
  dx_authorization.py -h | --help | -v | --version
List, delete and create authentication objects

Examples:
  dx_authorization.py --engine landsharkengine --create --role Data --user dev_user --target_type database --target test_vdb
  dx_authorization.py --engine landsharkengine --create --role Data --user dev_user --target_type group --target Sources
  dx_authorization.py --list
  dx_authorization.py --delete --role Data --user dev_user --target_type database --target test_vdb

Options:
  --create                  Create an authorization
  --role <name>             Role for authorization. Valid Roles are Data,
                             Read, Jet Stream User, OWNER, PROVISIONER
  --target <name>           Target object for authorization
  --target_type <name>      Target type. Valid target types are snapshot,
                             group, database
  --user <name>             User for the authorization
  --list                    List all authorizations
  --delete                  Delete authorization
  --engine <type>           Alt Identifier of Delphix engine in dxtools.conf.
  --all                     Run against all engines.
  --debug                   Enable debug logging
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./dxtools.conf]
  --logdir <path_to_file>    The path to the logfile you want to use.
                            [default: ./dx_authorization.log]
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
from delphixpy.v1_8_0.web import authorization
from delphixpy.v1_8_0.web import database
from delphixpy.v1_8_0.web import group
from delphixpy.v1_8_0.web import job
from delphixpy.v1_8_0.web import role
from delphixpy.v1_8_0.web import snapshot
from delphixpy.v1_8_0.web import user
from delphixpy.v1_8_0.web.vo import Authorization
from delphixpy.v1_8_0.web.vo import User
from lib.DlpxException import DlpxException
from lib.DxLogging import logging_est
from lib.DxLogging import print_debug
from lib.DxLogging import print_exception
from lib.DxLogging import print_info
from lib.GetReferences import find_obj_by_name
from lib.GetSession import GetSession

VERSION = "v.0.0.015"


def create_authorization(dlpx_obj, role_name, target_type, target_name, user_name):
    """
    Function to start, stop, enable or disable a VDB

    :param dlpx_obj: Virtualization Engine session object
    :type dlpx_obj: lib.GetSession.GetSession
    :param role_name: Name of the role
    :param target_type: Supports snapshot, group and database target types
    :param target_name: Name of the target
    :param user_name: User for the authorization
    """

    authorization_obj = Authorization()
    print_debug(
        "Searching for {}, {} and {} references.\n".format(
            role_name, target_name, user_name
        )
    )
    try:
        authorization_obj.role = find_obj_by_name(
            dlpx_obj.server_session, role, role_name
        ).reference
        authorization_obj.target = find_target_type(
            dlpx_obj, target_type, target_name
        ).reference
        authorization_obj.user = find_obj_by_name(
            dlpx_obj.server_session, user, user_name
        ).reference
        authorization.create(dlpx_obj.server_session, authorization_obj)
    except (RequestError, HttpError, JobError) as e:
        print_exception(
            "An error occurred while creating authorization:\n" "{}".format(e)
        )
    print("Authorization successfully created for {}.".format(user_name))


def delete_authorization(dlpx_obj, role_name, target_type, target_name, user_name):
    """
    Function to delete a given authorization

    :param dlpx_obj: Virtualization Engine session object
    :type dlpx_obj: lib.GetSession.GetSession
    :param role_name: Name of the role
    :type role_name: basestring
    :param target_type: Supports snapshot, group and database target types
    :type target_type basestring
    :param target_name: Name of the target
    :type target_name: basestring
    :param user_name: User for the authorization
    :type user_name: basestring
    """
    target_obj = find_target_type(dlpx_obj, target_type, target_name)
    user_obj = find_obj_by_name(dlpx_obj.server_session, user, user_name)
    role_obj = find_obj_by_name(dlpx_obj.server_session, role, role_name)
    auth_objs = authorization.get_all(dlpx_obj.server_session)

    try:

        del_auth_str = "({}, {}, {})".format(
            user_obj.reference, role_obj.reference, target_obj.reference
        )
        for auth_obj in auth_objs:
            if auth_obj.name == del_auth_str:
                authorization.delete(dlpx_obj.server_session, auth_obj.reference)
    except DlpxException as e:
        print_exception("ERROR: Could not delete authorization:\n{}".format(e))
    print("{} for user {} was deleted successfully".format(target_name, user_name))


def find_target_type(dlpx_obj, target_type, target_name):
    """
    Function to find the target authorization

    :param dlpx_obj: Virtualization Engine session object
    :type dlpx_obj: lib.GetSession.GetSession
    :param target_type: Type of target for authorization
    :param target_name: Name of the target
    """

    target_obj = None
    try:
        if target_type.lower() == "group":
            target_obj = find_obj_by_name(dlpx_obj.server_session, group, target_name)
        elif target_type.lower() == "database":
            target_obj = find_obj_by_name(
                dlpx_obj.server_session, database, target_name
            )
        elif target_type.lower() == "snapshot":
            target_obj = find_obj_by_name(
                dlpx_obj.server_session, snapshot, target_name
            )
    except (DlpxException, RequestError, HttpError) as e:
        print_exception(
            "Could not find authorization target type " "{}:\n{}".format(target_type, e)
        )
    return target_obj


def list_authorization(dlpx_obj):
    """
    Function to list authorizations for a given engine

    :param dlpx_obj: Virtualization Engine session object
    """
    target_obj = None

    try:
        auth_objs = authorization.get_all(dlpx_obj.server_session)
        print_info("User, Role, Target, Reference")
        for auth_obj in auth_objs:
            role_obj = role.get(dlpx_obj.server_session, auth_obj.role)
            user_obj = user.get(dlpx_obj.server_session, auth_obj.user)
            if auth_obj.target.startswith("USER"):
                target_obj = user.get(dlpx_obj.server_session, auth_obj.target)
            elif auth_obj.target.startswith("GROUP"):
                target_obj = group.get(dlpx_obj.server_session, auth_obj.target)
            elif auth_obj.target.startswith("DOMAIN"):
                target_obj = User()
                target_obj.name = "DOMAIN"
            print(
                "{}, {}, {}, {}".format(
                    user_obj.name, role_obj.name, target_obj.name, auth_obj.reference
                )
            )
    except (RequestError, HttpError, JobError, AttributeError) as e:
        print_exception(
            "An error occurred while listing authorizations.:\n" "{}\n".format((e))
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
    This function actually runs the jobs.
    Use the @run_async decorator to run this function asynchronously.
    This allows us to run against multiple Delphix Engine simultaneously

    engine: Dictionary of engines
    :type engine: dict
    dlpx_obj: Virtualization Engine session object
    :type dlpx_obj: lib.GetSession.GetSession

    """

    try:
        # Setup the connection to the Delphix Engine
        dlpx_obj.serversess(
            engine["ip_address"], engine["username"], engine["password"]
        )
    except DlpxException as e:
        print_exception(
            "ERROR: js_bookmark encountered an error authenticating"
            " to {} {}:\n{}\n".format(engine["hostname"], arguments["--target"], e)
        )
    thingstodo = ["thingtodo"]
    try:
        with dlpx_obj.job_mode(single_thread):
            while len(dlpx_obj.jobs) > 0 or len(thingstodo) > 0:
                if len(thingstodo) > 0:
                    if arguments["--create"]:
                        create_authorization(
                            dlpx_obj,
                            arguments["--role"],
                            arguments["--target_type"],
                            arguments["--target"],
                            arguments["--user"],
                        )
                    elif arguments["--delete"]:
                        delete_authorization(
                            dlpx_obj,
                            arguments["--role"],
                            arguments["--target_type"],
                            arguments["--target"],
                            arguments["--user"],
                        )
                    elif arguments["--list"]:
                        list_authorization(dlpx_obj)
                    thingstodo.pop()
                # get all the jobs, then inspect them
                i = 0
                for j in dlpx_obj.jobs.keys():
                    job_obj = job.get(dlpx_obj.server_session, dlpx_obj.jobs[j])
                    print_debug(job_obj)
                    print_info("{}: : {}".format(engine["hostname"], job_obj.job_state))
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
        print_exception(
            "\nError in dx_authorization: {}\n{}".format(engine["hostname"], e)
        )
        sys.exit(1)


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


def time_elapsed(time_start):
    """
    This function calculates the time elapsed since the beginning of the script.
    Call this anywhere you want to note the progress in terms of time

    :param time_start: float containing start time of the script.
    """
    return round((time() - time_start) / 60, +1)


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
