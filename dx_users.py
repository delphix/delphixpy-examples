#!/usr/bin/env python
# Adam Bowen - Aug 2017
# Description:
# This script will allow you to easily manage users in Delphix
# This script currently only supports Native authentication
#
# Requirements
# pip install docopt delphixpy.v1_8_0

# The below doc follows the POSIX compliant standards and allows us to use
# this doc to also define our arguments for the script.
"""Description
Usage:
  dx_users.py (--user_name <name> [(--add --password <password> --email <email_address> [--jsonly]) |--delete])
                  [--engine <identifier> | --all]
                  [--debug] [--parallel <n>] [--poll <n>]
                  [--config <path_to_file>] [--logdir <path_to_file>]
  dx_users.py --update --user_name <name> [ --password <password> ] [--email <email_address> ] [ --delete ] [--jsonly]
                  [--engine <identifier> | --all]
                  [--debug] [--parallel <n>] [--poll <n>]
                  [--config <path_to_file>] [--logdir <path_to_file>]      
  dx_users.py (--list)
                  [--engine <identifier> | --all]
                  [--debug] [--parallel <n>] [--poll <n>]
                  [--config <path_to_file>] [--logdir <path_to_file>]
  dx_users.py -h | --help | -v | --version
Description

Examples:
    dx_users.py --add --user_name dev --password delphix --email "test@something.com" --jsonly
    dx_users.py --debug --config delphixpy.v1_8_0-examples/dxtools_1.conf  --update --user_name dev --password not_delphix --email "test@somethingelse.com"
    dx_users.py --delete --user_name dev
    dx_users.py --list

Options:
  --user_name <name>        The name of the user
  --password <password>     The password of the user to be created/updated
  --email <email_address>   The email addres of the user to be created/updated
  --jsonly                  Designate the user as a Jet Stream Only User
  --add                     Add the identified user
  --update                  Update the identified user
  --delete                  Delete the identified user
  --engine <type>           Alt Identifier of Delphix engine in dxtools.conf.
  --all                     Run against all engines.
  --debug                   Enable debug logging
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./dxtools.conf]
  --logdir <path_to_file>    The path to the logfile you want to use.
                            [default: ./dx_skel.log]
  -h --help                 Show this screen.
  -v --version              Show version.
"""
from __future__ import print_function

import sys
from os.path import basename
from time import sleep
from time import time

from docopt import docopt

from delphixpy.v1_8_0.exceptions import HttpError
from delphixpy.v1_8_0.exceptions import JobError
from delphixpy.v1_8_0.exceptions import RequestError
from delphixpy.v1_8_0.web import authorization
from delphixpy.v1_8_0.web import job
from delphixpy.v1_8_0.web import role
from delphixpy.v1_8_0.web import user
from delphixpy.v1_8_0.web.vo import Authorization
from delphixpy.v1_8_0.web.vo import CredentialUpdateParameters
from delphixpy.v1_8_0.web.vo import PasswordCredential
from delphixpy.v1_8_0.web.vo import User
from lib.DlpxException import DlpxException
from lib.DxLogging import logging_est
from lib.DxLogging import print_debug
from lib.DxLogging import print_exception
from lib.DxLogging import print_info
from lib.GetReferences import find_all_objects
from lib.GetReferences import find_obj_by_name
from lib.GetSession import GetSession

VERSION = "v.0.0.004"


def add_user(user_name, user_password, user_email, jsonly=None):
    """
    This function adds the user
    """
    user_obj = User()
    user_obj.name = user_name
    user_obj.email_address = user_email
    user_obj.credential = PasswordCredential()
    user_obj.credential.password = user_password

    try:
        user.create(dx_session_obj.server_session, user_obj)
        print("Attempting to create {}".format(user_name))
    except (DlpxException, RequestError) as e:
        print_exception(
            "\nERROR: Creating the user {} "
            "encountered an error:\n{}".format(user_name, e)
        )
        sys.exit(1)

    js_only(user_name, jsonly)


def js_only(user_name, jsonly=None):
    """
    Switch the user to/from a jsonly user
    """
    user_obj = find_obj_by_name(dx_session_obj.server_session, user, user_name)
    role_obj = find_obj_by_name(dx_session_obj.server_session, role, "Jet Stream User")

    if jsonly:
        authorization_obj = Authorization()
        authorization_obj.role = role_obj.reference
        authorization_obj.target = user_obj.reference
        authorization_obj.user = user_obj.reference

        authorization.create(dx_session_obj.server_session, authorization_obj)
    else:

        auth_name = (
            "("
            + user_obj.reference
            + ", "
            + role_obj.reference
            + ", "
            + user_obj.reference
            + ")"
        )
        authorization.delete(
            dx_session_obj.server_session,
            find_obj_by_name(
                dx_session_obj.server_session, authorization, auth_name
            ).reference,
        )


def update_user(user_name, user_password=None, user_email=None, jsonly=None):
    """
    This function updates the user
    """

    if user_email:
        updated_user_obj = User()
        updated_user_obj.email_address = user_email

        try:
            user.update(
                dx_session_obj.server_session,
                find_obj_by_name(
                    dx_session_obj.server_session, user, user_name
                ).reference,
                updated_user_obj,
            )
            print("Attempting to update {}".format(user_name))
        except (DlpxException, RequestError) as e:
            print_exception(
                "\nERROR: Updating the user {} "
                "encountered an error:\n{}".format(user_name, e)
            )
            sys.exit(1)

    if user_password:
        new_password_obj = CredentialUpdateParameters()
        new_password_obj.new_credential = PasswordCredential()
        new_password_obj.new_credential.password = user_password

        try:
            user.update_credential(
                dx_session_obj.server_session,
                find_obj_by_name(
                    dx_session_obj.server_session, user, user_name
                ).reference,
                new_password_obj,
            )
            print("Attempting to update {} password".format(user_name))
        except (DlpxException, RequestError) as e:
            print_exception(
                "\nERROR: Updating the user {} password "
                "encountered an error:\n{}".format(user_name, e)
            )
            sys.exit(1)

    js_only(user_name, jsonly)


def delete_user(user_name):
    """
    This function adds the user
    """
    user_obj = find_obj_by_name(dx_session_obj.server_session, user, user_name)

    try:
        user.delete(dx_session_obj.server_session, user_obj.reference)
        print("Attempting to delete {}".format(user_name))
    except (DlpxException, RequestError) as e:
        print_exception(
            "\nERROR: Deleting the user {} "
            "encountered an error:\n{}".format(user_name, e)
        )
        sys.exit(1)


def list_users():
    """
    This function lists all users
    """
    user_list = find_all_objects(dx_session_obj.server_session, user)

    for user_obj in user_list:
        print("User: {}".format(user_obj.name))


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
    try:
        with dx_session_obj.job_mode(single_thread):
            while len(dx_session_obj.jobs) > 0 or len(thingstodo) > 0:
                if len(thingstodo) > 0:
                    if arguments["--add"]:
                        add_user(
                            arguments["--user_name"],
                            arguments["--password"],
                            arguments["--email"],
                            arguments["--jsonly"],
                        )
                    elif arguments["--update"]:
                        update_user(
                            arguments["--user_name"],
                            arguments["--password"],
                            arguments["--email"],
                            arguments["--jsonly"],
                        )
                    elif arguments["--delete"]:
                        delete_user(arguments["--user_name"])
                    elif arguments["--list"]:
                        list_users()
                    thingstodo.pop()
                # get all the jobs, then inspect them
                i = 0
                for j in dx_session_obj.jobs.keys():
                    job_obj = job.get(
                        dx_session_obj.server_session, dx_session_obj.jobs[j]
                    )
                    print_debug(job_obj)
                    print_info(
                        "{}: User: {}".format(engine["hostname"], job_obj.job_state)
                    )
                    if job_obj.job_state in ["CANCELED", "COMPLETED", "FAILED"]:
                        # If the job is in a non-running state, remove it
                        # from the
                        # running jobs list.
                        del dx_session_obj.jobs[j]
                    elif job_obj.job_state in "RUNNING":
                        # If the job is in a running state, increment the
                        # running job count.
                        i += 1
                    print_info("{}: {:d} jobs running.".format(engine["hostname"], i))
                    # If we have running jobs, pause before repeating the
                    # checks.
                    if len(dx_session_obj.jobs) > 0:
                        sleep(float(arguments["--poll"]))

    except (HttpError, RequestError, JobError, DlpxException) as e:
        print_exception("ERROR: Could not complete user " "operation: {}".format(e))


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

    elif arguments["--all"] is False:
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
        single_thread = False
        config_file_path = arguments["--config"]
        # Parse the dxtools.conf and put it into a dictionary
        dx_session_obj.get_config(config_file_path)

        # This is the function that will handle processing main_workflow for
        # all the servers.
        run_job()

        elapsed_minutes = time_elapsed()
        print_info(
            "script took {:.2f} minutes to get this far.".format(elapsed_minutes)
        )

    # Here we handle what we do when the unexpected happens
    except DlpxException as e:
        print_exception(
            "script encountered an error while processing the"
            "config file:\n{}".format(e)
        )

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
            "Please check the ERROR message:\n{}".format(e)
        )
        sys.exit(1)

    except JobError as e:
        """
        We use this exception handler when a job fails in Delphix so that
        we have actionable data
        """
        elapsed_minutes = time_elapsed()
        print_exception("A job failed in the Delphix Engine")
        print_info(
            "{} took {:.2f} minutes to get this far\n{}".format(
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
