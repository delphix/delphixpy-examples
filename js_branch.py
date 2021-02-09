#!/usr/bin/env python
# Program Name : js_branch.py
# Description  : Delphix implementation script
# Author       : Corey Brune
# Created: March 4 2016
#
# Copyright (c) 2016 by Delphix.
# All rights reserved.
# See http://docs.delphix.com/display/PS/Copyright+Statement for details
#
# Delphix Support statement available at
# See http://docs.delphix.com/display/PS/PS+Script+Support+Policy for details
#
# Warranty details provided in external file
# for customers who have purchased support.
#
"""Creates, updates, deletes, activates and lists branches
Usage:
  js_branch.py (--create_branch <name> --container_name <name> [--template_name <name> | --bookmark_name <name>]| --list_branches | --delete_branch <name> | --activate_branch <name> | --update_branch <name>)
                   [--engine <identifier> | --all] [--parallel <n>]
                   [--poll <n>] [--debug]
                   [--config <path_to_file>] [--logdir <path_to_file>]
  js_branch.py -h | --help | -v | --version

Creates, Lists, Removes a Jet Stream Branch

Examples:
  js_branch.py --list_branches
  js_branch.py --create_branch jsbranch1 --container_name jscontainer --template_name jstemplate1
  js_branch.py --activate_branch jsbranch1
  js_branch.py --delete_branch jsbranch1
  js_branch.py --update_branch jsbranch1

Options:
  --create_branch <name>    Name of the new JS Branch
  --bookmark_name <name     Name of the bookmark to create the branch
  --container_name <name>   Name of the container to use
  --update_branch <name>    Name of the branch to update
  --template_name <name>    Name of the template to use
  --activate_branch <name>  Name of the branch to activate
  --delete_branch <name>    Delete the JS Branch
  --list_branches           List the branchs on a given engine
  --engine <type>           Alt Identifier of Delphix engine in dxtools.conf.
  --all                     Run against all engines.
  --debug                   Enable debug logging
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./dxtools.conf]
  --logdir <path_to_file>   The path to the logfile you want to use.
                            [default: ./js_branch.log]
  -h --help                 Show this screen.
  -v --version              Show version.
"""
from __future__ import print_function

import re
import sys
import traceback
from os.path import basename
from time import sleep
from time import time

from docopt import docopt

from delphixpy.v1_8_0.exceptions import HttpError
from delphixpy.v1_8_0.exceptions import JobError
from delphixpy.v1_8_0.exceptions import RequestError
from delphixpy.v1_8_0.web import job
from delphixpy.v1_8_0.web.jetstream import bookmark
from delphixpy.v1_8_0.web.jetstream import branch
from delphixpy.v1_8_0.web.jetstream import container
from delphixpy.v1_8_0.web.jetstream import operation
from delphixpy.v1_8_0.web.jetstream import template
from delphixpy.v1_8_0.web.vo import JSBranch
from delphixpy.v1_8_0.web.vo import JSBranchCreateParameters
from delphixpy.v1_8_0.web.vo import JSTimelinePointBookmarkInput
from delphixpy.v1_8_0.web.vo import JSTimelinePointLatestTimeInput
from lib.DlpxException import DlpxException
from lib.DxLogging import logging_est
from lib.DxLogging import print_debug
from lib.DxLogging import print_exception
from lib.DxLogging import print_info
from lib.GetReferences import find_obj_by_name
from lib.GetReferences import find_obj_name
from lib.GetSession import GetSession

VERSION = "v.0.0.015"


def create_branch(
    dlpx_obj, branch_name, container_name, template_name=None, bookmark_name=None
):
    """
    Create the JS Branch

    :param dlpx_obj: Virtualization Engine session object
    :param branch_name: Name of the branch to create
    :param container_name: Name of the container to use
    :param template_name: Name of the template to use
    :param bookmark_name: Name of the bookmark to use
    """

    js_branch = JSBranchCreateParameters()
    js_branch.name = branch_name
    engine_name = dlpx_obj.dlpx_engines.keys()[0]
    data_container_obj = find_obj_by_name(
        dlpx_obj.server_session, container, container_name
    )
    js_branch.data_container = data_container_obj.reference

    if bookmark_name:
        js_branch.timeline_point_parameters = JSTimelinePointBookmarkInput()
        js_branch.timeline_point_parameters.bookmark = find_obj_by_name(
            dlpx_obj.server_session, bookmark, bookmark_name
        ).reference
    elif template_name:
        source_layout_ref = find_obj_by_name(
            dlpx_obj.server_session, template, template_name
        ).reference
        js_branch.timeline_point_parameters = JSTimelinePointLatestTimeInput()
        js_branch.timeline_point_parameters.source_data_layout = source_layout_ref

    try:
        branch.create(dlpx_obj.server_session, js_branch)
        dlpx_obj.jobs[engine_name] = dlpx_obj.server_session.last_job
    except (DlpxException, RequestError, HttpError) as e:
        print_exception("\nThe branch was not created. The error was:" "\n{}".format(e))
    print_info("JS Branch {} was created successfully.".format(branch_name))


def list_branches(dlpx_obj):
    """
    List all branches on a given engine

    :param dlpx_obj: Virtualization Engine session object
    """

    try:
        header = "\nBranch Name, Data Layout, Reference, End Time"
        js_data_layout = ""
        js_branches = branch.get_all(dlpx_obj.server_session)

        print(header)
        for js_branch in js_branches:
            js_end_time = operation.get(
                dlpx_obj.server_session, js_branch.first_operation
            ).end_time
            if re.search("TEMPLATE", js_branch.data_layout):
                js_data_layout = find_obj_name(
                    dlpx_obj.server_session, template, js_branch.data_layout
                )
            elif re.search("CONTAINER", js_branch.data_layout):
                js_data_layout = find_obj_name(
                    dlpx_obj.server_session, container, js_branch.data_layout
                )
            print_info(
                "{} {}, {}, {}".format(
                    js_branch._name[0], js_data_layout, js_branch.reference, js_end_time
                )
            )
    except (DlpxException, HttpError, RequestError) as e:
        print_exception(
            "\nERROR: JS Branches could not be listed. The "
            "error was:\n\n{}".format(e)
        )


def update_branch(dlpx_obj, branch_name):
    """
    Updates a branch

    :param dlpx_obj: Virtualization Engine session object
    :param branch_name: Name of the branch to update
    """

    js_branch_obj = JSBranch()
    try:
        branch_obj = find_obj_by_name(dlpx_obj.server_session, branch, branch_name)
        branch.update(dlpx_obj.server_session, branch_obj.reference, js_branch_obj)
        print_info("The branch {} was updated successfully.".format(branch_name))
    except (DlpxException, HttpError, RequestError) as e:
        print_exception(
            "\nERROR: The branch could not be updated. The "
            "error was:\n\n{}".format(e)
        )


def activate_branch(dlpx_obj, branch_name):
    """
    Activates a branch

    :param dlpx_obj: Virtualization Engine session object
    :param branch_name: Name of the branch to activate
    """

    engine_name = dlpx_obj.dlpx_engines.keys()[0]
    try:
        branch_obj = find_obj_by_name(dlpx_obj.server_session, branch, branch_name)
        branch.activate(dlpx_obj.server_session, branch_obj.reference)
        dlpx_obj.jobs[engine_name] = dlpx_obj.server_session.last_job
        print_info("The branch {} was activated successfully.".format(branch_name))
    except RequestError as e:
        print_exception("\nAn error occurred activating the " "branch:\n{}".format(e))


def delete_branch(dlpx_obj, branch_name):
    """
    Deletes a branch
    :param dlpx_obj: Virtualization Engine session object
    :param branch_name: Branch to delete
    """

    try:
        branch_obj = find_obj_by_name(dlpx_obj.server_session, branch, branch_name)
        branch.delete(dlpx_obj.server_session, branch_obj.reference)
    except (DlpxException, HttpError, RequestError) as e:
        print_exception(
            "\nERROR: The branch was not deleted. The "
            "error was:\n\n{}".format(e.message)
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


def time_elapsed():
    """
    This function calculates the time elapsed since the beginning of the script.
    Call this anywhere you want to note the progress in terms of time
    """
    return round((time() - time_start) / 60, +1)


@run_async
def main_workflow(engine, dlpx_obj):
    """
    This function is where we create our main workflow.
    Use the @run_async decorator to run this function asynchronously.
    The @run_async decorator allows us to run against multiple Delphix Engine
    simultaneously

    :param engine: Dictionary of engines
    :param dlpx_obj: Virtualization Engine session object
    """

    # Establish these variables as empty for use later
    environment_obj = None
    source_objs = None

    try:
        # Setup the connection to the Delphix Engine
        dlpx_obj.serversess(
            engine["ip_address"], engine["username"], engine["password"]
        )
    except DlpxException as e:
        print_exception(
            "\nERROR: Engine {} encountered an error while "
            "provisioning {}:\n{}\n".format(
                engine["hostname"], arguments["--target"], e
            )
        )
        sys.exit(1)

    thingstodo = ["thingtodo"]
    try:
        with dlpx_obj.job_mode(single_thread):
            while len(dlpx_obj.jobs) > 0 or len(thingstodo) > 0:
                if len(thingstodo) > 0:
                    if arguments["--create_branch"]:
                        create_branch(
                            dlpx_obj,
                            arguments["--create_branch"],
                            arguments["--container_name"],
                            arguments["--template_name"]
                            if arguments["--template_name"]
                            else None,
                            arguments["--bookmark_name"]
                            if arguments["--bookmark_name"]
                            else None,
                        )
                    elif arguments["--delete_branch"]:
                        delete_branch(dlpx_obj, arguments["--delete_branch"])
                    elif arguments["--update_branch"]:
                        update_branch(dlpx_obj, arguments["--update_branch"])
                    elif arguments["--activate_branch"]:
                        activate_branch(dlpx_obj, arguments["--activate_branch"])
                    elif arguments["--list_branches"]:
                        list_branches(dlpx_obj)
                    thingstodo.pop()
                # get all the jobs, then inspect them
                i = 0
                for j in dlpx_obj.jobs.keys():
                    job_obj = job.get(dlpx_obj.server_session, dlpx_obj.jobs[j])
                    print_debug(job_obj)
                    print_info(
                        "{}: Provisioning JS Branch: {}".format(
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
        print_exception("\nError in js_branch: {}\n{}".format(engine["hostname"], e))


def run_job(dlpx_obj, config_file_path):
    """
    This function runs the main_workflow aynchronously against all the
    servers specified

    dlpx_obj: Virtualization Engine session object
    config_file_path: path containing the dxtools.conf file.
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

            except (DlpxException, RequestError, KeyError) as e:
                print_exception(
                    "\nERROR: Delphix Engine {} cannot be found"
                    " in {}. Please check your value and try"
                    " again. Exiting.\n".format(arguments["--engine"], config_file_path)
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
    global time_start
    global debug

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
        run_job(dx_session_obj, config_file_path)
        elapsed_minutes = time_elapsed()
        print_info(
            "Script took {:.2f} minutes to get this far.".format(elapsed_minutes)
        )

    # Here we handle what we do when the unexpected happens
    except SystemExit as e:
        # This is what we use to handle our sys.exit(#)
        sys.exit(e)

    except DlpxException as e:
        # We use this exception handler when an error occurs in a function call.

        print("\nERROR: Please check the ERROR message below:\n{}".format(e.message))
        sys.exit(2)

    except HttpError as e:
        # We use this exception handler when our connection to Delphix fails

        print(
            "\nERROR: Connection failed to the Delphix Engine. Please "
            "check the ERROR message below:\n{}".format(e.message)
        )
        sys.exit(2)

    except JobError as e:
        # We use this exception handler when a job fails in Delphix so that we
        # have actionable data

        print("A job failed in the Delphix Engine:\n{}".format(e.job))
        elapsed_minutes = time_elapsed()
        print_info(
            "{} took {:.2f} minutes to get this far".format(
                basename(__file__), elapsed_minutes
            )
        )
        sys.exit(3)

    except KeyboardInterrupt:
        # We use this exception handler to gracefully handle ctrl+c exits

        print_debug("You sent a CTRL+C to interrupt the process")
        elapsed_minutes = time_elapsed()
        print_info(
            "{} took {:.2f} minutes to get this far".format(
                basename(__file__), elapsed_minutes
            )
        )

    except:
        # Everything else gets caught here

        print(sys.exc_info()[0])
        print(traceback.format_exc())
        elapsed_minutes = time_elapsed()
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
