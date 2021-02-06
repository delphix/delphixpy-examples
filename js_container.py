#!/usr/bin/env python
# Program Name : js_container.py
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
"""Create, delete, refresh and list JS containers.
Usage:
  js_container.py (--create_container <name> --template_name <name> --database <name> | --reset <name> | --list_hierarchy <name> | --list | --delete_container <name> [--keep_vdbs]| --refresh_container <name> | --add_owner <name> --container_name <name> | --remove_owner <name> --container_name <name> | --restore_container <name> --bookmark_name <name>)
                   [--engine <identifier> | --all] [--parallel <n>]
                   [--poll <n>] [--debug]
                   [--config <path_to_file>] [--logdir <path_to_file>]
  js_container.py -h | --help | -v | --version

Creates, Lists, Removes a Jet Stream Template

Examples:
  js_container.py --list
  js_container.py --list_hierarchy jscontainer1
  js_container.py --add_owner jsuser
  js_container.py --create_container jscontainer1 --database <name> --template_name jstemplate1
  js_container.py --delete_container jscontainer1
  js_container.py --refresh_container jscontainer1
  js_container.py --add_owner jsuser --container_name jscontainer1
  js_container.py --remove_owner jsuser --container_name jscontainer1
  js_container.py --refresh_container jscontainer1
  js_container.py --restore_container jscontainer1 --bookmark_name jsbookmark1
  js_conatiner.py --reset jscontainer1

Options:
  --create_container <name>  Name of the new JS Container
  --container_name <name>    Name of the JS Container
  --refresh_container <name> Name of the new JS Container
  --restore_container <name> Name of the JS Container to restore
  --reset <name>             Reset last data operation
  --template_name <name>     Name of the JS Template to use for the container
  --add_owner <name>         Name of the JS Owner for the container
  --remove_owner <name>      Name of the JS Owner to remove
  --bookmark_name <name>     Name of the JS Bookmark to restore the container
  --keep_vdbs                If set, deleting the container will not remove
                             the underlying VDB(s)
  --list_hierarchy <name>     Lists hierarchy of a given container name
  --delete_container <name>  Delete the JS Container
  --database <name>          Name of the child database(s) to use for the
                                JS Container
  --list_containers          List the containers on a given engine
  --engine <type>            Alt Identifier of Delphix engine in dxtools.conf.
  --all                      Run against all engines.
  --debug                    Enable debug logging
  --parallel <n>             Limit number of jobs to maxjob
  --poll <n>                 The number of seconds to wait between job polls
                             [default: 10]
  --config <path_to_file>    The path to the dxtools.conf file
                             [default: ./dxtools.conf]
  --logdir <path_to_file>    The path to the logfile you want to use.
                             [default: ./js_container.log]
  -h --help                  Show this screen.
  -v --version               Show version.
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
from delphixpy.v1_8_0.web import user
from delphixpy.v1_8_0.web.jetstream import bookmark
from delphixpy.v1_8_0.web.jetstream import container
from delphixpy.v1_8_0.web.jetstream import datasource
from delphixpy.v1_8_0.web.jetstream import template
from delphixpy.v1_8_0.web.vo import JSDataContainerCreateParameters
from delphixpy.v1_8_0.web.vo import JSDataContainerDeleteParameters
from delphixpy.v1_8_0.web.vo import JSDataContainerModifyOwnerParameters
from delphixpy.v1_8_0.web.vo import JSDataSourceCreateParameters
from delphixpy.v1_8_0.web.vo import JSTimelinePointBookmarkInput
from lib.DlpxException import DlpxException
from lib.DxLogging import logging_est
from lib.DxLogging import print_debug
from lib.DxLogging import print_exception
from lib.DxLogging import print_info
from lib.GetReferences import convert_timestamp
from lib.GetReferences import find_obj_by_name
from lib.GetReferences import find_obj_name
from lib.GetReferences import get_obj_reference
from lib.GetSession import GetSession

VERSION = "v.0.0.020"


def create_container(dlpx_obj, template_name, container_name, database_name):
    """
    Create the JS container

    dlpx_obj: Virtualization Engine session object
    container_name: Name of the container to create
    database_name: Name of the database(s) to use in the container
    """

    js_container_params = JSDataContainerCreateParameters()
    container_ds_lst = []
    engine_name = dlpx_obj.dlpx_engines.keys()[0]
    for db in database_name.split(":"):
        container_ds_lst.append(build_ds_params(dlpx_obj, database, db))

    try:
        js_template_obj = find_obj_by_name(
            dlpx_obj.server_session, template, template_name
        )
        js_container_params.template = js_template_obj.reference
        js_container_params.timeline_point_parameters = {
            "sourceDataLayout": js_template_obj.reference,
            "type": "JSTimelinePointLatestTimeInput",
        }
        js_container_params.data_sources = container_ds_lst
        js_container_params.name = container_name
        container.create(dlpx_obj.server_session, js_container_params)
        dlpx_obj.jobs[engine_name] = dlpx_obj.server_session.last_job
        print_info("JS Container {} was created successfully.".format(container_name))
    except (DlpxException, RequestError, HttpError) as e:
        print_exception(
            "Container {} was not created. The error "
            "was:\n{}\n".format(container_name, e)
        )


def remove_owner(dlpx_obj, owner_name, container_name):
    """
    Removes an owner from a container

    dlpx_obj: Virtualization Engine session object
    owner_name: Name of the owner to remove
    container_name: Name of the container
    """

    owner_params = JSDataContainerModifyOwnerParameters()
    try:
        user_ref = find_obj_by_name(dlpx_obj.server_session, user, owner_name).reference
        owner_params.owner = user_ref
        container_obj = find_obj_by_name(
            dlpx_obj.server_session, container, container_name
        )
        container.remove_owner(
            dlpx_obj.server_session, container_obj.reference, owner_params
        )
        print_info(
            "User {} was granted access to {}".format(owner_name, container_name)
        )
    except (DlpxException, RequestError, HttpError) as e:
        print_exception(
            "The user was not added to container {}. The "
            "error was:\n{}\n".format(container_name, e)
        )


def restore_container(dlpx_obj, container_name, bookmark_name):
    """
    Restores a container to a given JS bookmark

    dlpx_obj: Virtualization Engine session object
    container_name: Name of the container
    bookmark_name: Name of the bookmark to restore
    """
    bookmark_params = JSTimelinePointBookmarkInput()
    bookmark_params.bookmark = get_obj_reference(
        dlpx_obj.server_session, bookmark, bookmark_name
    ).pop()
    engine_name = dlpx_obj.dlpx_engines.keys()[0]
    try:
        container.restore(
            dlpx_obj.server_session,
            get_obj_reference(dlpx_obj.server_session, container, container_name).pop(),
            bookmark_params,
        )
        dlpx_obj.jobs[engine_name] = dlpx_obj.server_session.last_job
        print_info(
            "Container {} was restored successfully with "
            "bookmark {}".format(container_name, bookmark_name)
        )
    except (DlpxException, RequestError, HttpError) as e:
        print_exception(
            "The user was not added to container {}. The "
            "error was:\n{}\n".format(container_name, e)
        )


def add_owner(dlpx_obj, owner_name, container_name):
    """
    Adds an owner to a container

    dlpx_obj: Virtualization Engine session object
    owner_name: Grant authorizations for the given user on this container and
        parent template
    container_name: Name of the container
    """

    owner_params = JSDataContainerModifyOwnerParameters()
    try:
        owner_params.owner = get_obj_reference(
            dlpx_obj.server_session, user, owner_name
        ).pop()
        container.add_owner(
            dlpx_obj.server_session,
            get_obj_reference(dlpx_obj.server_session, container, container_name).pop(),
            owner_params,
        )
        print_info(
            "User {} was granted access to {}".format(owner_name, container_name)
        )
    except (DlpxException, RequestError, HttpError) as e:
        print_exception(
            "The user was not added to container {}. The error"
            " was:\n{}\n".format(container_name, e)
        )


def refresh_container(dlpx_obj, container_name):
    """
    Refreshes a container

    dlpx_obj: Virtualization Engine session object
    container_name: Name of the container to refresh
    """

    engine_name = dlpx_obj.dlpx_engines.keys()[0]
    try:
        container.refresh(
            dlpx_obj.server_session,
            get_obj_reference(dlpx_obj.server_session, container, container_name).pop(),
        )
        dlpx_obj.jobs[engine_name] = dlpx_obj.server_session.last_job
        print_info("The container {} was refreshed.".format(container_name))
    except (DlpxException, RequestError, HttpError) as e:
        print_exception(
            "\nContainer {} was not refreshed. The error "
            "was:\n{}\n".format(container_name, e)
        )


def delete_container(dlpx_obj, container_name, keep_vdbs=False):
    """
    Deletes a container

    dlpx_obj: Virtualization Engine session object
    container_name: Container to delete
    """

    try:
        if keep_vdbs:
            js_container_params = JSDataContainerDeleteParameters()
            js_container_params.delete_data_sources = False
            container.delete(
                dlpx_obj.server_session,
                get_obj_reference(
                    dlpx_obj.server_session, container, container_name
                ).pop(),
                js_container_params,
            )
        elif keep_vdbs is False:
            container.delete(
                dlpx_obj.server_session,
                get_obj_reference(
                    dlpx_obj.server_session, container, container_name
                ).pop(),
            )
    except (DlpxException, RequestError, HttpError) as e:
        print_exception(
            "\nContainer {} was not deleted. The error "
            "was:\n{}\n".format(container_name, e)
        )


def list_containers(dlpx_obj):
    """
    List all containers on a given engine

    dlpx_obj: Virtualization Engine session object
    """

    header = "Name, Active Branch, Owner, Reference, Template, Last Updated"
    js_containers = container.get_all(dlpx_obj.server_session)
    try:
        print(header)
        for js_container in js_containers:
            last_updated = convert_timestamp(
                dlpx_obj.server_session, js_container.last_updated[:-5]
            )
            print_info(
                "{}, {}, {}, {}, {}, {}".format(
                    js_container.name,
                    js_container.active_branch,
                    str(js_container.owner),
                    str(js_container.reference),
                    str(js_container.template),
                    last_updated,
                )
            )
    except (DlpxException, HttpError, RequestError) as e:
        print_exception(
            "\nERROR: JS Containers could not be listed. The "
            "error was:\n\n{}".format(e)
        )


def reset_container(dlpx_obj, container_name):
    """
    Undo the last refresh or restore operation
    :param dlpx_obj: Virtualization Engine session object
    :param container_name: Name of the container to reset
    """
    try:
        container.reset(
            dlpx_obj.server_session,
            find_obj_by_name(
                dlpx_obj.server_session, container, container_name
            ).reference,
        )
    except RequestError as e:
        print_exception(
            "\nERROR: JS Container was not reset. The " "error was:\n\n{}".format(e)
        )
    print("Container {} was reset.\n".format(container_name))


def list_hierarchy(dlpx_obj, container_name):
    """
    Filter container listing.

    dlpx_obj: Virtualization Engine session object
    container_name: Name of the container to list child VDBs.
    """

    database_dct = {}
    layout_ref = find_obj_by_name(
        dlpx_obj.server_session, container, container_name
    ).reference
    for ds in datasource.get_all(dlpx_obj.server_session, data_layout=layout_ref):
        db_name = find_obj_name(dlpx_obj.server_session, database, ds.container)
        if hasattr(ds.runtime, "jdbc_strings"):
            database_dct[db_name] = ds.runtime.jdbc_strings
        else:
            database_dct[db_name] = "None"
    try:
        print_info(
            "Container: {}\nRelated VDBs: {}\n".format(
                container_name, convert_dct_str(database_dct)
            )
        )
    except (AttributeError, DlpxException) as e:
        print_exception(e)


def convert_dct_str(obj_dct):
    """
    Convert dictionary into a string for printing

    obj_dct: Dictionary to convert into a string
    :return: string object
    """
    js_str = ""

    if isinstance(obj_dct, dict):
        for js_db, js_jdbc in obj_dct.iteritems():
            if isinstance(js_jdbc, list):
                js_str += "{}: {}\n".format(js_db, ", ".join(js_jdbc))
            elif isinstance(js_jdbc, str):
                js_str += "{}: {}\n".format(js_db, js_jdbc)
    else:
        raise DlpxException(
            "Passed a non-dictionary object to "
            "convert_dct_str(): {}".format(type(obj_dct))
        )
    return js_str


def build_ds_params(dlpx_obj, obj, db):
    """
    Builds the datasource parameters

    dlpx_obj: Virtualization Engine session object
    obj: object type to use when finding db
    db: Name of the database to use when building the parameters
    """
    ds_params = JSDataSourceCreateParameters()
    ds_params.source = {"type": "JSDataSource", "name": db}
    try:
        db_obj = find_obj_by_name(dlpx_obj.server_session, obj, db)
        ds_params.container = db_obj.reference
        return ds_params
    except RequestError as e:
        print_exception("\nCould not find {}\n{}".format(db, e.message))


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

    engine: Dictionary of engines
    dlpx_obj: Virtualization Engine session object
    """

    try:
        # Setup the connection to the Delphix Engine
        dlpx_obj.serversess(
            engine["ip_address"], engine["username"], engine["password"]
        )

    except DlpxException as e:
        print_exception(
            "\nERROR: Engine {} encountered an error while "
            "creating the session:\n{}\n".format(dlpx_obj.dlpx_engines["hostname"], e)
        )
        sys.exit(1)

    thingstodo = ["thingtodo"]
    try:
        with dlpx_obj.job_mode(single_thread):
            while len(dlpx_obj.jobs) > 0 or len(thingstodo) > 0:
                if len(thingstodo) > 0:
                    if arguments["--create_container"]:
                        create_container(
                            dlpx_obj,
                            arguments["--template_name"],
                            arguments["--create_container"],
                            arguments["--database"],
                        )
                    elif arguments["--delete_container"]:
                        delete_container(
                            dlpx_obj,
                            arguments["--delete_container"],
                            arguments["--keep_vdbs"],
                        )
                    elif arguments["--list"]:
                        list_containers(dlpx_obj)
                    elif arguments["--remove_owner"]:
                        remove_owner(
                            dlpx_obj,
                            arguments["--remove_owner"],
                            arguments["--container_name"],
                        )
                    elif arguments["--restore_container"]:
                        restore_container(
                            dlpx_obj,
                            arguments["--restore_container"],
                            arguments["--bookmark_name"],
                        )
                    elif arguments["--add_owner"]:
                        add_owner(
                            dlpx_obj,
                            arguments["--add_owner"],
                            arguments["--container_name"],
                        )
                    elif arguments["--refresh_container"]:
                        refresh_container(dlpx_obj, arguments["--refresh_container"])
                    elif arguments["--list_hierarchy"]:
                        list_hierarchy(dlpx_obj, arguments["--list_hierarchy"])
                    elif arguments["--reset"]:
                        reset_container(dlpx_obj, arguments["--reset"])
                    thingstodo.pop()
                # get all the jobs, then inspect them
                i = 0
                for j in dlpx_obj.jobs.keys():
                    job_obj = job.get(dlpx_obj.server_session, dlpx_obj.jobs[j])
                    print_debug(job_obj)
                    print_info(
                        "{}: JS Container operations: {}".format(
                            engine["hostname"], job_obj.job_state
                        )
                    )
                    if job_obj.job_state in ["CANCELED", "COMPLETED", "FAILED"]:
                        # If the job is in a non-running state, remove it
                        # from the
                        # running jobs list.
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
        print("\nError in js_container: {}:\n{}".format(engine["hostname"], e))
        sys.exit(1)


def run_job(dlpx_obj, config_file_path):
    """
    This function runs the main_workflow aynchronously against all the
    servers specified

    dlpx_obj: Virtualization Engine session object
    config_file_path: filename of the configuration file for virtualization
    engines
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
            print("Error encountered in run_job():\n{}".format(e))
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
            except (DlpxException, RequestError, KeyError) as e:
                print_exception(
                    "\nERROR: Delphix Engine {} cannot be "
                    "found in {}. Please check your value "
                    "and try again. Exiting.\n".format(
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
    """
    Main function - setup global variables and timer
    """
    # We want to be able to call on these variables anywhere in the script.
    global single_thread
    global time_start
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
        run_job(dx_session_obj, config_file_path)

        elapsed_minutes = time_elapsed()
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
            "\nERROR: Please check the ERROR message " "below:\n{}".format(e.message)
        )
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
