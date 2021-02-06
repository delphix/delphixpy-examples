#!/usr/bin/env python
# Program Name : js_template.py
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
"""Creates, deletes and lists JS templates.
Usage:
  js_template.py (--create_template <name> --database <name> | --list_templates | --delete_template <name>)
                   [--engine <identifier> | --all] [--parallel <n>]
                   [--poll <n>] [--debug]
                   [--config <path_to_file>] [--logdir <path_to_file>]
  js_template.py -h | --help | -v | --version

Creates, Lists, Removes a Jet Stream Template

Examples:
  js_template.py --list_templates
  js_template.py --create_template jstemplate1 --database <name>
  js_template.py --create_template jstemplate2 --database <name:name:name>
  js_template.py --delete_template jstemplate1

Options:
  --create_template <name>  Name of the new JS Template
  --delete_template <name>  Delete the JS Template
  --database <name>         Name of the database(s) to use for the JS Template
                                Note: If adding multiple template DBs, use a
                                comma (:) to delineate between the DB names.
  --list_templates          List the templates on a given engine
  --engine <type>           Alt Identifier of Delphix engine in dxtools.conf.
  --all                     Run against all engines.
  --debug                   Enable debug logging
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./dxtools.conf]
  --logdir <path_to_file>   The path to the logfile you want to use.
                            [default: ./js_template.log]
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
from delphixpy.v1_8_0.web.jetstream import template
from delphixpy.v1_8_0.web.vo import JSDataSourceCreateParameters
from delphixpy.v1_8_0.web.vo import JSDataTemplateCreateParameters
from lib.DlpxException import DlpxException
from lib.DxLogging import logging_est
from lib.DxLogging import print_debug
from lib.DxLogging import print_exception
from lib.DxLogging import print_info
from lib.GetReferences import convert_timestamp
from lib.GetReferences import find_obj_by_name
from lib.GetSession import GetSession

VERSION = "v.0.0.015"


def create_template(dlpx_obj, template_name, database_name):
    """
    Create the JS Template

    dlpx_obj: Virtualization Engine session object
    template_name: Name of the template to create
    database_name: Name of the database(s) to use in the template
    """

    js_template_params = JSDataTemplateCreateParameters()
    js_template_params.name = template_name
    template_ds_lst = []
    engine_name = dlpx_obj.dlpx_engines.keys()[0]

    for db in database_name.split(":"):
        template_ds_lst.append(build_ds_params(dlpx_obj, database, db))
    try:
        js_template_params.data_sources = template_ds_lst
        js_template_params.type = "JSDataTemplateCreateParameters"
        template.create(dlpx_obj.server_session, js_template_params)
        dlpx_obj.jobs[engine_name] = dlpx_obj.server_session.last_job
        print_info("Template {} was created successfully.\n".format(template_name))
    except (DlpxException, RequestError, HttpError) as e:
        print_exception(
            "\nThe template {} was not created. The error "
            "was:\n\n{}".format(template_name, e)
        )


def list_templates(dlpx_obj):
    """
    List all templates on a given engine

    dlpx_obj: Virtualization Engine session object
    """

    header = "Name, Reference, Active Branch, Last Updated"

    try:
        print(header)
        js_templates = template.get_all(dlpx_obj.server_session)
        for js_template in js_templates:
            last_updated = convert_timestamp(
                dlpx_obj.server_session, js_template.last_updated[:-5]
            )
            print_info(
                "{}, {}, {}, {}".format(
                    js_template.name,
                    js_template.reference,
                    js_template.active_branch,
                    last_updated,
                )
            )
    except (DlpxException, HttpError, RequestError) as e:
        raise DlpxException(
            "\nERROR: The templates could not be listed. "
            "The error was:\n\n{}".format(e.message)
        )


def delete_template(dlpx_obj, template_name):
    """
    Deletes a template

    dlpx_obj: Virtualization Engine session object
    template_name: Template to delete
    """

    try:
        template_obj = find_obj_by_name(
            dlpx_obj.server_session, template, template_name
        )
        template.delete(dlpx_obj.server_session, template_obj.reference)
        print("Template {} is deleted.".format(template_name))
    except (DlpxException, HttpError, RequestError) as e:
        print_exception(
            "\nERROR: The template {} was not deleted. The"
            " error was:\n\n{}".format(template_name, e.message)
        )


def build_ds_params(dlpx_obj, obj, db):
    """
    Builds the datasource parameters

    dlpx_obj: Virtualization Engine session object
    obj: object type to use when finding db
    db: Name of the database to use when building the parameters
    """

    try:
        db_obj = find_obj_by_name(dlpx_obj.server_session, obj, db)
        ds_params = JSDataSourceCreateParameters()
        ds_params.source = {"type": "JSDataSource", "name": db}
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
            "provisioning {}:\n{}\n".format(
                dlpx_obj.engine["hostname"], arguments["--target"], e
            )
        )
        sys.exit(1)

    thingstodo = ["thingtodo"]
    try:
        with dlpx_obj.job_mode(single_thread):
            while len(dlpx_obj.jobs) > 0 or len(thingstodo) > 0:
                if len(thingstodo) > 0:
                    if arguments["--create_template"]:
                        create_template(
                            dlpx_obj,
                            arguments["--create_template"],
                            arguments["--database"],
                        )
                    elif arguments["--delete_template"]:
                        delete_template(dlpx_obj, arguments["--delete_template"])
                    elif arguments["--list_templates"]:
                        list_templates(dlpx_obj)
                    thingstodo.pop()
                # get all the jobs, then inspect them
                i = 0
                for j in dlpx_obj.jobs.keys():
                    job_obj = job.get(dlpx_obj.server_session, dlpx_obj.jobs[j])
                    print_debug(job_obj)
                    print_info(
                        "{}: Provisioning JS Template: {}".format(
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
        print_exception("\nError in js_template: {}:\n{}".format(engine["hostname"], e))
        sys.exit(1)


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

            except (DlpxException, RequestError, KeyError):
                raise DlpxException(
                    "\nERROR: Delphix Engine {} cannot be "
                    "found in %s. Please check your value "
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
    # We want to be able to call on these variables anywhere in the script.
    global single_thread
    global time_start
    global debug

    try:
        dx_session_obj = GetSession()
        logging_est(arguments["--logdir"])
        print_debug(arguments)
        time_start = time()
        config_file_path = arguments["--config"]

        logging_est(arguments["--logdir"])
        print_debug(arguments)
        single_thread = False
        # Parse the dxtools.conf and put it into a dictionary
        dx_session_obj.get_config(config_file_path)

        # This is the function that will handle processing main_workflow for
        # all the servers.
        run_job(dx_session_obj, config_file_path)

        elapsed_minutes = time_elapsed()
        print_info("script took {:.2f} to get this far.".format(elapsed_minutes))

    # Here we handle what we do when the unexpected happens
    except SystemExit as e:
        # This is what we use to handle our sys.exit(#)
        sys.exit(e)

    except DlpxException as e:
        # We use this exception handler when an error occurs in a function call.
        print_info(
            "\nERROR: Please check the ERROR message below:\n{}".format(e.message)
        )
        sys.exit(2)

    except HttpError as e:
        # We use this exception handler when our connection to Delphix fails
        print_info(
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
        print("{}\n{}".format(sys.exc_info()[0], traceback.format_exc()))
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
