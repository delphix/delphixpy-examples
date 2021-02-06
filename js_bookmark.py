#!/usr/bin/env python
# Program Name : js_bookmark.py
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
"""Creates, lists, removes a Jet Stream Bookmark
Usage:
  js_bookmark.py (--create_bookmark <name> --data_layout <name> [--tags <tags> --description <name> --branch_name <name>]| --list_bookmarks [--tags <tags>] | --delete_bookmark <name> | --activate_bookmark <name> | --update_bookmark <name> | --share_bookmark <name> | --unshare_bookmark <name>)
                   [--engine <identifier> | --all] [--parallel <n>]
                   [--poll <n>] [--debug]
                   [--config <path_to_file>] [--logdir <path_to_file>]
  js_bookmark.py -h | --help | -v | --version

Creates, Lists, Removes a Jet Stream Bookmark

Examples:
  js_bookmark.py --list_bookmarks
  js_bookmark.py --list_bookmarks --tags "Jun 17, 25pct"
  js_bookmark.py --create_bookmark jsbookmark1 --data_layout jstemplate1
  js_bookmark.py --create_bookmark jsbookmark1 --data_layout jstemplate1 --tags "1.86.2,bobby" --description "Before commit"
  js_bookmark.py --create_bookmark jsbookmark1 --data_layout jstemplate1 --branch_name jsbranch1
  js_bookmark.py --activate_bookmark jsbookmark1
  js_bookmark.py --update_bookmark jsbookmark1
  js_bookmark.py --delete_bookmark jsbookmark1
  js_bookmark.py --share_bookmark jsbookmark1
  js_bookmark.py --unshare_bookmark jsbookmark1

Options:
  --create_bookmark <name>    Name of the new JS Bookmark
  --container_name <name>     Name of the container to use
  --tags <tags>               Tags to use for this bookmark (comma-delimited)
  --description <name>        Description of this bookmark
  --update_bookmark <name>    Name of the bookmark to update
  --share_bookmark <name>     Name of the bookmark to share
  --unshare_bookmark <name>   Name of the bookmark to unshare
  --branch_name <name>        Optional: Name of the branch to use
  --data_layout <name>        Name of the data layout (container or template) to use
  --activate_bookmark <name>  Name of the bookmark to activate
  --delete_bookmark <name>    Delete the JS Bookmark
  --list_bookmarks            List the bookmarks on a given engine
  --engine <type>             Alt Identifier of Delphix engine in dxtools.conf.
  --all                       Run against all engines.
  --debug                     Enable debug logging
  --parallel <n>              Limit number of jobs to maxjob
  --poll <n>                  The number of seconds to wait between job polls
                              [default: 10]
  --config <path_to_file>     The path to the dxtools.conf file
                              [default: ./dxtools.conf]
  --logdir <path_to_file>     The path to the logfile you want to use.
                              [default: ./js_bookmark.log]
  -h --help                   Show this screen.
  -v --version                Show version.
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
from delphixpy.v1_8_0.web import job
from delphixpy.v1_8_0.web.jetstream import bookmark
from delphixpy.v1_8_0.web.jetstream import branch
from delphixpy.v1_8_0.web.jetstream import container
from delphixpy.v1_8_0.web.jetstream import template
from delphixpy.v1_8_0.web.vo import JSBookmark
from delphixpy.v1_8_0.web.vo import JSBookmarkCreateParameters
from lib.DlpxException import DlpxException
from lib.DxLogging import logging_est
from lib.DxLogging import print_debug
from lib.DxLogging import print_exception
from lib.DxLogging import print_info
from lib.GetReferences import find_obj_by_name
from lib.GetReferences import find_obj_name
from lib.GetReferences import get_obj_reference
from lib.GetSession import GetSession

VERSION = "v.0.0.019"


def create_bookmark(
    dlpx_obj,
    bookmark_name,
    source_layout,
    branch_name=None,
    tags=None,
    description=None,
):
    """
    Create the JS Bookmark

    :param dlpx_obj: Virtualization Engine session object
    :type dlpx_obj: lib.GetSession.GetSession
    :param bookmark_name: Name of the bookmark to create
    :type bookmark_name: basestring
    :param source_layout: Name of the source (template or container) to use
    :type source_layout: basestring
    :param branch_name: Name of the branch to use
    :type branch_name: basestring
    :param tag_name: Tag to use for the bookmark
    :type tag: basestring
    :param description: Description of the bookmark
    :type description: basestring
    """

    branch_ref = None
    source_layout_ref = None
    engine_name = dlpx_obj.dlpx_engines.keys()[0]
    js_bookmark_params = JSBookmarkCreateParameters()
    if branch_name:
        try:
            source_layout_ref = find_obj_by_name(
                dlpx_obj.server_session, template, source_layout
            ).reference
        except DlpxException:
            source_layout_ref = find_obj_by_name(
                dlpx_obj.server_session, container, source_layout
            ).reference
        # import pdb;pdb.set_trace()
        for branch_obj in branch.get_all(dlpx_obj.server_session):
            if (
                branch_name == branch_obj.name
                and source_layout_ref == branch_obj.data_layout
            ):
                branch_ref = branch_obj.reference
                break
        if branch_ref is None:
            raise DlpxException(
                "Set the --data_layout parameter equal to "
                "the data layout of the bookmark.\n"
            )
    elif branch_name is None:
        try:
            (source_layout_ref, branch_ref) = find_obj_by_name(
                dlpx_obj.server_session, template, source_layout, True
            )
        except DlpxException:
            (source_layout_ref, branch_ref) = find_obj_by_name(
                dlpx_obj.server_session, container, source_layout, True
            )
        if branch_ref is None:
            raise DlpxException(
                "Could not find {} in engine {}".format(branch_name, engine_name)
            )
    js_bookmark_params.bookmark = JSBookmark()
    js_bookmark_params.bookmark.name = bookmark_name
    js_bookmark_params.bookmark.branch = branch_ref
    if tags:
        js_bookmark_params.bookmark.tags = tags.split(",")
    if description:
        js_bookmark_params.bookmark.description = description
    js_bookmark_params.timeline_point_parameters = {
        "sourceDataLayout": source_layout_ref,
        "type": "JSTimelinePointLatestTimeInput",
    }
    try:
        bookmark.create(dlpx_obj.server_session, js_bookmark_params)
        dlpx_obj.jobs[engine_name] = dlpx_obj.server_session.last_job
        print_info("JS Bookmark {} was created successfully.".format(bookmark_name))

    except (DlpxException, RequestError, HttpError) as e:
        print_exception(
            "\nThe bookmark {} was not created. The error "
            "was:\n\n{}".format(bookmark_name, e)
        )


def list_bookmarks(dlpx_obj, tags=None):
    """
    List all bookmarks on a given engine

    :param dlpx_obj: Virtualization Engine session object
    :param tag_filter: Only list bookmarks with given tag

    """

    header = "\nName, Reference, Branch Name, Template Name, Tags"
    try:
        js_bookmarks = bookmark.get_all(dlpx_obj.server_session)
        print(header)
        for js_bookmark in js_bookmarks:
            branch_name = find_obj_name(
                dlpx_obj.server_session, branch, js_bookmark.branch
            )
            tag_filter = [x.strip() for x in tags.decode("utf-8", "ignore").split(",")]
            if all(tag in js_bookmark.tags for tag in tag_filter):
                print(
                    "{}, {}, {}, {}, {}".format(
                        js_bookmark.name,
                        js_bookmark.reference,
                        branch_name,
                        js_bookmark.template_name,
                        ", ".join(tag for tag in js_bookmark.tags),
                    )
                )
            elif tag_filter is None:
                tag = js_bookmark.tags if js_bookmark.tags else None
                if tag:
                    tag = ", ".join(tag for tag in js_bookmark.tags)
                print(
                    "{}, {}, {}, {}, {}".format(
                        js_bookmark.name,
                        js_bookmark.reference,
                        branch_name,
                        js_bookmark.template_name,
                        tag,
                    )
                )
        print("\n")

    except (DlpxException, HttpError, RequestError) as e:
        print_exception(
            "\nERROR: The bookmarks on could not be listed. The "
            "error was:\n\n{}".format(e)
        )


def unshare_bookmark(dlpx_obj, bookmark_name):
    """
    Unshare a bookmark

    :param dlpx_obj: Virtualization Engine session object
    :param bookmark_name: Name of the bookmark to share
    """

    try:
        bookmark.unshare(
            dlpx_obj.server_session,
            get_obj_reference(dlpx_obj.server_session, bookmark, bookmark_name).pop(),
        )
        print_info("JS Bookmark {} was unshared successfully.".format(bookmark_name))
    except (DlpxException, HttpError, RequestError) as e:
        print_exception(
            "\nERROR: The bookmark {} could not be unshared. "
            "The error was:\n\n{}".format(bookmark_name, e)
        )


def share_bookmark(dlpx_obj, bookmark_name):
    """
    Share a bookmark

    :param dlpx_obj: Virtualization Engine session object
    :param bookmark_name: Name of the bookmark to share
    """

    try:
        bookmark.share(
            dlpx_obj.server_session,
            get_obj_reference(dlpx_obj.server_session, bookmark, bookmark_name).pop(),
        )
        print_info("JS Bookmark {} was shared successfully.".format(bookmark_name))
    except (DlpxException, HttpError, RequestError) as e:
        print_exception(
            "\nERROR: The bookmark {} could not be shared. The "
            "error was:\n\n{}".format(bookmark_name, e)
        )


def update_bookmark(dlpx_obj, bookmark_name):
    """
    Updates a bookmark

    :param dlpx_obj: Virtualization Engine session object
    :param bookmark_name: Name of the bookmark to update
    """

    js_bookmark_obj = JSBookmark()

    try:
        bookmark.update(
            dlpx_obj.server_session,
            get_obj_reference(dlpx_obj.server_session, bookmark, bookmark_name).pop(),
            js_bookmark_obj,
        )

    except (DlpxException, HttpError, RequestError) as e:
        print_exception(
            "ERROR: The bookmark {} could not be updated. The "
            "error was:\n{}".format(bookmark_name, e)
        )


def delete_bookmark(dlpx_obj, bookmark_name):
    """
    Deletes a bookmark

    :param dlpx_obj: Virtualization Engine session object
    :type dlpx_obj: lib.GetSession.GetSession
    :param bookmark_name: Bookmark to delete
    :type bookmark_name: str
    """

    try:
        bookmark.delete(
            dlpx_obj.server_session,
            get_obj_reference(dlpx_obj.server_session, bookmark, bookmark_name).pop(),
        )
        print_info("The bookmark {} was deleted successfully.".format(bookmark_name))
    except (DlpxException, HttpError, RequestError) as e:
        print_exception(
            "\nERROR: The bookmark {} was not deleted. The "
            "error was:\n\n{}".format(bookmark_name, e.message)
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


def time_elapsed(time_start):
    """
    This function calculates the time elapsed since the beginning of the script.
    Call this anywhere you want to note the progress in terms of time

    :param time_start: start time of the script.
    :type time_start: float
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
            "ERROR: js_bookmark encountered an error authenticating"
            " to {} {}:\n{}\n".format(engine["hostname"], arguments["--target"], e)
        )
        sys.exit(1)

    thingstodo = ["thingtodo"]
    try:
        with dlpx_obj.job_mode(single_thread):
            while len(dlpx_obj.jobs) > 0 or len(thingstodo) > 0:
                if len(thingstodo) > 0:
                    if arguments["--create_bookmark"]:
                        create_bookmark(
                            dlpx_obj,
                            arguments["--create_bookmark"],
                            arguments["--data_layout"],
                            arguments["--branch_name"]
                            if arguments["--branch_name"]
                            else None,
                            arguments["--tags"] if arguments["--tags"] else None,
                            arguments["--description"]
                            if arguments["--description"]
                            else None,
                        )
                    elif arguments["--delete_bookmark"]:
                        delete_bookmark(dlpx_obj, arguments["--delete_bookmark"])
                    elif arguments["--update_bookmark"]:
                        update_bookmark(dlpx_obj, arguments["--update_bookmark"])
                    elif arguments["--share_bookmark"]:
                        share_bookmark(dlpx_obj, arguments["--share_bookmark"])
                    elif arguments["--unshare_bookmark"]:
                        unshare_bookmark(dlpx_obj, arguments["--unshare_bookmark"])
                    elif arguments["--list_bookmarks"]:
                        list_bookmarks(
                            dlpx_obj,
                            arguments["--tags"] if arguments["--tags"] else None,
                        )
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
