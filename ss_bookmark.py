#!/usr/bin/env python3
# Create and manage Self-Service Bookmarks
# Copyright (c) 2019 by Delphix.
# All rights reserved.
# See http://docs.delphix.com/display/PS/Copyright+Statement for details
#
# Delphix Support statement available at
# See http://docs.delphix.com/display/PS/PS+Script+Support+Policy for details
#
# Warranty details provided in external file
# for customers who have purchased support.
#
"""Creates, lists, removes a Self Service Bookmark
Usage:
  ss_bookmark.py (--create_bookmark <name> --data_layout <name>
  [--tags <tags> --description <name> --branch_name <name>]
  | --list [--tags <tags>]
  | --delete_bookmark <name> | --activate_bookmark <name> |
   --update_bookmark <name> | --share_bookmark <name> |
   --unshare_bookmark <name>)
                   [--engine <identifier> | --all] [--single_thread <bool>]
                   [--poll <n>] [--debug]
                   [--config <path_to_file>] [--logdir <path_to_file>]
  ss_bookmark.py -h | --help | -v | --version

Creates, Lists, Removes a Self Service Bookmark

Examples:
  ss_bookmark.py --list
  ss_bookmark.py --list --tags "Jun 17, 25pct"
  ss_bookmark.py --create_bookmark ssbookmark1 --data_layout jstemplate1
  ss_bookmark.py --create_bookmark ssbookmark1 --data_layout jstemplate1 \
  --tags "1.86.2,bobby" --description "Before commit"
  ss_bookmark.py --create_bookmark ssbookmark1 --data_layout jstemplate1 \
  --branch_name jsbranch1
  ss_bookmark.py --activate_bookmark ssbookmark1
  ss_bookmark.py --update_bookmark ssbookmark1
  ss_bookmark.py --delete_bookmark ssbookmark1
  ss_bookmark.py --share_bookmark ssbookmark1
  ss_bookmark.py --unshare_bookmark ssbookmark1

Options:
  --create_bookmark <name>    Name of the new SS Bookmark
  --container_name <name>     Name of the container to use
  --tags <tags>               Tags to use for this bookmark (comma-delimited)
  --description <name>        Description of this bookmark
  --update_bookmark <name>    Name of the bookmark to update
  --branch_name <name>        Optional: Name of the branch to use
  --data_layout <name>        Name of the data layout (container or template)
  --activate_bookmark <name>  Name of the bookmark to activate
  --delete_bookmark <name>    Delete the SS Bookmark
  --list                      List the bookmarks on a given DDP
  --engine <name>             Alt Identifier of Delphix DDP in dxtools.conf.
                              [default: default]
  --all                       Run against all engines.
  --debug                     Enable debug logging
  --single_thread             Run as a single thread. False if running multiple
                              threads.
                              [default: True]
  --poll <n>                  The number of seconds to wait between job polls
                              [default: 10]
  --config <path_to_file>     The path to the dxtools.conf file
                              [default: ./dxtools.conf]
  --logdir <path_to_file>     The path to the logfile you want to use.
                              [default: ./ss_bookmark.log]
  -h --help                   Show this screen.
  -v --version                Show version.
"""
import sys
import time
from os.path import basename

import docopt

from delphixpy.v1_10_2 import exceptions
from delphixpy.v1_10_2.web import selfservice
from delphixpy.v1_10_2.web import vo
from lib import dlpx_exceptions
from lib import dx_logging
from lib import get_references
from lib import get_session
from lib import run_job
from lib.run_async import run_async

VERSION = "v.0.3.003"


def create_bookmark(
    dlpx_obj,
    bookmark_name,
    source_layout,
    branch_name=None,
    tags=None,
    description=None,
):
    """
    Create the Self Service Bookmark
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param bookmark_name: Name of the bookmark to create
    :type bookmark_name: str
    :param source_layout: Name of the source (template or container) to use
    :type source_layout: str
    :param branch_name: Name of the branch to use
    :type branch_name: str
    :param tags: Tag names to create the bookmark. Use commas to break up
    different tags
    :type tags: str
    :param description: Description of the bookmark
    :type description: str
    """
    bookmark_ref = None
    engine_name = list(dlpx_obj.dlpx_ddps)[0]
    ss_bookmark_params = vo.JSBookmarkCreateParameters()
    ss_bookmark_params.bookmark = vo.JSBookmark()
    ss_bookmark_params.bookmark.name = bookmark_name
    if branch_name:
        try:
            data_layout_obj = get_references.find_obj_by_name(
                dlpx_obj.server_session, selfservice.template, source_layout
            )
        except dlpx_exceptions.DlpxObjectNotFound:
            data_layout_obj = get_references.find_obj_by_name(
                dlpx_obj.server_session, selfservice.container, source_layout
            )
        for branch_obj in selfservice.branch.get_all(dlpx_obj.server_session):
            if (
                branch_name == branch_obj.name
                and data_layout_obj.reference == branch_obj.data_layout
            ):
                ss_bookmark_params.bookmark.branch = branch_obj.reference
                break
        if ss_bookmark_params.bookmark.branch is None:
            raise dlpx_exceptions.DlpxException(
                f"{branch_name} was not found. Set the --data_layout "
                f"parameter to the Self Service Template for the bookmark.\n"
            )
    elif branch_name is None:
        try:
            data_layout_obj = get_references.find_obj_by_name(
                dlpx_obj.server_session, selfservice.template, source_layout
            )
        except (dlpx_exceptions.DlpxException, exceptions.RequestError):
            raise dlpx_exceptions.DlpxException(
                f"Could not find a default branch in engine {engine_name}"
            )
        ss_bookmark_params.bookmark.branch = data_layout_obj.active_branch
    if tags:
        ss_bookmark_params.bookmark.tags = tags.split(",")
    if description:
        ss_bookmark_params.bookmark.description = description
    ss_bookmark_params.timeline_point_parameters = vo.JSTimelinePointLatestTimeInput()
    ss_bookmark_params.timeline_point_parameters.source_data_layout = (
        data_layout_obj.reference
    )
    try:
        bookmark_ref = selfservice.bookmark.create(
            dlpx_obj.server_session, ss_bookmark_params
        )
        dlpx_obj.jobs[engine_name] = dlpx_obj.server_session.last_job
    except (
        dlpx_exceptions.DlpxException,
        exceptions.RequestError,
        exceptions.HttpError,
    ) as err:
        dx_logging.print_exception(
            f"\nThe bookmark {bookmark_name} was not " f"created. The error was:\n{err}"
        )
    dx_logging.print_info(f"SS Bookmark {bookmark_name} was created " f"successfully.")
    return bookmark_ref


def list_bookmarks(dlpx_obj, tags=None):
    """
    List all bookmarks on a given engine
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param tags: Only list bookmarks with given tag
    :type tags: str
    """
    header = "\nName, Reference, Branch Name, Template Name, Tags"
    tag_filter = None
    try:
        ss_bookmarks = selfservice.bookmark.get_all(dlpx_obj.server_session)
        print(header)
        for ss_bookmark in ss_bookmarks:
            branch_name = get_references.find_obj_name(
                dlpx_obj.server_session, selfservice.branch, ss_bookmark.branch
            )
            if tags:
                tag_filter = [x.strip() for x in tags.split(",")]
            if tag_filter is None:
                tag = ss_bookmark.tags if ss_bookmark.tags else None
                if tag:
                    tag = ", ".join(tag for tag in ss_bookmark.tags)
                print(
                    f"{ss_bookmark.name}, {ss_bookmark.reference},"
                    f"{branch_name}, {ss_bookmark.template_name}, {tag}"
                )
            elif all(tag in ss_bookmark.tags for tag in tag_filter):
                print(
                    f"{ss_bookmark.name}, {ss_bookmark.reference},"
                    f"{branch_name}, {ss_bookmark.template_name}",
                    f'{", ".join(tag for tag in ss_bookmark.tags)}',
                )
        print("\n")
    except (
        dlpx_exceptions.DlpxException,
        exceptions.HttpError,
        exceptions.RequestError,
    ) as err:
        dx_logging.print_exception(
            f"\nERROR: The bookmarks on could not be "
            f"listed. The error was:\n\n{err}"
        )


def unshare_bookmark(dlpx_obj, bookmark_name):
    """
    Unshare a bookmark
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param bookmark_name: Name of the bookmark to share
    :type bookmark_name: str
    """
    try:
        selfservice.bookmark.unshare(
            dlpx_obj.server_session,
            get_references.find_obj_by_name(
                dlpx_obj.server_session, selfservice.bookmark, bookmark_name
            ).reference,
        )
        dx_logging.print_info(
            f"Bookmark {bookmark_name} was unshared " f"successfully."
        )
    except (
        dlpx_exceptions.DlpxException,
        exceptions.HttpError,
        exceptions.RequestError,
    ) as err:
        dx_logging.print_exception(
            f"\nERROR: {bookmark_name} could not be " f"unshared. The error was:\n{err}"
        )


def share_bookmark(dlpx_obj, bookmark_name):
    """
    Share a bookmark
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param bookmark_name: Name of the bookmark to share
    :type bookmark_name: str
    """
    try:
        selfservice.bookmark.share(
            dlpx_obj.server_session,
            get_references.find_obj_by_name(
                dlpx_obj.server_session, selfservice.bookmark, bookmark_name
            ).reference,
        )
        dx_logging.print_info(f"{bookmark_name} was shared successfully.")
    except (exceptions.HttpError, exceptions.RequestError) as err:
        dx_logging.print_exception(
            f"ERROR: {bookmark_name} could not be " f"shared. The error was:\n{err}"
        )


def update_bookmark(dlpx_obj, bookmark_name):
    """
    Updates a bookmark

    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param bookmark_name: Name of the bookmark to update
    :type bookmark_name: str
    """
    ss_bookmark_obj = vo.JSBookmark()
    try:
        selfservice.bookmark.update(
            dlpx_obj.server_session,
            get_references.find_obj_by_name(
                dlpx_obj.server_session, selfservice.bookmark, bookmark_name
            ).reference,
            ss_bookmark_obj,
        )
    except (
        dlpx_exceptions.DlpxException,
        exceptions.HttpError,
        exceptions.RequestError,
    ) as err:
        dx_logging.print_exception(
            f"ERROR: {bookmark_name} could not be " f"updated. The error was:\n{err}"
        )


def delete_bookmark(dlpx_obj, bookmark_name):
    """
    Deletes a bookmark

    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.get_session.GetSession object
    :param bookmark_name: Bookmark to delete
    :type bookmark_name: str
    """
    try:
        selfservice.bookmark.delete(
            dlpx_obj.server_session,
            get_references.find_obj_by_name(
                dlpx_obj.server_session, selfservice.bookmark, bookmark_name
            ).reference,
        )
        dx_logging.print_info(f"{bookmark_name} was deleted successfully.")
    except (
        dlpx_exceptions.DlpxException,
        exceptions.HttpError,
        exceptions.RequestError,
    ) as err:
        dx_logging.print_exception(
            f"ERROR: The bookmark {bookmark_name} "
            f"was not deleted. The error was:\n{err}"
        )


@run_async
def main_workflow(engine, dlpx_obj, single_thread):
    """
    This function is where we create our main workflow.
    Use the @run_async decorator to run this function asynchronously.
    The @run_async decorator allows us to run against multiple Delphix Engine
    simultaneously

    :param engine: Dictionary of engines
    :type engine: dictionary
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param single_thread: True - run single threaded, False - run multi-thread
    :type single_thread: bool
    """
    try:
        # Setup the connection to the Delphix DDP
        dlpx_obj.dlpx_session(
            engine["ip_address"], engine["username"], engine["password"]
        )
    except dlpx_exceptions.DlpxException as err:
        dx_logging.print_exception(
            f"ERROR: ss_bookmark encountered an error "
            f'authenticating to {engine["hostname"]} '
            f'{ARGUMENTS["--target"]}:\n{err}\n'
        )
    thingstodo = ["thingstodo"]
    try:
        with dlpx_obj.job_mode(single_thread):
            while dlpx_obj.jobs or thingstodo:
                if thingstodo:
                    if ARGUMENTS["--create_bookmark"]:
                        create_bookmark(
                            dlpx_obj,
                            ARGUMENTS["--create_bookmark"],
                            ARGUMENTS["--data_layout"],
                            ARGUMENTS["--branch_name"]
                            if ARGUMENTS["--branch_name"]
                            else None,
                            ARGUMENTS["--tags"] if ARGUMENTS["--tags"] else None,
                            ARGUMENTS["--description"]
                            if ARGUMENTS["--description"]
                            else None,
                        )
                    elif ARGUMENTS["--delete_bookmark"]:
                        delete_bookmark(dlpx_obj, ARGUMENTS["--delete_bookmark"])
                    elif ARGUMENTS["--update_bookmark"]:
                        update_bookmark(dlpx_obj, ARGUMENTS["--update_bookmark"])
                    elif ARGUMENTS["--share_bookmark"]:
                        share_bookmark(dlpx_obj, ARGUMENTS["--share_bookmark"])
                    elif ARGUMENTS["--unshare_bookmark"]:
                        unshare_bookmark(dlpx_obj, ARGUMENTS["--unshare_bookmark"])
                    elif ARGUMENTS["--list"]:
                        list_bookmarks(
                            dlpx_obj,
                            ARGUMENTS["--tags"] if ARGUMENTS["--tags"] else None,
                        )
                    thingstodo.pop()
    except (
        dlpx_exceptions.DlpxException,
        exceptions.RequestError,
        exceptions.JobError,
        exceptions.HttpError,
    ) as err:
        dx_logging.print_exception(
            f"Error in ss_bookmark:" f'{engine["ip_address"]}\n{err}'
        )
    run_job.find_job_state(engine, dlpx_obj)


def main():
    """
    main function - creates session and runs jobs
    """
    time_start = time.time()
    try:
        dx_session_obj = get_session.GetSession()
        dx_logging.logging_est(ARGUMENTS["--logdir"])
        config_file_path = ARGUMENTS["--config"]
        single_thread = ARGUMENTS["--single_thread"]
        engine = ARGUMENTS["--engine"]
        dx_session_obj.get_config(config_file_path)
        # This is the function that will handle processing main_workflow for
        # all the servers.
        for each in run_job.run_job(
            main_workflow, dx_session_obj, engine, single_thread
        ):
            # join them back together so that we wait for all threads to
            # complete
            each.join()
        elapsed_minutes = run_job.time_elapsed(time_start)
        dx_logging.print_info(
            f"script took {elapsed_minutes} minutes to " f"get this far."
        )
    # Here we handle what we do when the unexpected happens
    except SystemExit as err:
        # This is what we use to handle our sys.exit(#)
        sys.exit(err)

    except dlpx_exceptions.DlpxException as err:
        # We use this exception handler when an error occurs in a function
        # call.
        dx_logging.print_exception(
            f"ERROR: Please check the ERROR message " f"below:\n {err.error}"
        )
        sys.exit(2)

    except exceptions.HttpError as err:
        # We use this exception handler when our connection to Delphix fails
        dx_logging.print_exception(
            f"ERROR: Connection failed to the Delphix DDP. Please check "
            f"the ERROR message below:\n{err.status}"
        )
        sys.exit(2)

    except exceptions.JobError as err:
        # We use this exception handler when a job fails in Delphix so that we
        # have actionable data
        elapsed_minutes = run_job.time_elapsed(time_start)
        dx_logging.print_exception(
            f"A job failed in the Delphix Engine:\n{err.job}."
            f"{basename(__file__)} took {elapsed_minutes} minutes to get "
            f"this far"
        )
        sys.exit(3)

    except KeyboardInterrupt:
        # We use this exception handler to gracefully handle ctrl+c exits
        dx_logging.print_debug("You sent a CTRL+C to interrupt the process")
        elapsed_minutes = run_job.time_elapsed(time_start)
        dx_logging.print_info(
            f"{basename(__file__)} took {elapsed_minutes} " f"minutes to get this far."
        )


if __name__ == "__main__":
    # Grab our ARGUMENTS from the doc at the top of the script
    ARGUMENTS = docopt.docopt(__doc__, version=basename(__file__) + " " + VERSION)
    # Feed our ARGUMENTS to the main function, and off we go!
    main()
