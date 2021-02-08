#!/usr/bin/env python3
# Program Name : ss_branch.py
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
  ss_branch.py  (--create_branch <name> --container_name <name>
                [--template_name <name> | --bookmark_name <name>] [--timestamp <timestamp>] | \
                 --list | --delete_branch <name> | --activate_branch <name>
                 )
                [--single_thread <bool>]
                [--engine <enginename>]
                [--config <path_to_file>]
                [--logdir <path_to_file>]
  ss_branch.py -h | --help | -v | --version

Creates, Lists, Removes a Jet Stream Branch

Examples:
  ss_branch.py --list
  ss_branch.py --create_branch jsbranch1 --container_name jscontainer \
  --template_name jstemplate1
  ss_branch.py --activate_branch jsbranch1
  ss_branch.py --delete_branch jsbranch1
  ss_branch.py --create_branch tb4 --container_name dc1 \
    --timestamp "2021-02-07T04:34:48.952Z"

Options:
  --create_branch <name>    Name of the new JS Branch
  --bookmark_name <name     Name of the bookmark to create the branch
  --container_name <name>   Name of the container to use
  --template_name <name>    Name of the template to use
  --timestamp <timestamp>   Timestamp on active branch to create branch from
  --activate_branch <name>  Name of the branch to activate
  --delete_branch <name>    Delete the JS Branch
  --list                    List the branchs on a given engine
  --engine <type>           Alt Identifier of Delphix engine in dxtools.conf.
                            [default: default]
  --single_thread           Asynchronous/Synchronous mode
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./config/dxtools.conf]
  --logdir <path_to_file>   The path to the logfile you want to use.
                            [default: ./logs/ss_branch.log]
  -h --help                 Show this screen.
  -v --version              Show version.
"""
import datetime
import re
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

VERSION = "v.0.3.001"


def create_branch(
        dlpx_obj, branch_name, container_name, template_name=None, bookmark_name=None, timestamp=None
):
    """
    Create a Self-Service Branch
    :param dlpx_obj: Virtualization Engine session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param branch_name: Name of the branch to create
    :type branch_name: str
    :param container_name: Name of the container to use
    :type container_name: str
    :param template_name: Name of the template to use
    :type template_name: str
    :param bookmark_name: Name of the bookmark to use
    :type bookmark_name: str
    """
    ss_branch = vo.JSBranchCreateParameters()
    ss_branch.name = branch_name
    data_container_obj = get_references.find_obj_by_name(
        dlpx_obj.server_session, selfservice.container, container_name
    )
    ss_branch.data_container = data_container_obj.reference
    if timestamp:
        ss_branch.timeline_point_parameters = vo.JSTimelinePointTimeInput()
        ss_branch.timeline_point_parameters.time = timestamp
        ss_branch.timeline_point_parameters.branch = data_container_obj.active_branch
    elif bookmark_name:
        ss_branch.timeline_point_parameters = vo.JSTimelinePointBookmarkInput()
        ss_branch.timeline_point_parameters.bookmark = get_references.find_obj_by_name(
            dlpx_obj.server_session, selfservice.bookmark, bookmark_name
        ).reference
    elif template_name or container_name:
        if template_name:
            source_layout_ref = get_references.find_obj_by_name(
                dlpx_obj.server_session, selfservice.template, template_name
            ).reference
        else:
            source_layout_ref = get_references.find_obj_by_name(
                dlpx_obj.server_session, selfservice.container, container_name
            ).reference
        ss_branch.timeline_point_parameters = vo.JSTimelinePointLatestTimeInput()
        ss_branch.timeline_point_parameters.source_data_layout = source_layout_ref
    try:
        selfservice.branch.create(dlpx_obj.server_session, ss_branch)
        dlpx_obj.jobs[
            dlpx_obj.server_session.address
        ].append(dlpx_obj.server_session.last_job)
    except (exceptions.RequestError, exceptions.HttpError) as err:
        raise dlpx_exceptions.DlpxException(
            f"The branch was not created. The error was:\n{err}"
        )
    dx_logging.print_info(
        f"Self Service Branch {branch_name} is being created\n"
    )


def list_branches(dlpx_obj):
    """
    List all branches on a given engine
    :param dlpx_obj: Virtualization Engine session object
    :type dlpx_obj: lib.GetSession.GetSession object
    """
    try:
        #header = "\nBranch Name Data Layout Reference End Time"
        js_data_layout = ""
        ss_branches = selfservice.branch.get_all(dlpx_obj.server_session)
        if ss_branches:
            dx_logging.print_info("="*130)
            header = "{:<25} {:<25} {:<25} {:<25} {:<25}".format('Branch Name','Data Layout','Layout Type',' Branch Reference','End Time')
            dx_logging.print_info(header)
            dx_logging.print_info("-"*130)
            for ss_branch in ss_branches:
                js_end_time = selfservice.operation.get(
                    dlpx_obj.server_session, ss_branch.first_operation
                ).end_time
                js_obj_type="CONTAINER"
                if re.search("TEMPLATE", ss_branch.data_layout):
                    js_data_layout = get_references.find_obj_name(
                        dlpx_obj.server_session, selfservice.template, ss_branch.data_layout
                    )
                    js_obj_type = "TEMPLATE"
                elif re.search("CONTAINER", ss_branch.data_layout):
                    js_data_layout = get_references.find_obj_name(
                        dlpx_obj.server_session,
                        selfservice.container,
                        ss_branch.data_layout,
                    )
                dx_logging.print_info("{:<25} {:<25} {:<25} {:<25} {:<25}".format(ss_branch._name[0],js_data_layout,js_obj_type,ss_branch.reference,js_end_time))
            dx_logging.print_info("="*130)
        else:
            dx_logging.print_info(f"No branches found on engine.")

    except dlpx_exceptions.DlpxException as err:
        raise (
            f"ERROR: Self Service Branches could not be listed. The error "
            f"was:\n{err}"
        )


def update_branch(dlpx_obj, branch_name):
    """
    Updates a branch
    :param dlpx_obj: Virtualization Engine session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param branch_name: Name of the branch to update
    :type branch_name: str
    """

    ss_branch_obj = vo.JSBranch()
    try:
        branch_obj = get_references.find_obj_name(
            dlpx_obj.server_session, selfservice.branch, branch_name
        )
        selfservice.branch.update(
            dlpx_obj.server_session, branch_obj.reference, ss_branch_obj
        )
    except (
        dlpx_exceptions.DlpxException,
        exceptions.HttpError,
        exceptions.RequestError,
    ) as err:
        raise dlpx_exceptions.DlpxException(
            f"ERROR: The branch could not be " f"updated. The error was:{err}"
        )
    dx_logging.print_info(f"The branch {branch_name} was updated " f"successfully.\n")


def activate_branch(dlpx_obj, branch_name):
    """
    Activates a branch
    :param dlpx_obj: Virtualization Engine session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param branch_name: Name of the branch to activate
    :type branch_name: str
    """
    try:
        branch_obj = get_references.find_obj_by_name(
            dlpx_obj.server_session, selfservice.branch, branch_name
        )
        selfservice.branch.activate(dlpx_obj.server_session, branch_obj.reference)
        dlpx_obj.jobs[
            dlpx_obj.server_session.address
        ].append(dlpx_obj.server_session.last_job)
    except exceptions.RequestError as err:
        raise dlpx_exceptions.DlpxException(
            f"ERROR: An error occurred activating the {branch_name}:\n{err}"
        )
    dx_logging.print_info(f"The branch {branch_name} was activated " f"successfully.\n")


def delete_branch(dlpx_obj, branch_name):
    """
    Deletes a branch
    :param dlpx_obj: Virtualization Engine session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param branch_name: Branch to delete
    :type branch_name: str
    """

    try:
        branch_obj = get_references.find_obj_by_name(
            dlpx_obj.server_session, selfservice.branch, branch_name
        )
        selfservice.branch.delete(dlpx_obj.server_session, branch_obj.reference)
        dlpx_obj.jobs[
            dlpx_obj.server_session.address
        ].append(dlpx_obj.server_session.last_job)
    except (
            dlpx_exceptions.DlpxException,
            exceptions.HttpError,
            exceptions.RequestError,
    ) as err:
        dx_logging.print_exception(f"The branch could not be deleted: \n ERROR:{err}")
        raise dlpx_exceptions.DlpxException(
            f"ERROR: The branch was not deleted. The error was:\n{err}"
        )
    except Exception as err:
        dx_logging.print_exception(f"The branch could not be deleted: \n ERROR:{err}")
        raise err


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
            engine["ip_address"], engine["username"], engine["password"], engine["use_https"]
        )
    except dlpx_exceptions.DlpxException as err:
        dx_logging.print_exception(
            f"ERROR: dx_refresh_vdb encountered an error authenticating to "
            f'{engine["hostname"]} {ARGUMENTS["--target"]}:\n{err}\n'
        )
    try:
        with dlpx_obj.job_mode(single_thread):
            if ARGUMENTS["--create_branch"]:
                create_branch(
                    dlpx_obj,
                    ARGUMENTS["--create_branch"],
                    ARGUMENTS["--container_name"],
                    ARGUMENTS["--template_name"],
                    ARGUMENTS["--bookmark_name"],
                    ARGUMENTS["--timestamp"]
                )
            elif ARGUMENTS["--delete_branch"]:
                delete_branch(
                    dlpx_obj,
                    ARGUMENTS["--delete_branch"]
                )
            elif ARGUMENTS["--activate_branch"]:
                activate_branch(
                    dlpx_obj,
                    ARGUMENTS["--activate_branch"]
                )
            elif ARGUMENTS["--list"]:
                list_branches(dlpx_obj)
            run_job.track_running_jobs(engine, dlpx_obj)
    except (
            dlpx_exceptions.DlpxException,
            dlpx_exceptions.DlpxObjectNotFound,
            exceptions.RequestError,
            exceptions.JobError,
            exceptions.HttpError,
    ) as err:
        dx_logging.print_exception(
            f"Error in ss_branch:" f'{engine["ip_address"]}\n{err}'
        )
        raise err



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
        for each in run_job.run_job_mt(
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
