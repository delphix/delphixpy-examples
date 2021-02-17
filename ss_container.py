#!/usr/bin/env python3
# Program Name : ss_container.py
# Description  : Delphix implementation script
# Author       : Corey Brune
#
# Copyright (c) 2019 by Delphix.
# All rights reserved.
# See http://docs.delphix.com/display/PS/Copyright+Statement for details
#
# Warranty details provided in external file
# for customers who have purchased support.
#
"""Create, delete, refresh and list JS containers.
Usage:
    ss_container.py (--list | --create_container <name>  \
    --template_name <name> --database <name>) | \
    --delete_container <name> [--keep_vdbs] | \
    --restore_container <name> --bookmark_name <name> | \
    --remove_owner <name> --container_name <name> | \
    --add_owner <name> --container_name <name> | \
    --refresh_container <name> | --reset_container <name> | \
    --list_hierarchy <name>
    [--engine <enginename> --poll <n> --parallel <n>]
    [--single_thread <bool> --config <path_to_file>]
    [--logdir <path_to_file>]

    ss_container.py -h | --help | -v | --version

Creates, Lists, Removes a Self-Service Data Pod

Examples:
  ss_container.py --list
  ss_container.py --list_hierarchy suiteCRM-Dev-DataPod
  ss_container.py --add_owner dev --container_name suiteCRM-Dev-DataPod
  ss_container.py --create_container sscontainer1 --database <name>:<name> \
  --template_name jstemplate1
  ss_container.py --delete_container sscontainer1
  ss_container.py --refresh_container sscontainer1
  ss_container.py --add_owner jsuser --container_name sscontainer1
  ss_container.py --remove_owner jsuser --container_name sscontainer1
  ss_container.py --refresh_container sscontainer1
  ss_container.py --restore_container sscontainer1 --bookmark_name ssbookmark1
  js_conatiner.py --reset_container sscontainer1

Options:
  --create_container <name>  Name of the new SS Container
                             [default:None]
  --container_name <name>    Name of the SS Container
  --refresh_container <name> Name of the new SS Container
  --restore_container <name> Name of the SS Container to restore
  --reset_container <name>   Reset last data operation
  --template_name <name>     Name of the JS Template to use for the container
  --add_owner <name>         Name of the JS Owner for the container
  --remove_owner <name>      Name of the JS Owner to remove
  --bookmark_name <name>     Name of the JS Bookmark to restore the container
  --keep_vdbs                If set, deleting the container will not remove
                             the underlying VDB(s)
  --list_hierarchy <name>    Lists hierarchy of a given container name
  --delete_container <name>  Delete the SS Container
  --database <name>          Name of the child database(s) to use for the
                             SS Container
  --list                     List the containers on a given engine
  --engine <enginename>      dentifier of Delphix DDP in dxtools.conf.
                             [default: default]
  --single_thread <boolean> Run as a single thread. Use True if there are
                            multiple engines and the operation needs to run
                            in parallel.
                            [default: True]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./config/dxtools.conf]
  --logdir <path_to_file>   The path to the logfile you want to use.
                            [default: ./logs/ss_container.log]
  -h --help                 Show this screen.
  -v --version              Show version.
"""
import sys
import time
from os.path import basename

from docopt import docopt

from delphixpy.v1_10_2 import exceptions
from delphixpy.v1_10_2.web import database
from delphixpy.v1_10_2.web import selfservice
from delphixpy.v1_10_2.web import user
from delphixpy.v1_10_2.web import vo
from lib import dlpx_exceptions
from lib import dx_logging
from lib import get_references
from lib import get_session
from lib import run_job
from lib.run_async import run_async

VERSION = "v.0.3.001"


def create_container(dlpx_obj, template_name, container_name, database_name):
    """
    Create the SS container
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param template_name: name of the self-service template
    :type template_name: str
    :param container_name: Name of the container to create
    :type container_name: str
    :param database_name: Name of the database(s) to use in the container
    :type database_name: str
    :return created container reference
    :rtype str
    """
    ss_container_params = vo.JSDataContainerCreateWithoutRefreshParameters()
    container_ds_lst = []
    for data_set in database_name.split(":"):
        container_ds_lst.append(
            get_references.build_data_source_params(dlpx_obj, database,
                                                    data_set)
        )
    try:
        ss_template_ref = get_references.find_obj_by_name(
            dlpx_obj.server_session, selfservice.template, template_name
        ).reference
        ss_container_params.template = ss_template_ref
        ss_container_params.timeline_point_parameters = (
            vo.JSTimelinePointLatestTimeInput()
        )
        ss_container_params.timeline_point_parameters.sourceDataLayout = \
            ss_template_ref
        ss_container_params.data_sources = container_ds_lst
        ss_container_params.name = container_name
        container_ref = selfservice.container.create(
            dlpx_obj.server_session, ss_container_params
        )
        dlpx_obj.jobs[
            dlpx_obj.server_session.address
        ] = dlpx_obj.server_session.last_job
        return container_ref
    except (
        dlpx_exceptions.DlpxException,
        exceptions.RequestError,
        exceptions.HttpError,
    ) as err:
        dx_logging.print_exception(
            f'Container {container_name} was not created. The error was: {err}'
        )


def remove_owner(dlpx_obj, owner_name, container_name):
    """
    Removes an owner from a container
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param owner_name: Name of the owner to remove
    :type owner_name: str
    :param container_name: Name of the container
    :type container_name: str
    """
    owner_params = vo.JSDataContainerModifyOwnerParameters()
    try:
        owner_params.owner = get_references.find_obj_by_name(
            dlpx_obj.server_session, user, owner_name
        ).reference
        container_obj = get_references.find_obj_by_name(
            dlpx_obj.server_session, selfservice.container, container_name
        )
        selfservice.container.remove_owner(
            dlpx_obj.server_session, container_obj.reference, owner_params
        )
    except (
        dlpx_exceptions.DlpxObjectNotFound,
        exceptions.RequestError,
        exceptions.HttpError,
    ) as err:
        dx_logging.print_exception(
            f'The user was not added to container '
            f'{container_name}. The error was:\n{err}'
        )


def restore_container(dlpx_obj, container_name, bookmark_name):
    """
    Restores a container to a given JS bookmark
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param container_name: Name of the container
    :type container_name: str
    :param bookmark_name: Name of the bookmark to restore
    :type bookmark_name: str
    """
    bookmark_params = vo.JSDataContainerRestoreParameters()
    bookmark_params.timeline_point_parameters = vo.JSTimelinePointBookmarkInput()
    bookmark_params.timeline_point_parameters.bookmark = (
        get_references.find_obj_by_name(
            dlpx_obj.server_session, selfservice.bookmark, bookmark_name
        ).reference
    )
    bookmark_params.force_option = False
    try:
        selfservice.container.restore(
            dlpx_obj.server_session,
            get_references.find_obj_by_name(
                dlpx_obj.server_session, selfservice.container, container_name
            ).reference,
            bookmark_params,
        )
        dlpx_obj.jobs[
            dlpx_obj.server_session.address
        ] = dlpx_obj.server_session.last_job
    except (
        dlpx_exceptions.DlpxObjectNotFound,
        exceptions.RequestError,
        exceptions.HttpError,
    ) as err:
        dx_logging.print_exception(f'The container was not restored:\n{err}')


def add_owner(dlpx_obj, owner_name, container_name):
    """
    Adds an owner to a container
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param owner_name: Grant authorizations for the given user on this
    container and parent template
    :type owner_name: str
    :param container_name: Name of the container
    :type container_name: str
    """
    owner_params = vo.JSDataContainerModifyOwnerParameters()
    try:
        owner_params.owner = get_references.find_obj_by_name(
            dlpx_obj.server_session, user, owner_name
        ).reference
        selfservice.container.add_owner(
            dlpx_obj.server_session,
            get_references.find_obj_by_name(
                dlpx_obj.server_session, selfservice.container, container_name
            ).reference,
            owner_params,
        )
    except (
        dlpx_exceptions.DlpxObjectNotFound,
        exceptions.RequestError,
        exceptions.HttpError,
    ) as err:
        dx_logging.print_exception(
            f'The user was not removed from the container '
            f'{container_name}:\n{err}'
        )

def refresh_container(dlpx_obj, container_name):
    """
    Refreshes a container
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param container_name: Name of the container to refresh
    :type container_name: str
    """
    try:
        selfservice.container.refresh(
            dlpx_obj.server_session,
            get_references.find_obj_by_name(
                dlpx_obj.server_session, selfservice.container, container_name
            ).reference,
        )
        dlpx_obj.jobs[
            dlpx_obj.server_session.address
        ] = dlpx_obj.server_session.last_job
    except (
        dlpx_exceptions.DlpxObjectNotFound,
        exceptions.RequestError,
        exceptions.HttpError,
    ) as err:
        dx_logging.print_exception(
            f'Container {container_name} was not refreshed. '
            f'The error was:\n{err}'
        )


def delete_container(dlpx_obj, container_name, keep_vdbs=False):
    """
    Deletes a container
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param container_name: Name of the container to delete
    :type container_name: str
    :param keep_vdbs: When deleting the container, delete the VDBs as well
    if set to True
    :type keep_vdbs: bool
    """
    try:
        if keep_vdbs:
            ss_container_params = vo.JSDataContainerDeleteParameters()
            ss_container_params.delete_data_sources = False
            selfservice.container.delete(
                dlpx_obj.server_session,
                get_references.find_obj_by_name(
                    dlpx_obj.server_session, selfservice.container,
                    container_name
                ).reference,
                ss_container_params,
            )
        elif keep_vdbs is False:
            selfservice.container.delete(
                dlpx_obj.server_session,
                get_references.find_obj_by_name(
                    dlpx_obj.server_session, selfservice.container,
                    container_name
                ).reference,
            )
    except (
        dlpx_exceptions.DlpxException,
        exceptions.RequestError,
        exceptions.HttpError,
    ) as err:
        dx_logging.print_exception(
            f'Container {container_name} was not deleted. '
            f'The error was:\n{err}'
        )


def list_containers(dlpx_obj):
    """
    List all containers on a given engine
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    """
    header = "Name, Active Branch, Owner, Reference, Template, Last Updated"
    ss_containers = selfservice.container.get_all(dlpx_obj.server_session)
    try:
        print(header)
        for ss_container in ss_containers:
            last_updated = get_references.convert_timestamp(
                dlpx_obj.server_session, ss_container.last_updated[:-5]
            )
            dx_logging.print_info(
                f'{ss_container.name}, {ss_container.active_branch}, '
                f'{ss_container.owner}, {ss_container.reference},'
                f'{ss_container.template}, {last_updated}'
            )
    except (
        dlpx_exceptions.DlpxException,
        exceptions.HttpError,
        exceptions.RequestError,
    ) as err:
        dx_logging.print_exception(
            f'ERROR: SS Containers could not be listed. The error was:\n{err}'
        )


def reset_container(dlpx_obj, container_name):
    """
    Undo the last refresh or restore operation
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param container_name: Name of the container to reset
    :type container_name: str
    """
    try:
        selfservice.container.reset(
            dlpx_obj.server_session,
            get_references.find_obj_by_name(
                dlpx_obj.server_session, selfservice.container, container_name
            ).reference,
        )
    except exceptions.RequestError as err:
        dx_logging.print_exception(
            f'ERROR: SS Container was not reset. The error was:\n{err}'
        )


def list_hierarchy(dlpx_obj, container_name):
    """
    Filter container listing.
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param container_name: Name of the container to list child VDBs
    :type container_name: str
    """
    database_dct = {}
    layout_ref = get_references.find_obj_by_name(
        dlpx_obj.server_session, selfservice.container, container_name
    ).reference
    for data_source in selfservice.datasource.get_all(
        dlpx_obj.server_session, data_layout=layout_ref
    ):
        db_name = get_references.find_obj_name(
            dlpx_obj.server_session, database, data_source.container
        )

        if hasattr(data_source.runtime, 'instance_jdbc_string'):
            database_dct[db_name] = data_source.runtime.instance_jdbc_string
        else:
            database_dct[db_name] = None
    try:
        dx_logging.print_info(
            f'Container: {container_name}\n'
            f'Related VDBs: '
            f'{convert_dct_str(database_dct)}\n'
        )
    except AttributeError as err:
        dx_logging.print_exception(err)
        raise dlpx_exceptions.DlpxException(err)


def convert_dct_str(obj_dct):
    """
    Convert dictionary into a string for printing
    :param obj_dct: Dictionary to convert into a string
    :type obj_dct: dict
    :return: string object
    """
    js_str = ""
    if isinstance(obj_dct, dict):
        for js_db, js_jdbc in obj_dct.items():
            if isinstance(js_jdbc, list):
                js_str += f'{js_db}: {", ".join(js_jdbc)}\n'
            elif isinstance(js_jdbc, str):
                js_str += f'{js_db}: {js_jdbc}\n'
    else:
        raise dlpx_exceptions.DlpxException(
            f'Passed a non-dictionary object to convert_dct_str():'
            f'{type(obj_dct)}'
        )
    return js_str


@run_async
def main_workflow(engine, dlpx_obj, single_thread):
    """
    This function is where we create our main workflow.
    Use the @run_async decorator to run this function asynchronously.
    The @run_async decorator allows us to run against multiple Delphix Engine
    simultaneously
    :param engine: Dictionary of engines in the config file
    :type engine: dict
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param single_thread: True - run single threaded, False - run multi-thread
    :type single_thread: bool
    """
    try:
        # Setup the connection to the Delphix Engine
        dlpx_obj.dlpx_session(
            engine['ip_address'], engine['username'], engine['password'],
            engine['use_https']
        )
    except dlpx_exceptions.DlpxObjectNotFound as err:
        dx_logging.print_exception(
            f'ERROR: Delphix Engine {engine["ip_address"]} encountered '
            f"an error while creating the session:\n{err}\n"
        )
    try:
        with dlpx_obj.job_mode(single_thread):
            if ARGUMENTS['--create_container']:
                create_container(
                    dlpx_obj,
                    ARGUMENTS['--template_name'],
                    ARGUMENTS['--create_container'],
                    ARGUMENTS['--database'],
                )
                dx_logging.print_info(
                    f'Self Service Container {ARGUMENTS["--create_container"]}'
                    f'was created successfully.'
                )
            elif ARGUMENTS['--delete_container']:
                delete_container(
                    dlpx_obj,
                    ARGUMENTS['--delete_container'],
                    ARGUMENTS['--keep_vdbs'],
                )
            elif ARGUMENTS['--list']:
                list_containers(dlpx_obj)
            elif ARGUMENTS['--remove_owner']:
                remove_owner(
                    dlpx_obj,
                    ARGUMENTS['--remove_owner'],
                    ARGUMENTS['--container_name'],
                )
                dx_logging.print_info(
                    f'User {ARGUMENTS["--remove_owner"]} had '
                    f'access revoked from '
                    f'{ARGUMENTS["--container_name"]}'
                )
            elif ARGUMENTS['--restore_container']:
                restore_container(
                    dlpx_obj,
                    ARGUMENTS['--restore_container'],
                    ARGUMENTS['--bookmark_name'],
                )
                dx_logging.print_info(
                    f'Container {ARGUMENTS["--restore_container"]} '
                    f'was restored successfully with bookmark '
                    f'{ARGUMENTS["--bookmark_name"]}'
                )
            elif ARGUMENTS['--add_owner']:
                add_owner(
                    dlpx_obj,
                    ARGUMENTS['--add_owner'],
                    ARGUMENTS['--container_name'],
                )
                dx_logging.print_info(
                    f'User {ARGUMENTS["--add_owner"]} was granted '
                    f'access to {ARGUMENTS["--container_name"]}'
                )
            elif ARGUMENTS["--refresh_container"]:
                refresh_container(engine, dlpx_obj, ARGUMENTS["--refresh_container"])
                dx_logging.print_info(
                    f'The container {ARGUMENTS["--refresh_container"]}'
                    f' was refreshed.'
                )
            elif ARGUMENTS['--list_hierarchy']:
                list_hierarchy(dlpx_obj, ARGUMENTS['--list_hierarchy'])
            elif ARGUMENTS['--reset_container']:
                reset_container(dlpx_obj, ARGUMENTS['--reset_container'])
                print(f'Container {ARGUMENTS["--reset_container"]} was reset.')
    except (
        dlpx_exceptions.DlpxException,
        exceptions.RequestError,
        exceptions.JobError,
        exceptions.HttpError,
    ) as err:
        dx_logging.print_exception(
            f'Error in ss_container: {engine["hostname"]}\n{err}'
        )
    run_job.find_job_state(engine, dlpx_obj)


def main():
    """
    Main function - setup global variables and timer
    """
    time_start = time.time()
    try:
        dx_session_obj = get_session.GetSession()
        dx_logging.logging_est(ARGUMENTS['--logdir'])
        config_file_path = ARGUMENTS['--config']
        single_thread = ARGUMENTS["--single_thread"]
        engine = ARGUMENTS['--engine']
        dx_session_obj.get_config(config_file_path)
        for each in run_job.run_job_mt(
            main_workflow, dx_session_obj, engine, single_thread
        ):
            each.join()
        elapsed_minutes = run_job.time_elapsed(time_start)
        dx_logging.print_info(
            f'script took {elapsed_minutes} minutes to get this far.'
        )
    # Here we handle what we do when the unexpected happens
    except SystemExit as err:
        # This is what we use to handle our sys.exit(#)
        sys.exit(err)
    except dlpx_exceptions.DlpxException as err:
        # We use this exception handler when an error occurs in a function.
        dx_logging.print_exception(
            f'ERROR: Please check the ERROR message below:\n{err}'
        )
        sys.exit(2)
    except exceptions.HttpError as err:
        # We use this exception handler when our connection to Delphix fails
        print(
            f'ERROR: Connection failed to the Delphix Engine. Please '
            f'check the error message below:\n{err}'
        )
        sys.exit(2)
    except exceptions.JobError as err:
        # We use this exception handler when a job fails in Delphix so that we
        # have actionable data
        print(f'A job failed in the Delphix Engine:\n{err.job}')
        elapsed_minutes = run_job.time_elapsed(time_start)
        dx_logging.print_info(
            f'{basename(__file__)} took {elapsed_minutes} minutes to get this far.'
        )
        sys.exit(3)
    except KeyboardInterrupt:
        # We use this exception handler to gracefully handle ctrl+c exits
        dx_logging.print_debug('You sent a CTRL+C to interrupt the process')
        elapsed_minutes = run_job.time_elapsed(time_start)
        dx_logging.print_info(
            f'{basename(__file__)} took {elapsed_minutes} minutes to get this far.'
        )


if __name__ == '__main__':
    # Grab our ARGUMENTS from the doc at the top of the script
    ARGUMENTS = docopt(__doc__, version=basename(__file__) + ' ' + VERSION)
    # Feed our ARGUMENTS to the main function, and off we go!
    main()
