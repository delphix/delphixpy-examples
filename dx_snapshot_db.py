#!/usr/bin/env python3
# This script snapshots a vdb or dSource

# The below doc follows the POSIX compliant standards and allows us to use
# This doc to also define our ARGUMENTS for the script.
"""Snapshot dSources and VDB's

Usage:
  dx_snapshot_db.py (--group <name> |--name <name> | --all_dbs | --list)
                  [--engine <identifier>]
                  [--usebackup --bck_file <name> --parallel <n>]
                  [--poll <n> --create_bckup --single_thread <bool>]
                  [--config <path_to_file> --logdir <path_to_file>]
  dx_snapshot_db.py -h | --help | -v | --version

Snapshot a Delphix dSource or VDB

Examples:
  dx_snapshot_db.py --group "Sources" --usebackup
  dx_snapshot_db.py --name "Employee Oracle 11G DB"
  dx_snapshot_db.py --name dbw2 --usebackup --group Sources --create_bckup
  dx_snapshot_db.py --name dbw2 --usebackup --group Production \
  --bck_file dbw2_full_20170317_001.dmp


Options:
  --engine <identifier>     Alt Identifier of Delphix engine in dxtools.conf.
                            [default: default]
  --all_dbs                 Run against all database objects
  --list                    List all snapshots
  --single_thread           Run as a single thread. False if running multiple
                            threads.
                            [default: False]
  --bck_file <name>         Name of the specific ASE Sybase backup file(s)
                            or backup uuid for MSSQL.
                            [default: None]
  --name <name>             Name of object in Delphix to execute against.
  --group <name>            Name of group in Delphix to execute against.
  --usebackup               Snapshot using "Most Recent backup".
                            Available for MSSQL and ASE only.
                            [default: False]
  --create_bckup            Create and ingest a new Sybase backup or
                            copy-only MS SQL backup
                            [default: False]
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./config/dxtools.conf]
  --logdir <path_to_file>    The path to the logfile you want to use.
                            [default: ./logs/dx_snapshot_db.log]
  -h --help                 Show this screen.
  -v --version              Show version.
"""

import sys
import time
from os.path import basename

import docopt

from delphixpy.v1_10_2 import exceptions
from delphixpy.v1_10_2.web import database
from delphixpy.v1_10_2.web import snapshot
from delphixpy.v1_10_2.web import source
from delphixpy.v1_10_2.web import vo
from lib import dlpx_exceptions
from lib import dx_logging
from lib import get_references
from lib import get_session
from lib import run_job
from lib.run_async import run_async

VERSION = "v.0.3.003"

def list_snapshots(dlpx_obj, db_name=None):
    """
    """
    if db_name:
        snapshots = snapshot.get_all(dlpx_obj, database=db_name)
        for snap in snapshots:
            print(snap)
    elif db_name is None:
        snapshots = snapshot.get_all(dlpx_obj)
        for snap in snapshots:
            print(snap)

def snapshot_database(
    dlpx_obj,
    db_name=None,
    all_or_group_dbs=None,
    use_backup=False,
    backup_file=None,
    create_bckup=False,
):
    """
    Create a snapshot (sync) of a dSource or VDB
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param db_name: Name of the VDB or dSource to snapshot
    :type db_name: str or None
    :param all_or_group_dbs: List containing all DB on or all DBs in a group
    :type all_or_group_dbs: list or None
    :param use_backup: Snapshot using "Most recent backup"
    :type use_backup: bool
    :param backup_file: File to use for this snapshot
    :type backup_file: str or None
    :param create_bckup: Create a backup to use for the snapshot
    :type create_bckup: bool
    """
    sync_params = None
    if isinstance(db_name, str):
        all_or_group_dbs = [db_name]

    for db_sync in all_or_group_dbs:
        try:
            db_source_info = get_references.find_obj_by_name(
                dlpx_obj.server_session, source, db_sync
            )
            container_obj_ref = get_references.find_obj_by_name(
                dlpx_obj.server_session, database, db_sync
            ).reference
        except dlpx_exceptions.DlpxObjectNotFound as err:
            raise dlpx_exceptions.DlpxException from err
        if db_source_info.staging:
            raise dlpx_exceptions.DlpxException(
                f"{db_sync} is a staging " f"database. Cannot Sync.\n"
            )
        if db_source_info.runtime.enabled != "ENABLED":
            raise dlpx_exceptions.DlpxException(
                f"{db_sync} is not enabled " f"database. Cannot Sync.\n"
            )
        if db_source_info.runtime.enabled == "ENABLED":
            # If the database is a dSource and a MSSQL type, we need to tell
            # Delphix how we want to sync the database.
            # Delphix will just ignore the extra parameters if it is a VDB,
            # so we will omit any extra code to check
            if db_source_info.type == "MSSqlLinkedSource":
                if create_bckup is True:
                    sync_params = vo.MSSqlNewCopyOnlyFullBackupSyncParameters()
                    sync_params.compression_enabled = False
                elif use_backup is True:
                    if backup_file != None:
                        sync_params = vo.MSSqlExistingSpecificBackupSyncParameters()
                        sync_params.backup_uuid = backup_file
                    else:
                        sync_params = vo.MSSqlExistingMostRecentBackupSyncParameters()
            # Else if the database is a dSource and a ASE type, we need also to
            # tell Delphix how we want to sync the database...
            # Delphix will just ignore the extra parameters if it is a VDB, so
            # we will omit any extra code to check
            elif db_source_info.type == "ASELinkedSource":
                if use_backup is True:
                    if backup_file:
                        sync_params = vo.ASESpecificBackupSyncParameters()
                        sync_params.backup_files = backup_file.split(" ")
                    elif create_bckup:
                        sync_params = vo.ASENewBackupSyncParameters()
                    else:
                        sync_params = vo.ASELatestBackupSyncParameters()
                else:
                    sync_params = vo.ASENewBackupSyncParameters()
            if sync_params:
                database.sync(dlpx_obj.server_session, container_obj_ref, sync_params)
            else:
                database.sync(dlpx_obj.server_session, container_obj_ref)
            # Add the job into the jobs dictionary so we can track its progress
            dlpx_obj.jobs[dlpx_obj.server_session.address].append(
                dlpx_obj.server_session.last_job
            )
        print(container_obj_ref)
        return(dlpx_obj.server_session.last_job)


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
            engine["ip_address"],
            engine["username"],
            engine["password"],
            engine["use_https"],
        )
    except dlpx_exceptions.DlpxException as err:
        dx_logging.print_exception(
            f"ERROR: {basename(__file__)} encountered an error authenticating"
            f' to {engine["hostname"]} {ARGUMENTS["--target"]}:\n{err}'
        )
    try:
        with dlpx_obj.job_mode(single_thread):
            if ARGUMENTS["--name"] is not None:
                last_job = snapshot_database(
                    dlpx_obj,
                    ARGUMENTS["--name"],
                    None,
                    ARGUMENTS["--usebackup"],
                    ARGUMENTS["--bck_file"],
                    ARGUMENTS["--create_bckup"],
                )
            if ARGUMENTS["--group"]:
                databases = get_references.find_all_databases_by_group(
                    dlpx_obj.server_session, ARGUMENTS["--group"]
                )
                database_lst = []
                for db_name in databases:
                    database_lst.append(db_name.name)
                snapshot_database(
                    dlpx_obj,
                    None,
                    database_lst,
                    ARGUMENTS["--usebackup"],
                    ARGUMENTS["--bck_file"],
                    ARGUMENTS["--create_bckup"],
                )
            elif ARGUMENTS["--all_dbs"]:
                # Grab all databases
                databases = database.get_all(
                    dlpx_obj.server_session, no_js_data_source=False
                )
                database_lst = []
                for db_name in databases:
                    database_lst.append(db_name.name)
                snapshot_database(
                    dlpx_obj,
                    None,
                    database_lst,
                    ARGUMENTS["--usebackup"],
                    ARGUMENTS["--bck_file"],
                    ARGUMENTS["--create_bckup"],
                )
            elif ARGUMENTS["--list"]:
                list_snapshots(dlpx_obj.server_session)
            run_job.track_running_jobs(engine, dlpx_obj)
            snap_name = run_job.find_snapshot_ref_jobid(
                dlpx_obj.server_session, dlpx_obj.server_session.last_job
            )
            if snap_name:
                snap_obj = get_references.find_obj_by_name(
                    dlpx_obj.server_session, snapshot, f'{snap_name}'
                )
            print(snap_obj.name)
                
    except (
        dlpx_exceptions.DlpxObjectNotFound,
        exceptions.RequestError,
        exceptions.JobError,
        exceptions.HttpError,
    ) as err:
        dx_logging.print_exception(
            f'Error in {basename(__file__)}: {engine["hostname"]}\n{err}'
        )


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
