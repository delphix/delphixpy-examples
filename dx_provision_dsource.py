#!/usr/bin/env python3
# Corey Brune - Feb 2017
# Description:
# Create and sync a dSource
#
# Requirements
# pip install docopt delphixpy

# The below doc follows the POSIX compliant standards and allows us to use
# this doc to also define our ARGUMENTS for the script.
"""Create and sync a dSource
Usage:
  dx_provision_dsource.py --type <name> --dsource_name <name> --ip_addr <name>
  --db_name <name> --env_name <name> --db_install_path <name> --dx_group <name>
  --db_passwd <name> --db_user <name> [--port_num <name>]
  [--num_connections <name>][--link_now <name>][--files_per_set <name>]
  [--rman_channels <name>]
    [--engine <identifier> | --all]
    [--debug] [--parallel <n>] [--poll <n>]
    [--config <path_to_file>] [--logdir <path_to_file>]
  dx_provision_dsource.py --type <name> --dsource_name <name> --ase_user <name>
  --ase_passwd <name> --backup_path <name> --source_user <name>
  --stage_user aseadmin --stage_repo ASE1570_S2 --src_config <name>
  --env_name <name> --dx_group <name> [--bck_file <name>][--create_bckup]
    [--engine <identifier> | --all]
    [--debug] [--parallel <n>] [--poll <n>]
    [--config <path_to_file>] [--logdir <path_to_file>]
  dx_provision_dsource.py --type <name> --dsource_name <name> --dx_group <name>
  --db_passwd <name> --db_user <name> --stage_instance <name>
  --stage_env <name> --backup_path <name> [--backup_loc_passwd <passwd>
  --backup_loc_user <name> --logsync [--sync_mode <mode>] --load_from_backup]
    [--engine <identifier> | --all]
    [--debug] [--parallel <n>] [--poll <n>]
    [--config <path_to_file>] [--logdir <path_to_file>]
  dx_provision_dsource.py -h | --help | -v | --version

Create and sync a dSource
Examples:
    Oracle:
    dx_provision_dsource.py --type oracle --dsource_name oradb1
    --ip_addr 192.168.166.11 --db_name srcDB1 --env_name SourceEnv
    --db_install_path /u01/app/oracle/product/11.2.0.4/dbhome_1
    --db_user delphixdb --db_passwd delphixdb

    Sybase:
    dx_provision_dsource.py --type sybase --dsource_name dbw1 --ase_user sa
    --ase_passwd sybase --backup_path /data/db --source_user aseadmin
    --stage_user aseadmin --stage_repo ASE1570_S2 --src_config dbw1
    --env_name aseSource --dx_group Sources

    Specify backup files:
    dx_provision_dsource.py --type sybase --dsource_name dbw2 --ase_user sa
    --ase_passwd sybase --backup_path /data/db --source_user aseadmin
    --stage_user aseadmin --stage_repo ASE1570_S2 --src_config dbw2
    --env_name aseSource --dx_group Sources --bck_file "dbw2data.dat"

    Create a new backup and ingest:
    dx_provision_dsource.py --type sybase --dsource_name dbw2 --ase_user sa
    --ase_passwd sybase --backup_path /data/db --source_user aseadmin
    --stage_user aseadmin --stage_repo ASE1570_S2 --src_config dbw2
    --env_name aseSource --dx_group Sources --create_bckup

    MSSQL:
    dx_provision_dsource.py --type mssql  --dsource_name mssql_dsource
    --dx_group Sources --db_passwd delphix --db_user sa
    --stage_env mssql_target_svr --stage_instance MSSQLSERVER
    --backup_path \\bckserver\path\backups --backup_loc_passwd delphix
    --backup_loc_user delphix
    dx_provision_dsource.py --type mssql  --dsource_name AdventureWorks2014
    --dx_group "9 - Sources" --db_passwd delphixdb --db_user aw
    --stage_env WINDOWSTARGET --stage_instance MSSQLSERVER --logsync
    --backup_path auto --load_from_backup


Options:
  --type <name>             dSource type. mssql, sybase or oracle
                            [default: oracle]
  --ip_addr <name>          IP Address of the dSource
                            [default: None]
  --db_name <name>          Name of the dSource DB
  --env_name <name>         Name of the environment where the dSource installed
  --db_install_path <name>  Location of the installation path of the DB.
  --num_connections <name>  Number of connections for Oracle RMAN
                            [default: 5]
  --link_now <name>         Link the dSource
                            [default: True]
  --files_per_set <name>    Configures how many files per set for Oracle RMAN
                            [default: 5]
  --rman_channels <name>    Configures the number of Oracle RMAN Channels
                            [default: 2]
  --dx_group <name>         Group where the dSource will reside
  --create_bckup            Create and ingest a new Sybase backup
                            [default: None]
  --db_user <name>          Username of the dSource DB
                            [default: None]
  --db_passwd <name>        Password of the db_user
                            [default: None]
  --bck_file <name>         Fully qualified name of backup file
                            [default: None]
  --port_num <name>         Port number of the listener.
                            [default: 1521]
  --src_config <name>       Name of the configuration environment
  --ase_passwd <name>       ASE DB password
                            [default: None]
  --ase_user <name>         ASE username
                            [default: None]
  --backup_path <path>      Path to the ASE/MSSQL backups
                            [default: None]
  --sync_mode <name>        MSSQL validated sync mode
                            TRANSACTION_LOG|FULL_OR_DIFFERENTIAL|FULL|NONE
                            [default: FULL]
  --source_user <name>      Environment username
                            [default: delphix]
  --stage_user <name>       Stage username
                            [default: delphix]
  --stage_repo <name>       Stage repository
                            [default: None]
  --stage_instance <name>   Name of the PPT instance
                            [default: None]
  --stage_env <name>        Name of the PPT server
                            [default: None]
  --logsync                 Enable logsync
                            [default: True]
  --backup_loc_passwd <passwd>  Password of the shared backup path
                            [default: None]
  --backup_loc_user <nam>   User of the shared backup path
                            [default: None]
  --load_from_backup        If set, Delphix will try to load the most recent
                            full backup (MSSQL only)
                            [default: None]
  --dsource_name <name>     Name of the dSource
  --engine <type>           Alt Identifier of Delphix engine in dxtools.conf.
  --all                     Run against all engines.
  --debug                   Enable debug logging
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./dxtools.conf]
  --logdir <path_to_file>    The path to the logfile you want to use.
                            [default: ./dx_provision_dsource.log]
  -h --help                 Show this screen.
  -v --version              Show version.
"""
import sys
import time
from os.path import basename

import docopt

from delphixpy.v1_10_2 import exceptions
from delphixpy.v1_10_2.web import database
from delphixpy.v1_10_2.web import environment
from delphixpy.v1_10_2.web import group
from delphixpy.v1_10_2.web import job
from delphixpy.v1_10_2.web import sourceconfig
from delphixpy.v1_10_2.web import vo
from lib import dlpx_exceptions
from lib import dsource_link_ase
from lib import dsource_link_mssql
from lib import dx_logging
from lib import get_references
from lib import get_session
from lib import run_job
from lib.run_async import run_async

VERSION = "v.0.3.0000"


def create_ora_sourceconfig(
    dlpx_obj,
    db_name,
    env_name,
    db_install_path,
    dx_group,
    db_user,
    ip_addr,
    dsource_name,
    port_num=1521,
):
    """
    Create the sourceconfig used for provisioning an Oracle dSource
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param db_name: Name of the database to ingest
    :type db_name: str
    :param env_name: Name of the environment in Delphix
    :type env_name: str
    :param db_install_path: Path where the Oracle binaries are installed
    :type db_install_path: str
    :param dx_group: Group name where the linked dSource will be
    :type dx_group: str
    :param db_user: Database username
    :type db_user: str
    :param dsource_name: Name of the dsource
    :type dsource_name: str
    :param ip_addr: IP Address of the Delphix environment
    :type ip_addr: str
    :param port_num: Port number of the Oracle Listener (1521 default)
    :type port_num: int
    """
    create_ret = None
    engine_name = dlpx_obj.dlpx_ddps["engine_name"]
    env_obj = get_references.find_obj_by_name(
        dlpx_obj.server_session, environment, env_name
    )
    port_num = str(port_num)
    try:
        sourceconfig_ref = get_references.find_obj_by_name(
            dlpx_obj.server_session, sourceconfig, db_name
        )
    except dlpx_exceptions.DlpxException:
        sourceconfig_ref = None
    repo_ref = get_references.find_db_repo(
        dlpx_obj.server_session, "OracleInstall", env_obj.reference, db_install_path
    )
    dsource_params = vo.OracleSIConfig()
    connect_str = f"jdbc:oracle:thin:@{ip_addr}:{port_num}:{db_name}"
    dsource_params.database_name = db_name
    dsource_params.unique_name = db_name
    dsource_params.repository = repo_ref
    dsource_params.instance = vo.OracleInstance()
    dsource_params.instance.instance_name = db_name
    dsource_params.instance.instance_number = 1
    dsource_params.services = vo.OracleService()
    dsource_params.jdbcConnectionString = connect_str
    try:
        if sourceconfig_ref is None:
            create_ret = sourceconfig.create(dlpx_obj.server_session, dsource_params)
            link_ora_dsource(
                dlpx_obj,
                create_ret,
                env_obj.primary_user,
                dx_group,
                db_user,
                dsource_name,
            )
        elif sourceconfig_ref is not None:
            create_ret = link_ora_dsource(
                dlpx_obj,
                sourceconfig_ref,
                env_obj.primary_user,
                dx_group,
                db_user,
                dsource_name,
            )
        dx_logging.print_info(
            f"Created and linked the dSource {db_name} with "
            f"reference {create_ret}.\n"
        )
        link_job_ref = dlpx_obj.server_session.last_job
        link_job_obj = job.get(dlpx_obj.server_session, link_job_ref)
        while link_job_obj.job_state not in ["CANCELED", "COMPLETED", "FAILED"]:
            dx_logging.print_info(
                "Waiting three seconds for link job to" "complete, and sync to begin"
            )
            time.sleep(3)
            link_job_obj = job.get(dlpx_obj.server_session, link_job_ref)
        # Add the snapsync job to the jobs dictionary
        dlpx_obj.jobs[engine_name] = dlpx_obj.server_session.last_job
        dlpx_obj.jobs[engine_name] = get_references.get_running_job(
            dlpx_obj.server_session,
            get_references.find_obj_by_name(
                dlpx_obj.server_session, database, dsource_name
            ).reference,
        )
    except (exceptions.HttpError, exceptions.RequestError) as err:
        raise dlpx_exceptions.DlpxException(
            f"ERROR: Could not create the" f"sourceconfig:\n{err}"
        )


def link_ora_dsource(
    dlpx_obj, srcconfig_ref, primary_user_ref, dx_group, db_user, dsource_name
):
    """
    Link the dSource in Delphix
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param srcconfig_ref: Reference to the sourceconfig object
    :type srcconfig_ref: str
    :param primary_user_ref: Reference to the environment user
    :type primary_user_ref: str
    :param dx_group: Group name of where to link the dsource
    :type dx_group: str
    :param db_user: Database username
    :type db_user: str
    :param dsource_name: Name of the dsource
    :type dsource_name: str
    :return: Reference of the linked dSource
    """
    link_params = vo.LinkParameters()
    link_params.link_data = vo.OracleLinkData()
    link_params.link_data.sourcing_policy = vo.OracleSourcingPolicy()
    link_params.name = dsource_name
    link_params.group = get_references.find_obj_by_name(
        dlpx_obj.server_session, group, dx_group
    ).reference
    link_params.link_data.compressedLinkingEnabled = True
    link_params.link_data.environment_user = primary_user_ref
    link_params.link_data.db_user = db_user
    link_params.link_data.number_of_connections = int(ARGUMENTS["--num_connections"])
    link_params.link_data.link_now = bool(ARGUMENTS["--link_now"])
    link_params.link_data.files_per_set = int(ARGUMENTS["--files_per_set"])
    link_params.link_data.rman_channels = int(ARGUMENTS["--rman_channels"])
    link_params.link_data.db_credentials = {
        "type": "PasswordCredential",
        "password": ARGUMENTS["--db_passwd"],
    }
    link_params.link_data.sourcing_policy.logsync_enabled = True
    link_params.link_data.config = srcconfig_ref
    try:
        return database.link(dlpx_obj.server_session, link_params)
    except (exceptions.RequestError, exceptions.HttpError) as err:
        raise dlpx_exceptions.DlpxException(
            f"Database link failed for " f"{dsource_name}:\n{err}\n"
        )


# def link_mssql_dsource(dlpx_obj, dsource_name, stage_env, stage_instance,
#                       dx_group, load_from_backup, sync_mode):
#    """
#    Link an MSSQL dSource
#    :param dlpx_obj: DDP session object
#    :type dlpx_obj: lib.GetSession.GetSession object
#    :param dsource_name: Name of the dsource
#    :type dsource_name: str
#    :param stage_env: Name of the staging environment
#    :type stage_env: str
#    :param stage_instance: Name if the staging database instance
#    :type stage_instance: str
#    :param dx_group: Group name of where the dSource will reside
#    :type dx_group: str
#    :param backup_path: Directory of where the backup is located
#    :type backup_path: str
#    :param backup_loc_passwd: Password of the shared backup path
#    :type backup_loc_passwd: str
#    :param load_from_backup: If set, Delphix will try to load the most recent
#                            full backup (MSSQL only)
#    :type load_from_backup: bool
#    :param sync_mode: MSSQL validated sync mode
#                            [TRANSACTION_LOG|FULL_OR_DIFFERENTIAL|FULL|NONE]
#    :type sync_mode: str
#    """
#    engine_name = dlpx_obj.dlpx_ddps['engine_name']
#    link_params = vo.LinkParameters()
#    link_params.name = dsource_name
#    link_params.link_data = vo.MSSqlLinkData()
#    try:
#        env_obj_ref = get_references.find_obj_by_name(
#            dlpx_obj.server_session, environment, stage_env).reference
#        link_params.link_data.ppt_repository = \
#            get_references.find_db_repo(
#                dlpx_obj.server_session, 'MSSqlInstance', env_obj_ref,
#                stage_instance).reference
#        link_params.link_data.config = get_references.find_obj_by_name(
#            dlpx_obj.server_session, sourceconfig, dsource_name).reference
#        link_params.group = get_references.find_obj_by_name(
#            dlpx_obj.server_session, group, dx_group).reference
#    except dlpx_exceptions.DlpxException as err:
#        raise dlpx_exceptions.DlpxException(
#            f'Could not link {dsource_name}:\n{err}')
#    if ARGUMENTS['--backup_path'] != "auto":
#        link_params.link_data.shared_backup_location = \
#            ARGUMENTS['--backup_path']
#    if ARGUMENTS['--backup_loc_passwd']:
#        link_params.link_data.backup_location_credentials = \
#            vo.PasswordCredential()
#        link_params.link_data.backup_location_credentials.password = \
#            ARGUMENTS['--backup_loc_passwd']
#        link_params.link_data.backup_location_user = \
#            ARGUMENTS['--backup_loc_user']
#    link_params.link_data.db_credentials = vo.PasswordCredential()
#    link_params.link_data.db_credentials.password = ARGUMENTS['--db_passwd']
#    link_params.link_data.db_user = ARGUMENTS['--db_user']
#    link_params.link_data.sourcing_policy = vo.SourcingPolicy()
# #   if load_from_backup:
# #        link_params.link_data.sourcing_policy.load_from_backup = True
# #    if sync_mode:
# #        link_params.link_data.validated_sync_mode = sync_mode
#    if ARGUMENTS['--logsync']:
#        link_params.link_data.sourcing_policy.logsync_enabled = True
#    elif not ARGUMENTS['--logsync']:
#        link_params.link_data.sourcing_policy.logsync_enabled = False
#    try:
#        database.link(dlpx_obj.server_session, link_params)
#        dlpx_obj.jobs[engine_name] = dlpx_obj.server_session.last_job
#        dlpx_obj.jobs[engine_name + 'snap'] = get_references.get_running_job(
#            dlpx_obj.server_session, get_references.find_obj_by_name(
#                dlpx_obj.server_session, database, dsource_name).reference)
#    except (exceptions.HttpError, exceptions.RequestError,
#            exceptions.JobError) as err:
#        dlpx_exceptions.DlpxException(f'Database link failed for '
#                                      f'{dsource_name}:\n{err}')


# def link_ase_dsource(dlpx_obj, dsource_name, env_name):
#    """
#    Link an ASE dSource
#    :param dlpx_obj: DDP session object
#    :type dlpx_obj: lib.GetSession.GetSession object
#    :param dsource_name: Name of the dsource
#    :type dsource_name: str
#    :param env_name: Name of the environment
#    :type env_name: str
#    """
#    engine_name = dlpx_obj.dlpx_ddps['engine_name']
#    link_params = vo.LinkParameters()
#    link_params.name = dsource_name
#    link_params.link_data = vo.ASELinkData()
#    link_params.link_data.db_credentials = vo.PasswordCredential()
#    link_params.link_data.db_credentials.password = ARGUMENTS['--ase_passwd']
#    link_params.link_data.db_user = ARGUMENTS['--ase_user']
#    link_params.link_data.load_backup_path = ARGUMENTS['--backup_path']
#    if ARGUMENTS['--bck_file']:
#        link_params.link_data.sync_parameters = \
#            vo.ASESpecificBackupSyncParameters()
#        bck_files = (ARGUMENTS['--bck_file']).split(' ')
#        link_params.link_data.sync_parameters.backup_files = bck_files
#    elif ARGUMENTS['--create_bckup']:
#        link_params.link_data.sync_parameters = vo.ASENewBackupSyncParameters()
#    else:
#        link_params.link_data.sync_parameters = \
#            vo.ASELatestBackupSyncParameters()
#    try:
#        link_params.group = get_references.find_obj_by_name(
#            dlpx_obj.server_session, group,
#            ARGUMENTS['--dx_group']).reference
#        env_user_ref = link_params.link_data.stage_user = \
#            get_references.find_obj_by_name(
#                dlpx_obj.server_session, environment, env_name).primary_user
#        link_params.link_data.staging_host_user = env_user_ref
#        link_params.link_data.source_host_user = env_user_ref
#        link_params.link_data.config = get_references.find_obj_by_name(
#            dlpx_obj.server_session, sourceconfig,
#            ARGUMENTS['--src_config']).reference
#        link_params.link_data.staging_repository = \
#            get_references.find_obj_by_name(dlpx_obj.server_session,
#                                            repository,
#                                            ARGUMENTS['--stage_repo']).reference
#    except dlpx_exceptions.DlpxException as err:
#        raise dlpx_exceptions.DlpxException(f'Could not link '
#                                            f'{dsource_name}: {err}\n')
#    try:
#        dsource_ref = database.link(dlpx_obj.server_session, link_params)
#        dlpx_obj.jobs[engine_name] = dlpx_obj.server_session.last_job
#        dlpx_obj.jobs[engine_name + 'snap'] = get_references.get_running_job(
#            dlpx_obj.server_session, get_references.find_obj_by_name(
#                dlpx_obj.server_session, database, dsource_name).reference)
#        print(f'{dsource_ref} sucessfully linked {dsource_name}')
#    except (exceptions.RequestError, exceptions.HttpError) as err:
#        raise dlpx_exceptions.DlpxException(f'Database link failed for '
#                                            f'{dsource_name}:\n{err}')
#


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
            f"ERROR: {basename(__file__)} encountered an error authenticating"
            f' to {engine["hostname"]} {ARGUMENTS["--target"]}:\n{err}'
        )
    thingstodo = ["thingstodo"]
    try:
        with dlpx_obj.job_mode(single_thread):
            while dlpx_obj.jobs or thingstodo:
                if thingstodo:
                    if ARGUMENTS["--type"].lower() == "oracle":
                        create_ora_sourceconfig(
                            dlpx_obj,
                            ARGUMENTS["--db_name"],
                            ARGUMENTS["--env_name"],
                            ARGUMENTS["--db_install_path"],
                            ARGUMENTS["--dx_group"],
                            ARGUMENTS["--dx_user"],
                            ARGUMENTS["--ip_addr"],
                            ARGUMENTS["--dsource_name"],
                        )
                    elif ARGUMENTS["--type"].lower() == "sybase":
                        ase_obj = dsource_link_ase.DsourceLinkASE(
                            dlpx_obj,
                            ARGUMENTS["--dsource_name"],
                            ARGUMENTS["--db_passwd"],
                            ARGUMENTS["--db_user"],
                            ARGUMENTS["--dx_group"],
                            ARGUMENTS["--logsync"],
                            ARGUMENTS["--type"],
                        )
                        ase_obj.link_ase_dsource(
                            ARGUMENTS["--backup_path"],
                            ARGUMENTS["--bck_file"],
                            ARGUMENTS["--create_backup"],
                            ARGUMENTS["--env_name"],
                            ARGUMENTS["--stage_repo"],
                        )
                    elif ARGUMENTS["--type"].lower() == "mssql":
                        mssql_obj = dsource_link_mssql.DsourceLinkMssql(
                            dlpx_obj,
                            ARGUMENTS["--dsource_name"],
                            ARGUMENTS["--db_passwd"],
                            ARGUMENTS["--db_user"],
                            ARGUMENTS["--dx_group"],
                            ARGUMENTS["--logsync"],
                            ARGUMENTS["--type"],
                        )
                        mssql_obj.link_mssql_dsource(
                            ARGUMENTS["--stage_env"],
                            ARGUMENTS["--stage_instance"],
                            ARGUMENTS["--backup_path"],
                            ARGUMENTS["--backup_loc_passwd"],
                            ARGUMENTS["--backup_loc_user"],
                        )
                    thingstodo.pop()
    except (
        dlpx_exceptions.DlpxException,
        exceptions.RequestError,
        exceptions.JobError,
        exceptions.HttpError,
    ) as err:
        dx_logging.print_exception(
            f'Error in {basename(__file__)}: {engine["hostname"]}\n{err}'
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
