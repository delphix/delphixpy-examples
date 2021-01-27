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
  dx_provision_dsource.py --type <name> --dsource_name <name> \
  --ip_addr <name> --db_name <name> --env_name <name> \
  --db_install_path <name> --dx_group <name> --db_passwd <name> \
  --db_user <name>
  [--port_num <name> --num_connections <name> --link_now <name>]
  [--files_per_set <name> --rman_channels <name>]
  [--engine <identifier>]
  [--parallel <n> --poll <n> --config <path_to_file> --logdir <path_to_file>]
  dx_provision_dsource.py --type <name> --dsource_name <name> \
  --ase_user <name> --ase_passwd <name> --backup_path <name> \
  --source_user <name> --stage_user aseadmin --stage_repo ASE1570_S2 \
  --src_config <name> --env_name <name> --dx_group <name>
  [--bck_file <name> --create_bckup]
  [--engine <identifier>]
  [--parallel <n> --poll <n> --config <path_to_file> --logdir <path_to_file>]
  dx_provision_dsource.py --type <name> --dsource_name <name> \
  --dx_group <name> --db_passwd <name> --db_user <name> \
  --stage_instance <name> --stage_env <name> --backup_path <name>
  [--backup_loc_passwd <passwd> --backup_loc_user <name> --logsync]
  [--sync_mode <mode> --load_from_backup --single_thread <bool>]
  [--engine <identifier>]
  [--parallel <n> --poll <n> --config <path_to_file> --logdir <path_to_file>]
  dx_provision_dsource.py -h | --help | -v | --version

Create and sync a dSource
Examples:
    Oracle:
    dx_provision_dsource.py --type oracle --dsource_name oradb1 \
    --ip_addr 192.168.166.11 --db_name srcDB1 --env_name SourceEnv \
    --db_install_path /u01/app/oracle/product/11.2.0.4/dbhome_1 \
    --db_user delphixdb --db_passwd delphixdb --single_thread False
    Sybase:
    dx_provision_dsource.py --type sybase --dsource_name dbw1 --ase_user sa \
    --ase_passwd sybase --backup_path /data/db --source_user aseadmin \
    --stage_user aseadmin --stage_repo ASE1570_S2 --src_config dbw1 \
    --env_name aseSource --dx_group Sources --single_thread False
    Specify backup files:
    dx_provision_dsource.py --type sybase --dsource_name dbw2 --ase_user sa \
    --ase_passwd sybase --backup_path /data/db --source_user aseadmin \
    --stage_user aseadmin --stage_repo ASE1570_S2 --src_config dbw2 \
    --env_name aseSource --dx_group Sources --bck_file "dbw2data.dat"
    Create a new backup and ingest:
    dx_provision_dsource.py --type sybase --dsource_name dbw2 --ase_user sa \
    --ase_passwd sybase --backup_path /data/db --source_user aseadmin \
    --stage_user aseadmin --stage_repo ASE1570_S2 --src_config dbw2 \
    --env_name aseSource --dx_group Sources --create_bckup
    MSSQL:
    dx_provision_dsource.py --type mssql  --dsource_name mssql_dsource \
    --dx_group Sources --db_passwd delphix --db_user sa \
    --stage_env mssql_target_svr --stage_instance MSSQLSERVER \
    --backup_path \\bckserver\path\backups --backup_loc_passwd delphix \
    --backup_loc_user delphix
    dx_provision_dsource.py --type mssql  --dsource_name AdventureWorks2014 \
    --dx_group "9 - Sources" --db_passwd delphixdb --db_user aw \
    --stage_env WINDOWSTARGET --stage_instance MSSQLSERVER \
    --backup_path auto --load_from_backup



Options:
  --type <name>             dSource type. mssql, sybase or oracle
                            [default: oracle]
  --ip_addr <name>          IP Address of the dSource
                            [default: None]
  --db_name <name>          Name of the dSource DB
  --env_name<name>          Name of the environment where the dSource installed
  --dx_group<name>          Group where the dSource will reside
  --db_install_path<name>   Location of the installation path of the DB.
  --num_connections <name>  Number of connections for Oracle RMAN
                            [default: 5]
  --single_thread           Run as a single thred. False if running multiple
                            threads.
                            [default: True]
  --link_now <name>         Link the dSource
                            [default: True]
  --files_per_set <name>    Configures how many files per set for Oracle RMAN
                            [default: 5]
  --rman_channels <name>    Configures the number of Oracle RMAN Channels
                            [default: 2]
  --create_bckup            Create and ingest a new Sybase backup
                            [default: False]
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
  --backup_loc_passwd <passwd>  Password of the shared backup path
                            [default: None]
  --backup_loc_user <nam>   User of the shared backup path
                            [default: None]
  --load_from_backup        If set, Delphix will try to load the most recent
                            full backup (MSSQL only)
                            [default: None]
  --dsource_name <name>     Name of the dSource
  --engine <type>           Alt Identifier of Delphix engine in dxtools.conf.
                            [default: default]
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./config/dxtools.conf]
  --logdir <path_to_file>    The path to the logfile you want to use.
                            [default: ./logs/dx_provision_dsource.log]
  -h --help                 Show this screen.
  -v --version              Show version.
"""
import sys
from os.path import basename
import time
import docopt

from delphixpy.v1_10_2 import exceptions

from lib import dlpx_exceptions
from lib import get_session
from lib import dx_logging
from lib import dsource_link_mssql
from lib import dsource_link_oracle
from lib import dsource_link_ase
from lib import run_job
from lib.run_async import run_async

VERSION = 'v.0.3.0002'


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
        dlpx_obj.dlpx_session(engine['ip_address'], engine['username'],
                              engine['password'])
    except dlpx_exceptions.DlpxException as err:
        dx_logging.print_exception(
            f'ERROR: {basename(__file__)} encountered an error authenticating'
            f' to {engine["hostname"]} {ARGUMENTS["--target"]}:\n{err}')
    thingstodo = ['thingstodo']
    try:
        with dlpx_obj.job_mode(single_thread):
            while dlpx_obj.jobs or thingstodo:
                if thingstodo:
                    if ARGUMENTS['--type'].lower() == 'oracle':
                        dsource_link_oracle.DsourceLinkOracle(
                            dlpx_obj, ARGUMENTS['--db_name'],
                            ARGUMENTS['--env_name'],
                            ARGUMENTS['--db_install_path'],
                            ARGUMENTS['--dx_group'], ARGUMENTS['--dx_user'],
                            ARGUMENTS['--ip_addr'], ARGUMENTS['--dsource_name']
                        )
                    elif ARGUMENTS['--type'].lower() == 'sybase':
                        ase_obj = dsource_link_ase.DsourceLinkASE(
                            dlpx_obj, ARGUMENTS['--dsource_name'],
                            ARGUMENTS['--db_passwd'], ARGUMENTS['--db_user'],
                            ARGUMENTS['--dx_group'], ARGUMENTS['--type']
                        )
                        ase_obj.link_ase_dsource(
                            ARGUMENTS['--backup_path'],
                            ARGUMENTS['--bck_file'],
                            ARGUMENTS['--create_backup'],
                            ARGUMENTS['--env_name'], ARGUMENTS['--stage_repo']
                        )
                    elif ARGUMENTS['--type'].lower() == 'mssql':
                        mssql_obj = dsource_link_mssql.DsourceLinkMssql(
                            dlpx_obj, ARGUMENTS['--dsource_name'],
                            ARGUMENTS['--db_passwd'], ARGUMENTS['--db_user'],
                            ARGUMENTS['--dx_group'],
                            ARGUMENTS['--type']
                        )
                        mssql_obj.link_mssql_dsource(
                            ARGUMENTS['--stage_env'],
                            ARGUMENTS['--stage_instance'],
                            ARGUMENTS['--backup_path'],
                            ARGUMENTS['--backup_loc_passwd'],
                            ARGUMENTS['--backup_loc_user']
                        )
                    thingstodo.pop()
    except (dlpx_exceptions.DlpxException, exceptions.RequestError,
            exceptions.JobError, exceptions.HttpError) as err:
        dx_logging.print_exception(
            f'Error in {basename(__file__)}: {engine["hostname"]}\n{err}')
    run_job.find_job_state(engine, dlpx_obj)


def main():
    """
    main function - creates session and runs jobs
    """
    time_start = time.time()
    try:
        dx_session_obj = get_session.GetSession()
        dx_logging.logging_est(ARGUMENTS['--logdir'])
        config_file_path = ARGUMENTS['--config']
        single_thread = ARGUMENTS['--single_thread']
        engine = ARGUMENTS['--engine']
        dx_session_obj.get_config(config_file_path)
        # This is the function that will handle processing main_workflow for
        # all the servers.
        for each in run_job.run_job(main_workflow, dx_session_obj, engine,
                                    single_thread):
            # join them back together so that we wait for all threads to
            # complete
            each.join()
        elapsed_minutes = run_job.time_elapsed(time_start)
        dx_logging.print_info(f'script took {elapsed_minutes} minutes to '
                              f'get this far.')
    # Here we handle what we do when the unexpected happens
    except SystemExit as err:
        # This is what we use to handle our sys.exit(#)
        sys.exit(err)

    except dlpx_exceptions.DlpxException as err:
        # We use this exception handler when an error occurs in a function
        # call.
        dx_logging.print_exception(f'ERROR: Please check the ERROR message '
                                   f'below:\n {err.error}')
        sys.exit(2)

    except exceptions.HttpError as err:
        # We use this exception handler when our connection to Delphix fails
        dx_logging.print_exception(
            f'ERROR: Connection failed to the Delphix DDP. Please check '
            f'the ERROR message below:\n{err.status}')
        sys.exit(2)

    except exceptions.JobError as err:
        # We use this exception handler when a job fails in Delphix so that we
        # have actionable data
        elapsed_minutes = run_job.time_elapsed(time_start)
        dx_logging.print_exception(
            f'A job failed in the Delphix Engine:\n{err.job}.'
            f'{basename(__file__)} took {elapsed_minutes} minutes to get '
            f'this far')
        sys.exit(3)

    except KeyboardInterrupt:
        # We use this exception handler to gracefully handle ctrl+c exits
        dx_logging.print_debug('You sent a CTRL+C to interrupt the process')
        elapsed_minutes = run_job.time_elapsed(time_start)
        dx_logging.print_info(f'{basename(__file__)} took {elapsed_minutes} '
                              f'minutes to get this far.')


if __name__ == "__main__":
    # Grab our ARGUMENTS from the doc at the top of the script
    ARGUMENTS = docopt.docopt(__doc__,
                              version=basename(__file__) + " " + VERSION)
    # Feed our ARGUMENTS to the main function, and off we go!
    main()
