#!/usr/bin/env python3
# Adam Bowen - Apr 2016
# This script provisions a vdb or dSource
# Updated by Corey Brune Aug 2016
# --- Create vFiles VDB
# requirements
# pip install docopt delphixpy

# The below doc follows the POSIX compliant standards and allows us to use
# this doc to also define our ARGUMENTS for the script.

"""Provision VDB's
Usage:
  dx_provision_db.py --source <name> --target_grp <name> --db <name> \
  --env_name <name> --type <type>
  [--envinst <name> --vfiles_path <path> --timestamp_type <type>]
  [--timestamp <timepoint_semantic> --timeflow <name> --no_truncate_log]
  [--instname <sid> --mntpoint <path> --uniqname <name> --engine <identifier>]
  [--single_thread <bool> --vdb_restart <bool> --parallel <n> --poll <n>]
  [--config <path_to_file> --logdir <path_to_file> --postrefresh <name>]
  [--prerefresh <name> --configure-clone <name> --prerollback <name>]
  [--postrollback <name> --logsync]
  dx_provision_db.py --help | --version
Provision VDB from a defined source on the defined target environment.

Examples:
  dx_provision_vdb.py --engine landsharkengine --logsync \
  --source "ASE pubs3 DB" --db vase --target_grp Analytics \
  --env_name LINUXTARGET --type ase --envinst "LINUXTARGET"

  dx_provision_vdb.py --source "Employee Oracle 11G DB" \
  --instname autod --uniqname autoprod --db autoprod --target autoprod \
  --target_grp Analytics --env_name LINUXTARGET --type oracle \
  --envinst "/u01/app/oracle/product/11.2.0/dbhome_1"

  dx_provision_vdb.py --source "AdventureWorksLT2008R2" \
  --db vAW --target_grp Analytics --env_name WINDOWSTARGET \
  --type mssql --envinst MSSQLSERVER --all

  dx_provision_vdb.py --source UF_Source --db appDataVDB \
  --target_grp Untitled --env_name LinuxTarget --type vfiles \
  --vfiles_path /mnt/provision/appDataVDB \
  --prerollback "/u01/app/oracle/product/scripts/PreRollback.sh" \
  --postrollback "/u01/app/oracle/product/scripts/PostRollback.sh" \
  --vdb_restart true

Options:
  --source <name>           Name of the source object
  --target_grp <name>       The group into which Delphix will place the VDB.
  --db <name>               The name you want to give the database (Oracle Only)
  --vfiles_path <path>      The full path on the Target server where Delphix
                            will provision the vFiles
  --no_truncate_log         Don't truncate log on checkpoint (ASE only)
  --env_name <name>         The name of the Target environment in Delphix
  --type <type>             The type of VDB this is.
                            oracle | mssql | ase | vfiles
  --logsync                 Enable logsync
  --prerefresh <name>       Pre-Hook commands before a refresh
  --postrefresh <name>      Post-Hook commands after a refresh
  --prerollback <name>      Post-Hook commands before a rollback
  --postrollback <name>     Post-Hook commands after a rollback
  --configure-clone <name>  Configure Clone commands
  --vdb_restart <bool>      Either True or False. Default: False
  --envinst <name>          The identifier of the instance in Delphix.
                            ex. "/u01/app/oracle/product/11.2.0/dbhome_1"
                            ex. LINUXTARGET
  --timeflow <name>         Name of the timeflow from which you are provisioning
  --timestamp_type <type>   The type of timestamp you are specifying.
                            Acceptable Values: TIME, SNAPSHOT
                            [default: SNAPSHOT]
  --timestamp <timepoint_semantic>
                            The Delphix semantic for the point in time from
                            which you want to provision your VDB.
                            Formats:
                            latest point in time or snapshot: LATEST
                            point in time: "YYYY-MM-DD HH24:MI:SS"
                            snapshot name: "@YYYY-MM-DDTHH24:MI:SS.ZZZ"
                            snapshot time from GUI: "YYYY-MM-DD HH24:MI"
                            [default: LATEST]
  --instname <sid>          Target VDB SID name (Oracle Only)
  --uniqname <name>         Target VDB db_unique_name (Oracle Only)
  --mntpoint <path>         Mount point for the VDB
                            [default: /mnt/provision]
  --engine <type>           Identifier of Delphix engine in dxtools.conf.
                            [default: default]
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 5]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./config/dxtools.conf]
  --logdir <path_to_file>    The path to the logfile you want to use.
                            [default: ./logs/dx_provision_vdb.log]
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
from delphixpy.v1_10_2.web import repository
from delphixpy.v1_10_2.web import vo

from lib import dlpx_exceptions
from lib import get_references
from lib import get_session
from lib import dx_logging
from lib import run_job
from lib import dx_timeflow
from lib.run_async import run_async

VERSION = 'v.0.3.005'


def create_ase_vdb(dlpx_obj, vdb_group, vdb_name, environment_obj,
                   source_obj, env_inst, timestamp, timestamp_type='SNAPSHOT',
                   no_truncate_log=False):
    """
    Create a Sybase ASE VDB
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param vdb_group: Group name where the VDB will be created
    :type vdb_group: str
    :param vdb_name: Name of the VDB
    :type vdb_name: str
    :param environment_obj: Environment object where the VDB will be created
    :type environment_obj: class 'delphixpy.v1_10_2.web.objects
    :param source_obj: Database object of the source
    :type source_obj: class
    delphixpy.v1_10_2.web.objects.UnixHostEnvironment.UnixHostEnvironment
    :param env_inst: Environment installation identifier in Delphix.
    EX: "/u01/app/oracle/product/11.2.0/dbhome_1"
    EX: ASETARGET
    :type env_inst: str
    :param timestamp: The Delphix semantic for the point in time on the
        source from which to refresh the VDB
    :type timestamp: str
    :param timestamp_type: The Delphix semantic for the point in time on
    the source from which you want to refresh your VDB either SNAPSHOT or TIME
    :type timestamp_type: str
    :param no_truncate_log: Don't truncate log on checkpoint
    :type no_truncate_log: bool
    :return:
    """
    engine_name = list(dlpx_obj.dlpx_ddps)[0]
    dx_timeflow_obj = dx_timeflow.DxTimeflow(dlpx_obj.server_session)
    try:
        vdb_obj = get_references.find_obj_by_name(
            dlpx_obj.server_session, database, vdb_name)
        raise dlpx_exceptions.DlpxObjectExists(f'{vdb_obj} exists.')
    except dlpx_exceptions.DlpxObjectNotFound:
        pass
    vdb_group_ref = get_references.find_obj_by_name(
        dlpx_obj.server_session, group, vdb_group)
    vdb_params = vo.ASEProvisionParameters()
    vdb_params.container = vo.ASEDBContainer()
    if no_truncate_log:
        vdb_params.truncate_log_on_checkpoint = False
    else:
        vdb_params.truncate_log_on_checkpoint = True
    vdb_params.container.group = vdb_group_ref
    vdb_params.container.name = vdb_name
    vdb_params.source = vo.ASEVirtualSource()
    vdb_params.source_config = vo.ASESIConfig()
    vdb_params.source_config.database_name = vdb_name
    vdb_params.source_config.repository = get_references.find_obj_by_name(
                    dlpx_obj.server_session, repository, env_inst).reference
    vdb_params.timeflow_point_parameters = dx_timeflow_obj.set_timeflow_point(
        source_obj, timestamp_type, timestamp)
    vdb_params.timeflow_point_parameters.container = source_obj.reference
    dx_logging.print_info(f'{engine_name} provisioning {vdb_name}')
    database.provision(dlpx_obj.server_session, vdb_params)
    # Add the job into the jobs dictionary so we can track its progress
    dlpx_obj.jobs[
        dlpx_obj.server_session.address
    ] = dlpx_obj.server_session.last_job


def create_mssql_vdb(dlpx_obj, group_ref, vdb_name,
                     environment_obj, source_obj, env_inst, timestamp,
                     timestamp_type='SNAPSHOT'
                     ):
    """
    Create a MSSQL VDB
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param group_ref: Reference of Group name where the VDB will be created
    :type group_ref: str
    :param vdb_name: Name of the VDB
    :type vdb_name: str
    :param environment_obj: Environment object where the VDB will be created
    :type environment_obj: class 'delphixpy.v1_10_2.web.objects
    :param source_obj: Database object of the source
    :type source_obj:
    :param env_inst: Environment installation identifier in Delphix.
    EX: "/u01/app/oracle/product/11.2.0/dbhome_1"
    EX: ASETARGET
    :type env_inst: str
    :param timestamp: The Delphix semantic for the point in time on the
    source from which to refresh the VDB
    :type timestamp: str
    :param timestamp_type: The Delphix semantic for the point in time on
    the source from which you want to refresh your VDB either SNAPSHOT or TIME
    :type timestamp_type: str
    :return:
    """
    engine_name = list(dlpx_obj.dlpx_ddps)[0]
    timeflow_obj = dx_timeflow.DxTimeflow(dlpx_obj.server_session)
    try:
        vdb_obj = get_references.find_obj_by_name(
            dlpx_obj.server_session, database, vdb_name)
        raise dlpx_exceptions.DlpxObjectExists(f'{vdb_obj} exists.')
    except dlpx_exceptions.DlpxObjectNotFound:
        pass
    vdb_params = vo.MSSqlProvisionParameters()
    vdb_params.container = vo.MSSqlDatabaseContainer()
    vdb_params.container.group = group_ref
    vdb_params.container.name = vdb_name
    vdb_params.source = vo.MSSqlVirtualSource()
    vdb_params.source.allow_auto_vdb_restart_on_host_reboot = False
    vdb_params.source_config = vo.MSSqlSIConfig()
    vdb_params.source_config.database_name = vdb_name
    vdb_params.source_config.environment_user = \
        environment_obj.primary_user
    vdb_params.source_config.repository = get_references.find_obj_by_name(
        dlpx_obj.server_session, repository, env_inst).reference
    vdb_params.timeflow_point_parameters = \
        timeflow_obj.set_timeflow_point(source_obj, timestamp_type,
                                        timestamp)
    vdb_params.timeflow_point_parameters.container = source_obj.reference
    dx_logging.print_info(f'{engine_name} provisioning {vdb_name}')
    print(type(source_obj), type(environment_obj))
    database.provision(dlpx_obj.server_session, vdb_params)
    # Add the job into the jobs dictionary so we can track its progress
    dlpx_obj.jobs[
        dlpx_obj.server_session.address
    ] = dlpx_obj.server_session.last_job


def create_vfiles_vdb(dlpx_obj, group_ref, vfiles_name,
                      environment_obj, source_obj, env_inst, timestamp,
                      timestamp_type='SNAPSHOT', pre_refresh=None,
                      post_refresh=None, pre_rollback=None, 
                      post_rollback=None, configure_clone=None):
    """
    Create a vfiles VDB
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param group_ref: Reference of group name where the VDB will be created
    :type group_ref: str
    :param vfiles_name: Name of the vfiles VDB
    :type vfiles_name: str
    :param environment_obj: Environment object where the VDB will be created
    :type environment_obj: class 'delphixpy.v1_10_2.web.objects
    :param source_obj: vfiles object of the source
    :type source_obj: class
    delphixpy.v1_10_2.web.objects.OracleDatabaseContainer.OracleDatabaseContainer
    :param env_inst: Environment installation identifier in Delphix.
    EX: "/u01/app/oracle/product/11.2.0/dbhome_1"
    EX: ASETARGET
    :type env_inst: str
    :param timestamp: The Delphix semantic for the point in time on the
    source from which to refresh the VDB
    :type timestamp: str
    :param timestamp_type: The Delphix semantic for the point in time on
    the source from which you want to refresh your VDB either SNAPSHOT or TIME
    :type timestamp_type: str
    :param pre_refresh: Pre-Hook commands before a refresh
    :type pre_refresh: str
    :param post_refresh: Post-Hook commands after a refresh
    :type post_refresh: str
    :param pre_rollback: Commands before a rollback
    :type pre_rollback: str
    :param post_rollback: Commands after a rollback
    :type post_rollback: str
    :param configure_clone: Configure clone commands
    :type configure_clone: str
    """
    engine_name = list(dlpx_obj.dlpx_ddps)[0]
    timeflow_obj = dx_timeflow.DxTimeflow(dlpx_obj.server_session)
    try:
        vdb_obj = get_references.find_obj_by_name(
            dlpx_obj.server_session, database, vfiles_name)
        raise dlpx_exceptions.DlpxObjectExists(f'{vdb_obj} exists.')
    except dlpx_exceptions.DlpxObjectNotFound:
        pass
    vfiles_params = vo.AppDataProvisionParameters()
    vfiles_params.source = vo.AppDataVirtualSource()
    vfiles_params.source_config = vo.AppDataDirectSourceConfig()
    vfiles_params.source.allow_auto_vdb_restart_on_host_reboot = True
    vfiles_params.container = vo.AppDataContainer()
    vfiles_params.group = group_ref
    vfiles_params.name = vfiles_name
    vfiles_params.source_config.name = ARGUMENTS['--target']
    vfiles_params.source_config.path = ARGUMENTS['--vfiles_path']
    vfiles_params.source_config.environment_user = environment_obj.primary_user
    vfiles_params.source_config.repository = get_references.find_obj_by_name(
        dlpx_obj.server_session, repository, env_inst).reference
    vfiles_params.source.name = vfiles_name
    vfiles_params.source.name = vfiles_name
    vfiles_params.source.operations = vo.VirtualSourceOperations()
    if pre_refresh:
        vfiles_params.source.operations.pre_refresh = \
            vo.RunCommandOnSourceOperation()
        vfiles_params.source.operations.pre_refresh.command = pre_refresh
    if post_refresh:
        vfiles_params.source.operations.post_refresh = \
            vo.RunCommandOnSourceOperation()
        vfiles_params.source.operations.post_refresh.command = post_refresh
    if pre_rollback:
        vfiles_params.source.operations.pre_rollback = \
            vo.RunCommandOnSourceOperation
        vfiles_params.source.operations.pre_rollback.command = pre_rollback
    if post_rollback:
        vfiles_params.source.operations.post_rollback = \
            vo.RunCommandOnSourceOperation()
        vfiles_params.source.operations.post_rollback.command = post_rollback
    if configure_clone:
        vfiles_params.source.operations.configure_clone = \
            vo.RunCommandOnSourceOperation()
        vfiles_params.source.operations.configure_clone.command = \
            configure_clone
    if timestamp_type is None:
        vfiles_params.timeflow_point_parameters = vo.TimeflowPointSemantic()
        vfiles_params.timeflow_point_parameters.container = \
            source_obj.reference,
        vfiles_params.timeflow_point_parameters.location = 'LATEST_POINT'
    elif timestamp_type.upper() == 'SNAPSHOT':
        try:
            dx_snap_params = timeflow_obj.set_timeflow_point(
                source_obj, timestamp_type, timestamp)
        except exceptions.RequestError as err:
            raise dlpx_exceptions.DlpxException(
                f'Could not set the timeflow point:\n{err}')
        if dx_snap_params.type == 'TimeflowPointSemantic':
            vfiles_params.timeflow_point_parameters = vo.TimeflowPointSemantic()
            vfiles_params.timeflow_point_parameters.container = \
                dx_snap_params.container
            vfiles_params.timeflow_point_parameters.location = \
                dx_snap_params.location
        elif dx_snap_params.type == 'TimeflowPointTimestamp':
            vfiles_params.timeflow_point_parameters = \
                vo.TimeflowPointTimestamp()
            vfiles_params.timeflow_point_parameters.timeflow = \
                dx_snap_params.timeflow
            vfiles_params.timeflow_point_parameters.timestamp = \
                dx_snap_params.timestamp
    dx_logging.print_info(f'{engine_name}: Provisioning {vfiles_name}\n')
    try:
        database.provision(dlpx_obj.server_session, vfiles_params)
    except (exceptions.RequestError, exceptions.HttpError) as err:
        raise dlpx_exceptions.DlpxException(
            f'ERROR: Could not provision the database {vfiles_name}\n{err}')
    # Add the job into the jobs dictionary so we can track its progress
    dlpx_obj.jobs[
        dlpx_obj.server_session.address
    ] = dlpx_obj.server_session.last_job


def create_oracle_si_vdb(dlpx_obj, group_ref, vdb_name,
                         environment_obj, source_obj, env_inst, timestamp,
                         timestamp_type='SNAPSHOT', pre_refresh=None,
                         post_refresh=None, pre_rollback=None,
                         post_rollback=None, configure_clone=None):
    """
    Create an Oracle SI VDB
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param group_ref: Group name where the VDB will be created
    :type group_ref: str
    :param vdb_name: Name of the VDB
    :type vdb_name: str
    :param source_obj: Database object of the source
    :type source_obj: class
    delphixpy.v1_10_2.web.objects.OracleDatabaseContainer.OracleDatabaseContainer
    :param environment_obj: Environment object where the VDB will be created
    :type environment_obj: class 'delphixpy.v1_10_2.web.objects
    :param env_inst: Environment installation identifier in Delphix.
    EX: "/u01/app/oracle/product/11.2.0/dbhome_1"
    EX: ASETARGET
    :type env_inst: str
    :param timestamp: The Delphix semantic for the point in time on the
    source from which to refresh the VDB
    :type timestamp: str
    :param timestamp_type: The Delphix semantic for the point in time on
    the source from which you want to refresh your VDB either SNAPSHOT or TIME
    :type timestamp_type: str
    """
    engine_name = list(dlpx_obj.dlpx_ddps)[0]
    try:
        vdb_obj = get_references.find_obj_by_name(
            dlpx_obj.server_session, database, vdb_name)
        raise dlpx_exceptions.DlpxObjectExists(f'{vdb_obj} exists.')
    except dlpx_exceptions.DlpxObjectNotFound:
        pass
    vdb_params = vo.OracleProvisionParameters()
    vdb_params.open_resetlogs = True
    vdb_params.container = vo.OracleDatabaseContainer()
    vdb_params.container.group = group_ref
    vdb_params.container.name = vdb_name
    vdb_params.source = vo.OracleVirtualSource()
    vdb_params.source.allow_auto_vdb_restart_on_host_reboot = False
    vdb_params.source.mount_base = ARGUMENTS['--mntpoint']
    vdb_params.source_config = vo.OracleSIConfig()
    vdb_params.source_config.environment_user = \
        environment_obj.primary_user
    vdb_params.source.operations = vo.VirtualSourceOperations()
    if pre_refresh:
        vdb_params.source.operations.pre_refresh = \
            vo.RunCommandOnSourceOperation()
        vdb_params.source.operations.pre_refresh.command = pre_refresh
    if post_refresh:
        vdb_params.source.operations.post_refresh = \
            vo.RunCommandOnSourceOperation()
        vdb_params.source.operations.post_refresh.command = post_refresh
    if pre_rollback:
        vdb_params.source.operations.pre_rollback = \
            vo.RunCommandOnSourceOperation
        vdb_params.source.operations.pre_rollback.command = pre_rollback
    if post_rollback:
        vdb_params.source.operations.post_rollback = \
            vo.RunCommandOnSourceOperation()
        vdb_params.source.operations.post_rollback.command = post_rollback
    if configure_clone:
        vdb_params.source.operations.configure_clone = \
            vo.RunCommandOnSourceOperation()
        vdb_params.source.operations.configure_clone.command = \
            configure_clone
    vdb_params.source_config.database_name = vdb_name
    vdb_params.source_config.unique_name = vdb_name
    vdb_params.source_config.instance = vo.OracleInstance()
    vdb_params.source_config.instance.instance_name = vdb_name
    vdb_params.source_config.instance.instance_number = 1
    vdb_params.source_config.repository = get_references.find_db_repo(
        dlpx_obj.server_session, 'OracleInstall', environment_obj.reference,
        env_inst
    )
    timeflow_obj = dx_timeflow.DxTimeflow(dlpx_obj.server_session)
    vdb_params.timeflow_point_parameters = \
        timeflow_obj.set_timeflow_point(source_obj, timestamp_type,
                                        timestamp)
    dx_logging.print_info(f'{engine_name}: Provisioning {vdb_name}')
    try:
        database.provision(dlpx_obj.server_session, vdb_params)
    except (exceptions.RequestError, exceptions.HttpError) as err:
        raise dlpx_exceptions.DlpxException(
            f'ERROR: Could not provision the database {vdb_name}\n{err}')
    # Add the job into the jobs dictionary so we can track its progress
    dlpx_obj.jobs[
        dlpx_obj.server_session.address
    ] = dlpx_obj.server_session.last_job


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
            engine['ip_address'], engine['username'], engine['password'])
    except dlpx_exceptions.DlpxException as err:
        dx_logging.print_exception(
            f'ERROR: dx_provision_vdb encountered an error authenticating to '
            f' {engine["ip_address"]} :\n{err}')
    group_ref = get_references.find_obj_by_name(
        dlpx_obj.server_session, group, ARGUMENTS['--target_grp']).reference
    environment_obj = get_references.find_obj_by_name(
        dlpx_obj.server_session, environment, ARGUMENTS['--env_name'])
    source_obj = get_references.find_obj_by_name(
        dlpx_obj.server_session, database, ARGUMENTS['--source'])
    thingstodo = ["thingstodo"]
    try:
        with dlpx_obj.job_mode(single_thread):
            while dlpx_obj.jobs or thingstodo:
                if thingstodo:
                    arg_type = ARGUMENTS['--type'].lower()
                    if arg_type == "oracle":
                        create_oracle_si_vdb(
                            dlpx_obj, group_ref, ARGUMENTS['--db'],
                            environment_obj, source_obj,
                            ARGUMENTS['--envinst'], ARGUMENTS['--timestamp'],
                            ARGUMENTS['--timestamp_type'],
                            ARGUMENTS['--prerefresh'],
                            ARGUMENTS['--postrefresh'],
                            ARGUMENTS['--prerollback'],
                            ARGUMENTS['--postrollback'],
                            ARGUMENTS['--configure-clone']
                        )
                    elif arg_type == "ase":
                        create_ase_vdb(dlpx_obj, group_ref, ARGUMENTS['--db'],
                                       environment_obj, source_obj,
                                       ARGUMENTS['--envinst'],
                                       ARGUMENTS['--timestamp'],
                                       ARGUMENTS['--timestamp_type'],
                                       ARGUMENTS['--no_truncate_log']
                                       )
                    elif arg_type == "mssql":
                        create_mssql_vdb(
                            dlpx_obj, group_ref, ARGUMENTS['--db'],
                            environment_obj, source_obj,
                            ARGUMENTS['--envinst'], ARGUMENTS['--timestamp'],
                            ARGUMENTS['--timestamp_type']
                        )
                    elif arg_type == "vfiles":
                        create_vfiles_vdb(
                            dlpx_obj, group_ref, ARGUMENTS['--db'],
                            environment_obj, source_obj,
                            ARGUMENTS['--prerefresh'],
                            ARGUMENTS['--postrefresh'],
                            ARGUMENTS['--prerollback'],
                            ARGUMENTS['--postrollback'],
                            ARGUMENTS['--configure-clone']
                        )
                    thingstodo.pop()
                run_job.find_job_state(engine, dlpx_obj)
    except (
        dlpx_exceptions.DlpxException,
        dlpx_exceptions.DlpxObjectNotFound,
        exceptions.RequestError,
        exceptions.JobError,
        exceptions.HttpError,
    ) as err:
        dx_logging.print_exception(f'Error in dx_rewind_vdb: '
                                   f'{engine["ip_address"]}\n{err}')


def main():
    """
    main function - creates session and runs jobs
    """
    time_start = time.time()
    dx_session_obj = get_session.GetSession()
    dx_logging.logging_est(ARGUMENTS['--logdir'])
    config_file_path = ARGUMENTS['--config']
    single_thread = ARGUMENTS['--single_thread']
    engine = ARGUMENTS['--engine']
    try:
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

    except KeyError as err:
        dx_logging.print_exception(f'ERROR: Key not found:\n{err}')
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
