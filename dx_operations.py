#!/usr/bin/env python3
# Corey Brune - Oct 2016
# This script starts or stops a VDB
# requirements
# pip install docopt delphixpy

# The below doc follows the POSIX compliant standards and allows us to use
# this doc to also define our ARGUMENTS for the script.
"""List all VDBs or Start, stop, enable, disable a VDB
Usage:
  dx_operations_vdb.py (--vdb <name> [--stop | --start | --enable | \
  --disable] | --list | --all_dbs <name>)
  [--engine <identifier> | --all]
  [--force] [--debug] [--parallel <n>] [--poll <n>]
  [--config <path_to_file>] [--logdir <path_to_file>]
  dx_operations_vdb.py -h | --help | -v | --version
List all VDBs, start, stop, enable, disable a VDB

Examples:
  dx_operations_vdb.py --engine landsharkengine --vdb testvdb --stop
  dx_operations_vdb.py --vdb testvdb --start
  dx_operations_vdb.py --all_dbs enable
  dx_operations_vdb.py --all_dbs disable
  dx_operations_vdb.py --list

Options:
  --vdb <name>              Name of the VDB to stop or start
  --start                   Stop the VDB
  --stop                    Stop the VDB
  --all_dbs <name>          Enable or disable all dSources and VDBs
  --list                    List all databases from an engine
  --enable                  Enable the VDB
  --disable                 Disable the VDB
  -d <identifier>           Identifier of Delphix engine in dxtools.conf.
  --engine <type>           Alt Identifier of Delphix engine in dxtools.conf.
  --all                     Run against all engines.
  --force                   Do not clean up target in VDB disable operations
  --debug                   Enable debug logging
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./dxtools.conf]
  --logdir <path_to_file>    The path to the logfile you want to use.
                            [default: ./dx_operations_vdb.log]
  -h --help                 Show this screen.
  -v --version              Show version.
"""
import sys
from os.path import basename
import time
import docopt

from delphixpy.v1_10_2 import exceptions
from delphixpy.v1_10_2.web import database
from delphixpy.v1_10_2.web import source
from delphixpy.v1_10_2.web.capacity import consumer
from delphixpy.v1_10_2.web.vo import SourceDisableParameters

from lib import dlpx_exceptions
from lib import get_session
from lib import dx_logging
from lib import get_references
from lib import run_job
from lib.run_async import run_async


VERSION = 'v.0.3.000'


def dx_obj_operation(dlpx_obj, vdb_name, operation):
    """
    Function to start, stop, enable or disable a VDB
    :param dlpx_obj: Virtualization Engine session object
    :type dlpx_obj: lib.GetSession.GetSession
    :param vdb_name: Name of the object to stop/start/enable/disable
    :type vdb_name: str
    :param operation: enable or disable dSources and VDBs
    :type operation: str
    """
    engine_name = dlpx_obj.dlpx_engines.keys()[0]
    vdb_obj = get_references.find_obj_by_name(dlpx_obj.server_session,
                                              source, vdb_name)
    try:
        if vdb_obj:
            if operation == 'start':
                source.start(dlpx_obj.server_session, vdb_obj.reference)
            elif operation == 'stop':
                source.stop(dlpx_obj.server_session, vdb_obj.reference)
            elif operation == 'enable':
                source.enable(dlpx_obj.server_session, vdb_obj.reference)
            elif operation == 'disable':
                source.disable(dlpx_obj.server_session,
                               vdb_obj.reference)
            elif operation == 'force_disable':
                disable_params = SourceDisableParameters()
                disable_params.attempt_cleanup = False
                source.disable(dlpx_obj.server_session,
                               vdb_obj.reference,
                               disable_params)
            dlpx_obj.jobs[engine_name] = dlpx_obj.server_session.last_job
    except (exceptions.RequestError, exceptions.JobError, AttributeError) \
            as err:
        raise dlpx_exceptions.DlpxException(
            f'An error occurred while performing {operation} on {vdb_obj}:\n'
            f'{err}')
    print(f'{operation} was successfully performed on {vdb_name}.')


def all_databases(dlpx_obj, operation):
    """
    Enable or disable all dSources and VDBs on an engine
    :param dlpx_obj: Virtualization Engine session object
    :type dlpx_obj: lib.GetSession.GetSession
    :param operation: enable or disable dSources and VDBs
    :type operation: str
    """
    for db in database.get_all(dlpx_obj.server_session):
        try:
            dx_obj_operation(dlpx_obj, db.name, operation)
        except (exceptions.RequestError, exceptions.HttpError):
            pass
        time.sleep(2)


def list_databases(dlpx_obj):
    """
    Function to list all databases and stats for an engine
    :param dlpx_obj: Virtualization Engine session object
    :type dlpx_obj: lib.GetSession.GetSession
    """
    source_stats_lst = get_references.find_all_objects(dlpx_obj.server_session,
                                                       source)
    source_stats = None
    db_stats = None
    try:
        for db_stats in get_references.find_all_objects(
                dlpx_obj.server_session, consumer):
            source_stats = get_references.find_obj_list(source_stats_lst,
                                                        db_stats.name)
    except (exceptions.RequestError, AttributeError,
            dlpx_exceptions.DlpxException) as err:
        print(f'An error occurred while listing databases: {err}')
    if source_stats is not None:
        active_space = db_stats.breakdown.active_space / 1024 \
                       / 1024 / 1024
        sync_space = db_stats.breakdown.sync_space / 1024 / 1024 / 1024
        log_space = db_stats.breakdown.log_space / 1024 / 1024
        db_size = source_stats.runtime.database_size / 1024 / 1024 / 1024
        if source_stats.virtual is False:
            print(f'name: {db_stats.name}, provision container:'
                  f' {db_stats.parent}, disk usage: {db_size:.2f}GB,'
                  f'Size of Snapshots: {active_space:.2f}GB, '
                  f'dSource Size: {sync_space:.2f}GB, '
                  f'Log Size: {log_space:.2f}MB,'
                  f'Enabled: {source_stats.runtime.enabled},'
                  f'Status: {source_stats.runtime.status}')
        elif source_stats.virtual is True:
            print(f'name: {db_stats.name}, provision container: '
                  f'{db_stats.parent}, disk usage: '
                  f'{active_space:.2f}GB, Size of Snapshots: {sync_space:.2f}GB'
                  f'Log Size: {log_space:.2f}MB, Enabled: '
                  f'{source_stats.runtime.enabled}, '
                  f'Status: {source_stats.runtime.status}')
        elif source_stats is None:
            print(f'name: {db_stats.name},provision container: '
                  f'{db_stats.parent}, database disk usage: {db_size:.2f}GB,'
                  f'Size of Snapshots: {active_space:.2f}GB,'
                  'Could not find source information. This could be a '
                  'result of an unlinked object')


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
                    if ARGUMENTS['--start']:
                        dx_obj_operation(dlpx_obj, ARGUMENTS['--vdb'], 'start')
                    elif ARGUMENTS['--stop']:
                        dx_obj_operation(dlpx_obj, ARGUMENTS['--vdb'], 'stop')
                    elif ARGUMENTS['--enable']:
                        dx_obj_operation(dlpx_obj, ARGUMENTS['--vdb'], 'enable')
                    elif ARGUMENTS['--disable']:
                        if ARGUMENTS['--force']:
                            dx_obj_operation(
                                dlpx_obj, ARGUMENTS['--vdb'], 'force_disable')
                        else:
                            dx_obj_operation(
                                dlpx_obj, ARGUMENTS['--vdb'], 'disable')
                    elif ARGUMENTS['--list']:
                        list_databases(dlpx_obj)
                    elif ARGUMENTS['--all_dbs']:
                        all_databases(dlpx_obj, ARGUMENTS['--all_dbs'])
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
