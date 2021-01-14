#!/usr/bin/env python3
# Adam Bowen - Apr 2016
# This script refreshes a vdb
# Updated by Corey Brune Oct 2016
# requirements
# pip install --upgrade setuptools pip docopt delphixpy

# The below doc follows the POSIX compliant standards and allows us to use
# this doc to also define our ARGUMENTS for the script.
"""Refresh a vdb
Usage:
  dx_delete_vdb.py --vdb <name>
  [--engine <identifier>]
  [--group_name <groupname>][--force][--debug]
  [--poll <n>] [--single_thread <bool>]
  [--config <path_to_file>] [--logdir <path_to_file>]
  dx_delete_vdb.py -h | --help | -v | --version
  dx_delete_vdb.py -h | --help | -v | --version
Delete a Delphix VDB
Examples:
  dx_delete_vdb.py --vdb "aseTest"
  dx_delete_vdb.py --all_vdbs --group_name "Analytics" --all
Options:
  --vdb <name>              Name of the object you are refreshing.
  --all_vdbs                Refresh all VDBs that meet the filter criteria.
  --group_name <name>       Name of the group to execute against.
  --single_thread           Run as a single thread. False if running multiple
                            threads.
                            [default: False]
  --engine <name>           Alt Identifier of Delphix DDP in dxtools.conf.
                            [default: default]
  --debug                   Enable debug logging
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./dxtools.conf]
  --logdir <path_to_file>   The path to the logfile you want to use.
                            [default: ./dx_refresh_db.log]
  --force                   Force delete
  -h --help                 Show this screen.
  -v --version              Show version.
"""

from os.path import basename
import sys
import time
import docopt

from delphixpy.v1_10_2 import exceptions
from delphixpy.v1_10_2.web import database
from delphixpy.v1_10_2.web import vo

from lib import dlpx_exceptions
from lib import dx_timeflow
from lib import get_session
from lib import get_references
from lib import dx_logging
from lib import run_job
from lib.run_async import run_async

VERSION = 'v.0.3.001'

def delete_vdb(dlpx_obj, vdb_name,force_delete):
    dx_logging.print_info(f'Commence Delete')
    container_obj = get_references.find_obj_by_name( dlpx_obj.server_session, database, vdb_name)
    # Check to make sure our container object has a reference
    source_obj = get_references.find_source_by_db_name( dlpx_obj.server_session, vdb_name)
    if container_obj.reference:
        try:
            if source_obj.virtual is not True or source_obj.staging is True:
                raise dlpx_exceptions.DlpxException(f'ERROR: {container_obj.name} is not a virtual object\n')
            else:
                dx_logging.print_info(f'INFO: Deleting {container_obj.name} on engine {dlpx_obj.server_session.address}\n')
                delete_params = None
                if force_delete and str(container_obj.reference).startswith('MSSQL'):
                    delete_params = vo.DeleteParameters()
                    delete_params.force = True
                try:
                    database.delete(dlpx_obj.server_session, container_obj.reference,delete_params)
                    dlpx_obj.jobs[dlpx_obj.server_session.address] =  dlpx_obj.server_session.last_job
                except (dlpx_exceptions.DlpxException, exceptions.RequestError, exceptions.HttpError) as err:
                    raise dlpx_exceptions.DlpxException( f'{err}')
        # This exception is raised if refreshing a vFiles VDB since
        # AppDataContainer does not have virtual, staging or enabled attributes
        except AttributeError as err:
            dx_logging.print_exception(f'ERROR: Deleting {container_obj.name} on engine {dlpx_obj.server_session.address}\n')
            dx_logging.print_exception(f'AttributeError:{err.message}\n')
        except dlpx_exceptions.DlpxException as err:
            dx_logging.print_exception(f'DlpxException:{err}')
        except Exception as err:
            dx_logging.print_exception(f'Exception:\n{err}')
        dx_logging.print_info(f'Delete process complete')
        # return the last job id.
        return dlpx_obj.server_session.last_job

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
    # TODO: Implement multi-threading.
    try:
        # Setup the connection to the Delphix DDP
        dx_logging.print_info(f'Executing main_workflow')
        dlpx_obj.dlpx_session(engine['ip_address'], engine['username'], engine['password'])
    except dlpx_exceptions.DlpxException as err:
        dx_logging.print_exception(f'ERROR: dx_refresh_vdb encountered an error authenticating to '
            f'{engine["hostname"]} {ARGUMENTS["--target"]}:\n{err}\n')
    try:
        with dlpx_obj.job_mode(single_thread):
            job_id = delete_vdb( dlpx_obj, ARGUMENTS['--vdb'],ARGUMENTS['--force'])
            # locking threads
            run_job.find_job_state_by_jobid(engine, dlpx_obj,job_id)
    except (dlpx_exceptions.DlpxException, dlpx_exceptions.DlpxObjectNotFound,
            exceptions.RequestError, exceptions.JobError,
            exceptions.HttpError) as err:
        dx_logging.print_exception(f'Error in dx_delete_vdb on Delpihx Engine: {engine["ip_address"]}\n{err}')

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
        # This is the function that will handle processing main_workflow for all the servers.
        for each in run_job.run_job(main_workflow, dx_session_obj, engine, single_thread):
            # join them back together so that we wait for all threads to complete
            each.join()
            # waits for threads to finish.
        elapsed_minutes = run_job.time_elapsed(time_start)
        dx_logging.print_info(f'delete operation took {elapsed_minutes} minutes to complete.')
    # Here we handle what we do when the unexpected happens
    except SystemExit as err:
        # This is what we use to handle our sys.exit(#)
        sys.exit(err)

    except dlpx_exceptions.DlpxException as err:
        # We use this exception handler when an error occurs in a function
        # call.
        dx_logging.print_exception(f'ERROR: Please check the ERROR message below:\n {err.error}')
        sys.exit(2)

    except exceptions.HttpError as err:
        # We use this exception handler when our connection to Delphix fails
        dx_logging.print_exception(
            f'ERROR: Connection failed to the Delphix DDP. Please check the ERROR message below:\n{err.status}')
        sys.exit(2)

    except exceptions.JobError as err:
        # We use this exception handler when a job fails in Delphix so that we
        # have actionable data
        elapsed_minutes = run_job.time_elapsed(time_start)
        dx_logging.print_exception(
            f'A job failed in the Delphix Engine:\n{err.job}.'
            f'{basename(__file__)} took {elapsed_minutes} minutes to get this far')
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
