"""
This does stuff
"""
import time

from delphixpy.v1_10_2 import exceptions
from delphixpy.v1_10_2.web import job

from lib import dx_logging
from lib import dlpx_exceptions


def run_job(main_func, dx_obj, engine='default', single_thread=True):
    """
    This method runs the main_func asynchronously against all the
    servers specified
    :param main_func: function to run against the DDP(s).
     In these examples, it's main_workflow().
    :type main_func: function
    :param dx_obj: Delphix session object from config
    :type dx_obj: lib.get_session.GetSession object
    :param engine: name of an engine, all or None
    :type engine: str
    :param single_thread: Run as single thread (True) or
                    multiple threads (False)
    :type single_thread: bool
    """
    # If "all" argument was given, run against every engine in config_file
    if engine == 'all':
        dx_logging.print_info(f'Executing against all Delphix DDPs')
        try:
            for delphix_ddp in dx_obj.dlpx_ddps:
                yield main_func(dx_obj.dlpx_ddps[delphix_ddp], dx_obj, single_thread)
        except dlpx_exceptions.DlpxException as err:
            dx_logging.print_exception(f'Error encountered in run_job():\n{err}')
    elif engine == 'default':
        try:
            for delphix_ddp in dx_obj.dlpx_ddps.keys():
                if dx_obj.dlpx_ddps[delphix_ddp]['default'] == 'True':
                    dx_obj_default = dx_obj
                    dx_obj_default.dlpx_ddps = {
                        delphix_ddp: dx_obj.dlpx_ddps[delphix_ddp]}
                    dx_logging.print_info(f'Executing against default DDP')
                    yield main_func(dx_obj.dlpx_ddps[delphix_ddp], dx_obj, single_thread)
                break
        except TypeError as err:
            raise dlpx_exceptions.DlpxException(f'Error in run_job: {err}')
    else:
        # Test to see if the engine exists in config_file
        try:
            yield main_func(dx_obj.dlpx_ddps[engine], dx_obj, single_thread)
            dx_logging.print_info(f'Executing against Delphix DDP: '
                                  f'{dx_obj.dlpx_ddps[engine]}')
        except (exceptions.RequestError, KeyError):
            raise dlpx_exceptions.DlpxException(
                f'\nERROR: Delphix DDP {engine} cannot be found. Please '
                f'check your value and try again.')
    if engine is None:
        raise dlpx_exceptions.DlpxException(f'ERROR: No default Delphix '
                                            f'DDP found.')


def run_job_mt(main_func, dx_obj, engine='default', single_thread=True):
    """
    This method runs the main_func asynchronously against all the
    delphix engines specified
    :param main_func: function to run against the DDP(s).
     In these examples, it's main_workflow().
    :type main_func: function
    :param dx_obj: Delphix session object from config
    :type dx_obj: lib.get_session.GetSession object
    :param engine: name of an engine, all or None
    :type engine: str
    :param single_thread: Run as single thread (True) or
                    multiple threads (False)
    :type single_thread: bool
    """
    threads = []
    # if engine ="all", run against every engine in config_file
    if engine == 'all':
        dx_logging.print_info(f'Executing against all Delphix DDPs')
        try:
            for delphix_ddp in dx_obj.dlpx_ddps:
                t = main_func(dx_obj.dlpx_ddps[delphix_ddp], dx_obj, single_thread)
                threads.append(t)
                # TODO: Revisit threading logic
                # This sleep has been tactically added to prevent errors in the parallel
                # processing of operations across multiple engines
                time.sleep(5)
        except dlpx_exceptions.DlpxException as err:
            dx_logging.print_exception(f'Error encountered in run_job():\n{err}')
    elif engine == 'default':
        try:
            for delphix_ddp in dx_obj.dlpx_ddps.keys():
                if dx_obj.dlpx_ddps[delphix_ddp]['default'] == 'True':
                    dx_obj_default = dx_obj
                    dx_obj_default.dlpx_ddps = {
                        delphix_ddp: dx_obj.dlpx_ddps[delphix_ddp]}
                    dx_logging.print_info(f'Executing against default DDP')
                    t=main_func(dx_obj.dlpx_ddps[delphix_ddp], dx_obj, single_thread)
                    threads.append(t)
                break
        except TypeError as err:
            raise dlpx_exceptions.DlpxException(f'Error in run_job: {err}')
    else:
        # Test to see if the engine exists in config_file
        try:
            t = main_func(dx_obj.dlpx_ddps[engine], dx_obj, single_thread)
            threads.append(t)
            dx_logging.print_info(f'Executing against Delphix DDP: '
                                  f'{dx_obj.dlpx_ddps[engine]}')
        except (exceptions.RequestError, KeyError):
            raise dlpx_exceptions.DlpxException(
                f'\nERROR: Delphix DDP {engine} cannot be found. Please '
                f'check your value and try again.')
    if engine is None:
        raise dlpx_exceptions.DlpxException(f'ERROR: No default Delphix '
                                            f'DDP found.')
    return threads



def find_job_state(engine, dx_obj, poll=10):
    """
    Retrieves running job state
    :param engine: Dictionary containing info on the DDP (IP, username, etc.)
    :param poll: How long to sleep between querying jobs
    :param dx_obj: Delphix session object from config
    :type dx_obj: lib.get_session.GetSession object
    :type poll: int
    :return:
    """
    # get all the jobs, then inspect them
    i = 0
    for j in dx_obj.jobs.keys():
        job_obj = job.get(dx_obj.server_session, dx_obj.jobs[j])
        dx_logging.print_debug(job_obj)
        dx_logging.print_info( f'{engine["ddp_identifier"]}: Running job: {job_obj.job_state}')
        if job_obj.job_state in ['CANCELED', 'COMPLETED', 'FAILED']:
            # If the job is in a non-running state, remove it
            # from the running jobs list.
            del dx_obj.jobs[j]
        elif job_obj.job_state in 'RUNNING':
            # If the job is in a running state, increment the
            # running job count.
            i += 1
        dx_logging.print_info(f'{engine["ddp_identifier"]}: {i} jobs running.')
        # If we have running jobs, pause before repeating the
        # checks.
        if dx_obj.jobs:
            time.sleep(poll)


def find_job_state_by_jobid(engine, dx_obj,job_id, poll=20):
    """
    Retrieves running job state
    :param engine: Dictionary containing info on the DDP (IP, username, etc.)
    :param poll: How long to sleep between querying jobs
    :param dx_obj: Delphix session object from config
    :type dx_obj: lib.get_session.GetSession object
    :param job_id: Job ID to check the state
    :type poll: int
    :return:
    """
    # get the job object
    job_obj = job.get(dx_obj.server_session, job_id)
    dx_logging.print_debug(job_obj)
    dx_logging.print_info(f' Polling for : {job_id} to finish')
    while job_obj.job_state == 'RUNNING':
        time.sleep(poll)
        job_obj = job.get(dx_obj.server_session, job_id)
    dx_logging.print_info(f'Job: {job_id} completed with status: {job_obj.job_state}')
    if job_obj.job_state =='FAILED':
        raise dlpx_exceptions.DlpxException('Job: {job_id} failed. Please check the error and retry.')
    # TODO: Pass the error message back to calling function.

def time_elapsed(time_start):
    """
    This function calculates the time elapsed since the beginning of the script.
    Call this anywhere you want to note the progress in terms of time

    :param time_start: start time of the script.
    :type time_start: float
    """
    return round((time.time() - time_start)/60, +1)
