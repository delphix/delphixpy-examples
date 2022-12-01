"""
Runs jobs passing a function as an argument. Thread safe.
"""
import re
import time

from delphixpy.v1_10_2 import exceptions
from delphixpy.v1_10_2.web import job
from lib import dlpx_exceptions
from lib import dx_logging

VERSION = "v.0.3.005"


def run_job(main_func, dx_obj, engine="default", single_thread=True):
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
    import pdb;pdb.set_trace()
    if engine == "all":
        dx_logging.print_info(f"Executing against all Delphix Engines")
        try:
            for delphix_ddp in dx_obj.dlpx_ddps:
                t = main_func(dx_obj.dlpx_ddps[delphix_ddp], dx_obj, single_thread)
                threads.append(t)
                # TODO: Revisit threading logic
                # This sleep has been tactically added to prevent errors in the parallel
                # processing of operations across multiple engines
                time.sleep(1)
        except dlpx_exceptions.DlpxException as err:
            dx_logging.print_exception(f"Error encountered in run_job():{err}")
    elif engine == "default":
        try:
            for delphix_ddp in dx_obj.dlpx_ddps.keys():
                if dx_obj.dlpx_ddps[delphix_ddp]["default"] == "True":
                    dx_obj_default = dx_obj
                    dx_obj_default.dlpx_ddps = {
                        delphix_ddp: dx_obj.dlpx_ddps[delphix_ddp]
                    }
                    dx_logging.print_info("Executing against default" "Delphix Engine")
                    t = main_func(dx_obj.dlpx_ddps[delphix_ddp], dx_obj, single_thread)
                    threads.append(t)
                break
        except TypeError as err:
            raise dlpx_exceptions.DlpxException(f"Error in run_job: {err}")
    else:
        # Test to see if the engine exists in config_file
        try:
            engine_ref = dx_obj.dlpx_ddps[engine]
            t = main_func(engine_ref, dx_obj, single_thread)
            threads.append(t)
            dx_logging.print_info(
                f"Executing against Delphix Engine: " f'{engine_ref["ip_address"]}'
            )
        except (exceptions.RequestError, KeyError):
            raise dlpx_exceptions.DlpxException(
                f"\nERROR: Delphix DDP {engine} cannot be found. Please "
                f"check your input and try again."
            )
    if engine is None:
        raise dlpx_exceptions.DlpxException(f"ERROR: No default Delphix " f"DDP found.")
    return threads


def run_job_mt(main_func, dx_obj, engine="default", single_thread=True):
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
    if engine == "all":
        dx_logging.print_info(f"Executing against all Delphix Engines")
        try:
            for delphix_ddp in dx_obj.dlpx_ddps:
                engine_ref = dx_obj.dlpx_ddps[delphix_ddp]
                dx_obj.jobs[engine_ref["ip_address"]] = []
                t = main_func(dx_obj.dlpx_ddps[delphix_ddp], dx_obj, single_thread)
                threads.append(t)
                # TODO: Revisit threading logic
                # This sleep has been tactically added to prevent errors in the parallel
                # processing of operations across multiple engines
                time.sleep(2)
        except dlpx_exceptions.DlpxException as err:
            dx_logging.print_exception(f"Error encountered in run_job():\n{err}")
            raise err
    elif engine == "default":
        try:
            for delphix_ddp in dx_obj.dlpx_ddps.keys():
                is_default = dx_obj.dlpx_ddps[delphix_ddp]["default"]
                if is_default is True:
                    dx_obj_default = dx_obj
                    dx_obj_default.dlpx_ddps = {
                        delphix_ddp: dx_obj.dlpx_ddps[delphix_ddp]
                    }
                    engine_ref = dx_obj.dlpx_ddps[delphix_ddp]
                    dx_obj.jobs[engine_ref["ip_address"]] = []
                    dx_logging.print_info(f"Executing against default Delphix Engine")
                    t = main_func(dx_obj.dlpx_ddps[delphix_ddp], dx_obj, single_thread)
                    threads.append(t)
                    break
        except TypeError as err:
            raise dlpx_exceptions.DlpxException(f"Error in run_job: {err}")
        except (dlpx_exceptions.DlpxException) as e:
            dx_logging.print_exception(f"Error in run_job():\n{e}")
            raise e
    else:
        # Test to see if the engine exists in config_file
        try:
            engine_ref = dx_obj.dlpx_ddps[engine]
            dx_obj.jobs[engine_ref["ip_address"]] = []
            t = main_func(engine_ref, dx_obj, single_thread)
            threads.append(t)
            dx_logging.print_info(
                f"Executing against Delphix Engine: " f'{engine_ref["ip_address"]}'
            )
        except (exceptions.RequestError, KeyError):
            raise dlpx_exceptions.DlpxException(
                f"\nERROR: Delphix DDP {engine} cannot be found. Please "
                f"check your input and try again."
            )
        except (dlpx_exceptions.DlpxException) as e:
            dx_logging.print_exception(f"Error in run_job():\n{e}")
            raise e
    if engine is None:
        raise dlpx_exceptions.DlpxException(f"ERROR: No default Delphix " f"DDP found.")
    return threads


def track_running_jobs(engine, dx_obj, poll=10):
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
    dx_logging.print_info(f'checking running jobs on engine: {engine["hostname"]}')
    engine_running_jobs = dx_obj.jobs[engine["ip_address"]]
    while engine_running_jobs:
        for j in engine_running_jobs:
            job_obj = job.get(dx_obj.server_session, j)
            if job_obj.job_state in ["COMPLETED"]:
                engine_running_jobs.remove(j)
                dx_logging.print_info(
                    f'Engine: {engine["hostname"]}: {job_obj.reference} is 100% COMPLETE'
                )
            elif job_obj.job_state in ["CANCELED", "FAILED"]:
                engine_running_jobs.remove(j)
                dx_logging.print_info(
                    f'Engine: {engine["hostname"]}: {job_obj.reference} was CANCELLED or FAILED due to an error'
                )
                # raise dlpx_exceptions.DlpxException('Job {job_obj.job_id} {job_obj.job_state}')
            elif job_obj.job_state in "RUNNING":
                dx_logging.print_info(
                    f'Engine: {engine["hostname"]}: {job_obj.reference} is RUNNING and {job_obj.percent_complete}% complete '
                )
        if dx_obj.jobs:
            time.sleep(poll)


def find_job_state(engine, dx_obj, poll=5):
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
    dx_logging.print_info(f"Checking running jobs state")
    i = 0
    for j in dx_obj.jobs.keys():
        print(len(dx_obj.jobs), j)
        job_obj = job.get(dx_obj.server_session, dx_obj.jobs[j])
        dx_logging.print_info(
            f'{engine["ip_address"]}: Running job: ' f"{job_obj.job_state}"
        )
        if job_obj.job_state in ["CANCELED", "COMPLETED", "FAILED"]:
            # If the job is in a non-running state, remove it
            # from the running jobs list.
            del dx_obj.jobs[j]
            if len(dx_obj.jobs) == 0:
                break
        elif job_obj.job_state in "RUNNING":
            # If the job is in a running state, increment the
            # running job count.
            i += 1
        dx_logging.print_info(f'{engine["ip_address"]}: {i} jobs running.')
        # If we have running jobs, pause before repeating the
        # checks.
        if dx_obj.jobs:
            time.sleep(poll)
        else:
            dx_logging.print_info(f"No jobs running")
            break


def find_job_state_by_jobid(engine, dx_obj, job_id, poll=20):
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
    dx_logging.print_info(f" Waiting for : {job_id} to finish")
    while job_obj.job_state == "RUNNING":
        time.sleep(poll)
        job_obj = job.get(dx_obj.server_session, job_id)
    dx_logging.print_info(f"Job: {job_id} completed with status: {job_obj.job_state}")
    return job_obj.job_state


def find_snapshot_ref_jobid(dx_obj, job_id):
    """
    Retrieves snapshot ref
    :param engine: Dictionary containing info on the DDP (IP, username, etc.)
    :param dx_obj: Delphix session object from config
    :type dx_obj: lib.get_session.GetSession object
    :param job_id: Job ID to check the state
    :return:
    """
    # get the job object
    job_obj = job.get(dx_obj, job_id)
    snapshot_details = job_obj.events[7].message_details
    try:
        snapshot_name = re.search('@.*Z', snapshot_details)
        return(snapshot_name.group())
    except AttributeError:
        return False


def time_elapsed(time_start):
    """
    This function calculates the time elapsed since the beginning of the script.
    Call this anywhere you want to note the progress in terms of time

    :param time_start: start time of the script.
    :type time_start: float
    """
    return round((time.time() - time_start) / 60, +1)
