#!/usr/bin/env python3
# Description:
# This script will setup replication between two hosts.
#
# Requirements
# pip install docopt delphixpy

# The below doc follows the POSIX compliant standards and allows us to use
# this doc to also define our ARGUMENTS for the script.
"""Description
Usage:
  dx_replication.py --rep_name <name> --target_host <target> --target_user <name> --target_pw <password> --rep_objs <objects> [--schedule <name> --bandwidth <MBs> --num_cons <connections> --enabled]
                  [--engine <identifier> | --all] [--single_thread <bool>]
                  [--debug] [--parallel <n>] [--poll <n>]
                  [--config <path_to_file>] [--logdir <path_to_file>]
  dx_replication.py --delete <rep_name>
                  [--config <path_to_file>] [--logdir <path_to_file>]
  dx_replication.py --execute <rep_name>
                  [--config <path_to_file>] [--logdir <path_to_file>]
  dx_replication.py --list
                  [--config <path_to_file>] [--logdir <path_to_file>]
  
  dx_replication.py -h | --help | -v | --version

Description
Setup replication between two hosts.
Examples:
dx_replication.py --rep_name mytest --target_host 172.16.169.141 --target_user admin --target_pw delphix --rep_objs mytest1 --schedule '55 0 19 * * ?' --enabled
dx_replication.py --rep_name mytest --target_host 172.16.169.141 --target_user admin --target_pw delphix --rep_objs mytest1 --schedule '0 40 20 */4 * ?' --bandwidth 5 --num_cons 2 --enabled

dx_replication.py --delete mytest

Options:
  --rep_name <name>         Name of the replication job.
  --target_host <target>    Name / IP of the target replication host.
  --target_user <name>      Username for the replication target host.
  --target_pw <password>    Password for the user.
  --schedule <name>         Schedule of the replication job in crontab format. (seconds, minutes, hours, day of month, month)
                            [default: 0 0 0 * * ?]
  --rep_objs <objects>      Comma delimited list of objects to replicate.
  --delete <rep_name>       Name of the replication job to delete.
  --bandwidth <MBs>         Limit bandwidth to MB/s.
  --num_cons <connections>  Number of network connections for the replication job.
  --list                    List all of the replication jobs.
  --execute <rep_name>      Name of the replication job to execute.
  --single_thread <bool>    Run as a single thread? True or False
                            [default: False]
  --engine <type>           Alt Identifier of Delphix engine in dxtools.conf.
                            [default: default]
  --all                     Run against all engines.
  --debug                   Enable debug logging
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./dxtools.conf]
  --logdir <path_to_file>    The path to the logfile you want to use.
                            [default: ./dx_replication.log]
  -h --help                 Show this screen.
  -v --version              Show version.
"""
from __future__ import print_function

VERSION = "v.0.1.002"

import sys
from os.path import basename
from time import sleep
import time
from tabulate import tabulate
import docopt

from delphixpy.v1_10_2 import exceptions
from delphixpy.v1_10_2.web import host
from delphixpy.v1_10_2.web import vo
from delphixpy.v1_10_2.web.replication import spec

from lib import dlpx_exceptions
from lib import dx_logging
from lib import get_references
from lib import get_session
from lib import run_job
from lib.run_async import run_async

def create_replication_job(dx_session_obj):
    """
    Create a replication job
    :return: Reference to the spec object
    """
    rep_spec = vo.ReplicationSpec()
    rep_spec.name = ARGUMENTS["--rep_name"]
    rep_spec.target_host = ARGUMENTS["--target_host"]
    rep_spec.target_principal = ARGUMENTS["--target_user"]
    rep_spec.target_credential = {
        "type": "PasswordCredential",
        "password": ARGUMENTS["--target_pw"],
    }
    rep_spec.object_specification = vo.ReplicationSecureList()
    rep_spec.schedule = ARGUMENTS["--schedule"]
    rep_spec.encrypted = True

    if ARGUMENTS["--num_cons"]:
        rep_spec.number_of_connections = int(ARGUMENTS["--num_cons"])
    if ARGUMENTS["--bandwidth"]:
        rep_spec.bandwidth_limit = int(ARGUMENTS["--bandwidth"])
    if ARGUMENTS["--enabled"]:
        rep_spec.enabled = True
    try:
        rep_spec.object_specification.containers = get_references.find_obj_specs(
            dx_session_obj.server_session, ARGUMENTS["--rep_objs"].split(",")
        )
        #rep_spec.object_specification.objects = get_references.find_obj_specs(
        #    dx_session_obj.server_session, ARGUMENTS["--rep_objs"].split(",")
        #)

        ref = spec.create(dx_session_obj.server_session, rep_spec)
        if dx_session_obj.server_session.last_job:
            dx_session_obj.jobs[
                dx_session_obj.server_session.address
            ] = dx_session_obj.server_session.last_job
        dx_logging.print_info(
            "Successfully created {} with reference "
            "{}\n".format(ARGUMENTS["--rep_name"], ref)
        )

    except (exceptions.HttpError, exceptions.RequestError, dlpx_exceptions.DlpxException) as e:
        dx_logging.print_exception(
            "Could not create replication job {}:\n{}".format(
                ARGUMENTS["--rep_name"], e
            )
        )


def delete_replication_job(dx_session_obj):
    """
    Delete a replication job.
    :return: Reference to the spec object
    """
    try:
        spec.delete(
            dx_session_obj.server_session,
            get_references.find_obj_by_name(
                dx_session_obj.server_session, spec, ARGUMENTS["--delete"]
            ).reference,
        )
        if dx_session_obj.server_session.last_job:
            dx_session_obj.jobs[
                dx_session_obj.server_session.address
            ] = dx_session_obj.server_session.last_job
        dx_logging.print_info("Successfully deleted {}.\n".format(ARGUMENTS["--delete"]))

    except (exceptions.HttpError, exceptions.RequestError, dlpx_exceptions.DlpxException) as e:
        print_exception(
            "Was not able to delete {}:\n{}".format(ARGUMENTS["--delete"], e)
        )


def list_replication_jobs(dx_session_obj):
    """
    List the replication jobs on a given engine
    """
    table_lst = []
    final_lst = []
    #import pdb;pdb.set_trace()
    for rep_job in spec.get_all(dx_session_obj.server_session):
        table_lst = [rep_job.name,
                     rep_job.reference,
                     rep_job.schedule,
                     rep_job.bandwidth_limit,
        ]
        final_lst.append(table_lst)
    print (tabulate(final_lst,
                    headers=["Name", "Reference",
                             "Schedule", "Bandwidth Limit"]))
        #for obj_spec_ref in rep_job.object_specification:
        #    obj_names_lst.append(
        #        database.get(dx_session_obj.server_session, obj_spec_ref).name
        #    )
        #print (
        #    "Name: {}\nReplicated Objects: {}\nEnabled: {}\nEncrypted: {}\n"
        #    "Reference: {}\nSchedule: {}\nTarget Host: {}\n\n".format(
        #        rep_job.name,
        #        ", ".join(obj_names_lst),
        #        rep_job.enabled,
        #        rep_job.encrypted,
        #        rep_job.reference,
        #        rep_job.schedule,
        #        rep_job.target_host,
        #    )
        #)


def execute_replication_job(obj_name, dx_session_obj):
    """
    Execute a replication job immediately.
    :param obj_name: name of object to execute.
    """
    try:
        spec.execute(
            dx_session_obj.server_session,
            get_references.find_obj_by_name(dx_session_obj.server_session, spec, obj_name).reference,
        )
        if dx_session_obj.server_session.last_job:
            dx_session_obj.jobs[
                dx_session_obj.server_session.address
            ] = dx_session_obj.server_session.last_job
        dx_logging.print_info("Successfully executed {}.\n".format(obj_name))
    except (exceptions.HttpError, exceptions.RequestError, dlpx_exceptions.DlpxException, exceptions.JobError) as e:
        print_exception("Could not execute job {}:\n{}".format(obj_name, e))


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
            engine["ip_address"], engine["username"], engine["password"],
            engine["use_https"]
        )   
    except dlpx_exceptions.DlpxException as err:
        dx_logging.print_exception(
            f"ERROR: dx_environment encountered an error authenticating to "
            f' {engine["ip_address"]} :\n{err}'
        )
    try:
        with dlpx_obj.job_mode(single_thread):
            if ARGUMENTS["--rep_name"]:
                create_replication_job(dlpx_obj)
            elif ARGUMENTS["--delete"]:
                delete_replication_job(dlpx_obj)
            elif ARGUMENTS["--list"]:
                list_replication_jobs(dlpx_obj)
            elif ARGUMENTS["--execute"]:
                execute_replication_job(ARGUMENTS["--execute"], dlpx_obj)
    except (
        dlpx_exceptions.DlpxException,
        exceptions.RequestError,
        exceptions.JobError,
        exceptions.HttpError,
    ) as err:
        dx_logging.print_exception(
            f"Error in dx_environment for engine:"
            f'{engine["ip_address"]}: Error Message: {err}'
        )
    except (exceptions.HttpError,
            exceptions.RequestError,
            exceptions.JobError,
            dlpx_exceptions.DlpxException
           ) as err:
        print_exception(
            "ERROR: Could not complete replication" " operation:{}".format(err)
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
        for each in run_job.run_job_mt(
            main_workflow, dx_session_obj, engine, single_thread
        ):
            each.join()
        elapsed_minutes = run_job.time_elapsed(time_start)
        dx_logging.print_info(
            f"dx_replication took {elapsed_minutes} minutes to complete."
        )
    except SystemExit as err:
        # This is what we use to handle our sys.exit(#)
        sys.exit(err)
    except dlpx_exceptions.DlpxException as err:
        # Handle an error occurs in a function call.
        dx_logging.print_exception(
            f"ERROR: Please check the ERROR message below:\n {err.error}"
        )
        sys.exit(2)
    except exceptions.HttpError as err:
        dx_logging.print_exception(
            f"ERROR: Connection failed to the Delphix DDP. Please check "
            f"the ERROR message below:\n{err.status}"
        )
        sys.exit(2)
    except exceptions.JobError as err:
        # If a job fails in Delphix so that we have actionable data
        elapsed_minutes = run_job.time_elapsed(time_start)
        dx_logging.print_exception(
            f"A job failed in the Delphix Engine:\n{err.job}."
            f"{basename(__file__)} took {elapsed_minutes} minutes to "
            f"complete"
        )
        sys.exit(3)
    except KeyboardInterrupt:
        # Gracefully handle ctrl+c exits
        dx_logging.print_debug("You sent a CTRL+C to interrupt the process")
        elapsed_minutes = run_job.time_elapsed(time_start)
        dx_logging.print_info(
            f"{basename(__file__)} took {elapsed_minutes} " f"minutes to complete."
        )


if __name__ == "__main__":
    # Grab our ARGUMENTS from the doc at the top of the script
    ARGUMENTS = docopt.docopt(__doc__, version=basename(__file__) + " " + VERSION)
    # Feed our ARGUMENTS to the main function, and off we go!
    main()

