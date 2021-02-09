#!/usr/bin/env python
# Corey Brune - Feb 2017
# Description:
# Create and sync a dSource
#
# Requirements
# pip install docopt delphixpy

# The below doc follows the POSIX compliant standards and allows us to use
# this doc to also define our arguments for the script.
"""Create and sync a dSource
Usage:
  dx_provision_dsource.py (--type <name>)
  dx_provision_dsource.py --type <name> --dsource_name <name> --ip_addr <name> --db_name <name> --env_name <name> --db_install_path <name> --dx_group <name> --db_passwd <name> --db_user <name> [--port_num <name>][--num_connections <name>][--link_now <name>][--files_per_set <name>][--rman_channels <name>]
    [--engine <identifier> | --all]
    [--debug] [--parallel <n>] [--poll <n>]
    [--config <path_to_file>] [--logdir <path_to_file>]
  dx_provision_dsource.py --type <name> --dsource_name <name> --ase_user <name> --ase_passwd <name> --backup_path <name> --source_user <name> --stage_user aseadmin --stage_repo ASE1570_S2 --src_config <name> --env_name <name> --dx_group <name> [--bck_file <name>][--create_bckup]
    [--engine <identifier> | --all]
    [--debug] [--parallel <n>] [--poll <n>]
    [--config <path_to_file>] [--logdir <path_to_file>]
  dx_provision_dsource.py --type <name> --dsource_name <name> --dx_group <name> --db_passwd <name> --db_user <name> --stage_instance <name> --stage_env <name> --backup_path <name> [--backup_loc_passwd <passwd> --backup_loc_user <name> --logsync [--sync_mode <mode>] --load_from_backup]
    [--engine <identifier> | --all]
    [--debug] [--parallel <n>] [--poll <n>]
    [--config <path_to_file>] [--logdir <path_to_file>]
  dx_provision_dsource.py -h | --help | -v | --version

Create and sync a dSource
Examples:
    Oracle:
    dx_provision_dsource.py --type oracle --dsource_name oradb1 --ip_addr 192.168.166.11 --db_name srcDB1 --env_name SourceEnv --db_install_path /u01/app/oracle/product/11.2.0.4/dbhome_1 --db_user delphixdb --db_passwd delphixdb

    Sybase:
    dx_provision_dsource.py --type sybase --dsource_name dbw1 --ase_user sa --ase_passwd sybase --backup_path /data/db --source_user aseadmin --stage_user aseadmin --stage_repo ASE1570_S2 --src_config dbw1 --env_name aseSource --dx_group Sources

    Specify backup files:
    dx_provision_dsource.py --type sybase --dsource_name dbw2 --ase_user sa --ase_passwd sybase --backup_path /data/db --source_user aseadmin --stage_user aseadmin --stage_repo ASE1570_S2 --src_config dbw2 --env_name aseSource --dx_group Sources --bck_file "dbw2data.dat"

    Create a new backup and ingest:
    dx_provision_dsource.py --type sybase --dsource_name dbw2 --ase_user sa --ase_passwd sybase --backup_path /data/db --source_user aseadmin --stage_user aseadmin --stage_repo ASE1570_S2 --src_config dbw2 --env_name aseSource --dx_group Sources --create_bckup

    MSSQL:
    dx_provision_dsource.py --type mssql  --dsource_name mssql_dsource --dx_group Sources --db_passwd delphix --db_user sa --stage_env mssql_target_svr --stage_instance MSSQLSERVER --backup_path \\bckserver\path\backups --backup_loc_passwd delphix --backup_loc_user delphix
    dx_provision_dsource.py --type mssql  --dsource_name AdventureWorks2014 --dx_group "9 - Sources" --db_passwd delphixdb --db_user aw --stage_env WINDOWSTARGET --stage_instance MSSQLSERVER --logsync --backup_path auto --load_from_backup


Options:
  --type <name>             dSource type. mssql, sybase or oracle
  --ip_addr <name>          IP Address of the dSource
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
  --db_user <name>          Username of the dSource DB
  --db_passwd <name>        Password of the db_user
  --bck_file <name>         Fully qualified name of backup file
  --port_num <name>         Port number of the listener. Default: 1521
  --src_config <name>       Name of the configuration environment
  --ase_passwd <name>       ASE DB password
  --ase_user <name>         ASE username
  --backup_path <path>      Path to the ASE/MSSQL backups
  --sync_mode <name>        MSSQL validated sync mode
                            [TRANSACTION_LOG|FULL_OR_DIFFERENTIAL|FULL|NONE]
  --source_user <name>      Environment username
  --stage_user <name>       Stage username
  --stage_repo <name>       Stage repository
  --stage_instance <name>   Name of the PPT instance
  --stage_env <name>        Name of the PPT server
  --logsync                 Enable logsync
  --backup_loc_passwd <passwd>  Password of the shared backup path (--bckup_path)
  --backup_loc_user <nam>   User of the shared backup path (--bckup_path)
  --load_from_backup        If set, Delphix will try to load the most recent full backup (MSSQL only)
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
from __future__ import print_function

import sys
from os.path import basename
from time import sleep
from time import time

from docopt import DocoptExit
from docopt import docopt

from delphixpy.v1_8_0.exceptions import HttpError
from delphixpy.v1_8_0.exceptions import JobError
from delphixpy.v1_8_0.exceptions import RequestError
from delphixpy.v1_8_0.web import database
from delphixpy.v1_8_0.web import environment
from delphixpy.v1_8_0.web import group
from delphixpy.v1_8_0.web import job
from delphixpy.v1_8_0.web import repository
from delphixpy.v1_8_0.web import sourceconfig
from delphixpy.v1_8_0.web.vo import ASELatestBackupSyncParameters
from delphixpy.v1_8_0.web.vo import ASELinkData
from delphixpy.v1_8_0.web.vo import ASENewBackupSyncParameters
from delphixpy.v1_8_0.web.vo import ASESpecificBackupSyncParameters
from delphixpy.v1_8_0.web.vo import LinkParameters
from delphixpy.v1_8_0.web.vo import MSSqlLinkData
from delphixpy.v1_8_0.web.vo import OracleInstance
from delphixpy.v1_8_0.web.vo import OracleLinkData
from delphixpy.v1_8_0.web.vo import OracleSIConfig
from delphixpy.v1_8_0.web.vo import OracleSourcingPolicy
from delphixpy.v1_8_0.web.vo import SourcingPolicy
from lib.DlpxException import DlpxException
from lib.DxLogging import logging_est
from lib.DxLogging import print_debug
from lib.DxLogging import print_exception
from lib.DxLogging import print_info
from lib.GetReferences import find_dbrepo
from lib.GetReferences import find_obj_by_name
from lib.GetReferences import get_running_job
from lib.GetSession import GetSession

VERSION = "v.0.2.0018"


def create_ora_sourceconfig(engine_name, port_num=1521):
    """
    :param ip_addr:
    :param db_name:
    :return:
    """
    create_ret = None
    env_obj = find_obj_by_name(
        dx_session_obj.server_session, environment, arguments["--env_name"]
    )

    try:
        sourceconfig_ref = find_obj_by_name(
            dx_session_obj.server_session, sourceconfig, arguments["--db_name"]
        ).reference
    except DlpxException:
        sourceconfig_ref = None

    repo_ref = find_dbrepo(
        dx_session_obj.server_session,
        "OracleInstall",
        env_obj.reference,
        arguments["--db_install_path"],
    ).reference

    dsource_params = OracleSIConfig()

    connect_str = (
        "jdbc:oracle:thin:@"
        + arguments["--ip_addr"]
        + ":"
        + str(port_num)
        + ":"
        + arguments["--db_name"]
    )

    dsource_params.database_name = arguments["--db_name"]
    dsource_params.unique_name = arguments["--db_name"]
    dsource_params.repository = repo_ref
    dsource_params.instance = OracleInstance()
    dsource_params.instance.instance_name = arguments["--db_name"]
    dsource_params.instance.instance_number = 1
    dsource_params.services = [
        {"type": "OracleService", "jdbcConnectionString": connect_str}
    ]

    try:
        if sourceconfig_ref is None:
            create_ret = link_ora_dsource(
                sourceconfig.create(dx_session_obj.server_session, dsource_params),
                env_obj.primary_user,
            )
        elif sourceconfig_ref is not None:
            create_ret = link_ora_dsource(sourceconfig_ref, env_obj.primary_user)

        print_info(
            "Created and linked the dSource {} with reference {}.\n".format(
                arguments["--db_name"], create_ret
            )
        )
        link_job_ref = dx_session_obj.server_session.last_job
        link_job_obj = job.get(dx_session_obj.server_session, link_job_ref)
        while link_job_obj.job_state not in ["CANCELED", "COMPLETED", "FAILED"]:
            print_info(
                "Waiting three seconds for link job to complete, and sync to begin"
            )
            sleep(3)
            link_job_obj = job.get(dx_session_obj.server_session, link_job_ref)

        # Add the snapsync job to the jobs dictionary
        dx_session_obj.jobs[engine_name + "snap"] = get_running_job(
            dx_session_obj.server_session,
            find_obj_by_name(
                dx_session_obj.server_session, database, arguments["--dsource_name"]
            ).reference,
        )
        print_debug(
            "Snapshot Job Reference: {}.\n".format(
                dx_session_obj.jobs[engine_name + "snap"]
            )
        )
    except (HttpError, RequestError) as e:
        print_exception("ERROR: Could not create the sourceconfig:\n" "{}".format(e))
        sys.exit(1)


def link_ora_dsource(srcconfig_ref, primary_user_ref):
    """
    :param srcconfig_ref: Reference to the sourceconfig object
    :param primary_user_ref: Reference to the environment user
    :return: Reference of the linked dSource
    """

    link_params = LinkParameters()
    link_params.link_data = OracleLinkData()
    link_params.link_data.sourcing_policy = OracleSourcingPolicy()
    link_params.name = arguments["--dsource_name"]
    link_params.group = find_obj_by_name(
        dx_session_obj.server_session, group, arguments["--dx_group"]
    ).reference
    link_params.link_data.compressedLinkingEnabled = True
    link_params.link_data.environment_user = primary_user_ref
    link_params.link_data.db_user = arguments["--db_user"]
    link_params.link_data.number_of_connections = int(arguments["--num_connections"])
    link_params.link_data.link_now = bool(arguments["--link_now"])
    link_params.link_data.files_per_set = int(arguments["--files_per_set"])
    link_params.link_data.rman_channels = int(arguments["--rman_channels"])
    link_params.link_data.db_credentials = {
        "type": "PasswordCredential",
        "password": arguments["--db_passwd"],
    }
    link_params.link_data.sourcing_policy.logsync_enabled = True
    # link_params.link_data.sourcing_policy.logsync_mode = 'ARCHIVE_REDO_MODE'
    link_params.link_data.config = srcconfig_ref
    try:
        return database.link(dx_session_obj.server_session, link_params)
    except (RequestError, HttpError) as e:
        print_exception(
            "Database link failed for {}:\n{}\n".format(arguments["--dsource_name"], e)
        )
        sys.exit(1)


def link_mssql_dsource(engine_name):
    """
    Link an MSSQL dSource
    """
    link_params = LinkParameters()
    link_params.name = arguments["--dsource_name"]
    link_params.link_data = MSSqlLinkData()

    try:
        env_obj_ref = find_obj_by_name(
            dx_session_obj.server_session, environment, arguments["--stage_env"]
        ).reference

        link_params.link_data.ppt_repository = find_dbrepo(
            dx_session_obj.server_session,
            "MSSqlInstance",
            env_obj_ref,
            arguments["--stage_instance"],
        ).reference
        link_params.link_data.config = find_obj_by_name(
            dx_session_obj.server_session, sourceconfig, arguments["--dsource_name"]
        ).reference
        link_params.group = find_obj_by_name(
            dx_session_obj.server_session, group, arguments["--dx_group"]
        ).reference

    except DlpxException as e:
        print_exception(
            "Could not link {}: {}\n".format(arguments["--dsource_name"], e)
        )
        sys.exit(1)

    if arguments["--backup_path"] != "auto":
        link_params.link_data.shared_backup_location = arguments["--backup_path"]

    if arguments["--backup_loc_passwd"]:
        link_params.link_data.backup_location_credentials = {
            "type": "PasswordCredential",
            "password": arguments["--backup_loc_passwd"],
        }
        link_params.link_data.backup_location_user = arguments["--backup_loc_user"]

    link_params.link_data.db_credentials = {
        "type": "PasswordCredential",
        "password": arguments["--db_passwd"],
    }
    link_params.link_data.db_user = arguments["--db_user"]

    link_params.link_data.sourcing_policy = SourcingPolicy()

    if arguments["--load_from_backup"]:
        link_params.link_data.sourcing_policy.load_from_backup = True

    if arguments["--sync_mode"]:
        link_params.link_data.validated_sync_mode = arguments["sync_mode"]

    if arguments["--logsync"]:
        link_params.link_data.sourcing_policy.logsync_enabled = True

    try:
        database.link(dx_session_obj.server_session, link_params)
        dx_session_obj.jobs[engine_name] = dx_session_obj.server_session.last_job
        dx_session_obj.jobs[engine_name + "snap"] = get_running_job(
            dx_session_obj.server_session,
            find_obj_by_name(
                dx_session_obj.server_session, database, arguments["--dsource_name"]
            ).reference,
        )

    except (HttpError, RequestError, JobError) as e:
        print_exception(
            "Database link failed for {}:\n{}\n".format(arguments["--dsource_name"], e)
        )


def link_ase_dsource(engine_name):
    """
    Link an ASE dSource
    """

    link_params = LinkParameters()
    link_params.name = arguments["--dsource_name"]
    link_params.link_data = ASELinkData()
    link_params.link_data.db_credentials = {
        "type": "PasswordCredential",
        "password": arguments["--ase_passwd"],
    }
    link_params.link_data.db_user = arguments["--ase_user"]
    link_params.link_data.load_backup_path = arguments["--backup_path"]

    if arguments["--bck_file"]:
        link_params.link_data.sync_parameters = ASESpecificBackupSyncParameters()
        bck_files = (arguments["--bck_file"]).split(" ")
        link_params.link_data.sync_parameters.backup_files = bck_files

    elif arguments["--create_bckup"]:
        link_params.link_data.sync_parameters = ASENewBackupSyncParameters()

    else:
        link_params.link_data.sync_parameters = ASELatestBackupSyncParameters()

    try:
        link_params.group = find_obj_by_name(
            dx_session_obj.server_session, group, arguments["--dx_group"]
        ).reference
        env_user_ref = link_params.link_data.stage_user = find_obj_by_name(
            dx_session_obj.server_session, environment, arguments["--env_name"]
        ).primary_user
        link_params.link_data.staging_host_user = env_user_ref
        link_params.link_data.source_host_user = env_user_ref

        link_params.link_data.config = find_obj_by_name(
            dx_session_obj.server_session, sourceconfig, arguments["--src_config"]
        ).reference
        link_params.link_data.staging_repository = find_obj_by_name(
            dx_session_obj.server_session, repository, arguments["--stage_repo"]
        ).reference

    except DlpxException as e:
        print_exception(
            "Could not link {}: {}\n".format(arguments["--dsource_name"], e)
        )
        sys.exit(1)

    try:
        dsource_ref = database.link(dx_session_obj.server_session, link_params)
        dx_session_obj.jobs[engine_name] = dx_session_obj.server_session.last_job
        dx_session_obj.jobs[engine_name + "snap"] = get_running_job(
            dx_session_obj.server_session,
            find_obj_by_name(
                dx_session_obj.server_session, database, arguments["--dsource_name"]
            ).reference,
        )
        print(
            "{} sucessfully linked {}".format(dsource_ref, arguments["--dsource_name"])
        )
    except (RequestError, HttpError) as e:
        print_exception(
            "Database link failed for {}:\n{}".format(arguments["--dsource_name"], e)
        )


def run_async(func):
    """
    http://code.activestate.com/recipes/576684-simple-threading-decorator/
    run_async(func)
        function decorator, intended to make "func" run in a separate
        thread (asynchronously).
        Returns the created Thread object
        E.g.:
        @run_async
        def task1():
            do_something
        @run_async
        def task2():
            do_something_too
        t1 = task1()
        t2 = task2()
        ...
        t1.join()
        t2.join()
    """
    from threading import Thread
    from functools import wraps

    @wraps(func)
    def async_func(*args, **kwargs):
        func_hl = Thread(target=func, args=args, kwargs=kwargs)
        func_hl.start()
        return func_hl

    return async_func


@run_async
def main_workflow(engine):
    """
    This function actually runs the jobs.
    Use the @run_async decorator to run this function asynchronously.
    This allows us to run against multiple Delphix Engine simultaneously

    engine: Dictionary of engines
    """
    jobs = {}

    try:
        # Setup the connection to the Delphix Engine
        dx_session_obj.serversess(
            engine["ip_address"], engine["username"], engine["password"]
        )

    except DlpxException as e:
        print_exception(
            "\nERROR: Engine {} encountered an error while"
            "{}:\n{}\n".format(
                dx_session_obj.dlpx_engines["hostname"], arguments["--target"], e
            )
        )
        sys.exit(1)
    thingstodo = ["thingtodo"]
    try:
        with dx_session_obj.job_mode(single_thread):
            while len(dx_session_obj.jobs) > 0 or len(thingstodo) > 0:
                if len(thingstodo) > 0:
                    if arguments["--type"].lower() == "oracle":
                        create_ora_sourceconfig(engine["hostname"])
                    elif arguments["--type"].lower() == "sybase":
                        link_ase_dsource(engine["hostname"])
                    elif arguments["--type"].lower() == "mssql":
                        link_mssql_dsource(engine["hostname"])
                    thingstodo.pop()
                # get all the jobs, then inspect them
                i = 0
                for j in dx_session_obj.jobs.keys():
                    job_obj = job.get(
                        dx_session_obj.server_session, dx_session_obj.jobs[j]
                    )
                    print_debug(job_obj)
                    print_info(
                        "{}: Provisioning dSource: {}".format(
                            engine["hostname"], job_obj.job_state
                        )
                    )
                    if job_obj.job_state in ["CANCELED", "COMPLETED", "FAILED"]:
                        # If the job is in a non-running state, remove it
                        # from the
                        # running jobs list.
                        del dx_session_obj.jobs[j]
                    elif job_obj.job_state in "RUNNING":
                        # If the job is in a running state, increment the
                        # running job count.
                        i += 1
                    print_info("{}: {:d} jobs running.".format(engine["hostname"], i))
                    # If we have running jobs, pause before repeating the
                    # checks.
                    if len(dx_session_obj.jobs) > 0:
                        sleep(float(arguments["--poll"]))

    except (HttpError, RequestError, JobError, DlpxException) as e:
        print_exception(
            "ERROR: Could not complete ingesting the source " "data:\n{}".format(e)
        )
        sys.exit(1)


def run_job():
    """
    This function runs the main_workflow aynchronously against all the servers
    specified
    """
    # Create an empty list to store threads we create.
    threads = []
    engine = None

    # If the --all argument was given, run against every engine in dxtools.conf
    if arguments["--all"]:
        print_info("Executing against all Delphix Engines in the dxtools.conf")

        try:
            # For each server in the dxtools.conf...
            for delphix_engine in dx_session_obj.dlpx_engines:
                engine = dx_session_obj[delphix_engine]
                # Create a new thread and add it to the list.
                threads.append(main_workflow(engine))

        except DlpxException as e:
            print("Error encountered in run_job():\n%s" % (e))
            sys.exit(1)

    elif arguments["--all"] is False:
        # Else if the --engine argument was given, test to see if the engine
        # exists in dxtools.conf
        if arguments["--engine"]:
            try:
                engine = dx_session_obj.dlpx_engines[arguments["--engine"]]
                print_info(
                    "Executing against Delphix Engine: {}\n".format(
                        (arguments["--engine"])
                    )
                )

            except (DlpxException, RequestError, KeyError) as e:
                raise DlpxException(
                    "\nERROR: Delphix Engine {} cannot be "
                    "found in {}. Please check your value "
                    "and try again. Exiting.\n".format(
                        arguments["--engine"], config_file_path
                    )
                )

        else:
            # Else search for a default engine in the dxtools.conf
            for delphix_engine in dx_session_obj.dlpx_engines:
                if dx_session_obj.dlpx_engines[delphix_engine]["default"] == "true":

                    engine = dx_session_obj.dlpx_engines[delphix_engine]
                    print_info(
                        "Executing against the default Delphix Engine "
                        "in the dxtools.conf: %s"
                        % (dx_session_obj.dlpx_engines[delphix_engine]["hostname"])
                    )

                break

            if engine == None:
                raise DlpxException("\nERROR: No default engine found. Exiting")

    # run the job against the engine
    threads.append(main_workflow(engine))

    # For each thread in the list...
    for each in threads:
        # join them back together so that we wait for all threads to complete
        # before moving on
        each.join()


def time_elapsed():
    """
    This function calculates the time elapsed since the beginning of the script.
    Call this anywhere you want to note the progress in terms of time
    """
    elapsed_minutes = round((time() - time_start) / 60, +1)
    return elapsed_minutes


def main(argv):
    # We want to be able to call on these variables anywhere in the script.
    global single_thread
    global usebackup
    global time_start
    global config_file_path
    global database_name
    global dx_session_obj
    global debug

    if arguments["--debug"]:
        debug = True

    try:
        dx_session_obj = GetSession()
        logging_est(arguments["--logdir"])
        print_debug(arguments)
        time_start = time()
        engine = None
        single_thread = False
        config_file_path = arguments["--config"]
        # Parse the dxtools.conf and put it into a dictionary
        dx_session_obj.get_config(config_file_path)

        # This is the function that will handle processing main_workflow for
        # all the servers.
        run_job()

        elapsed_minutes = time_elapsed()
        print_info(
            "script took {} minutes to get this far.".format(str(elapsed_minutes))
        )

    # Here we handle what we do when the unexpected happens
    except SystemExit as e:
        """
        This is what we use to handle our sys.exit(#)
        """
        sys.exit(e)

    except HttpError as e:
        """
        We use this exception handler when our connection to Delphix fails
        """
        print_exception(
            "Connection failed to the Delphix Engine"
            "Please check the ERROR message below:\n{}".format(e)
        )
        sys.exit(1)

    except JobError as e:
        """
        We use this exception handler when a job fails in Delphix so that
        we have actionable data
        """
        elapsed_minutes = time_elapsed()
        print_exception("A job failed in the Delphix Engine")
        print_info(
            "{} took {:.2f} minutes to get this far:\n{}\n".format(
                basename(__file__), elapsed_minutes, e
            )
        )
        sys.exit(3)

    except KeyboardInterrupt:
        """
        We use this exception handler to gracefully handle ctrl+c exits
        """
        print_debug("You sent a CTRL+C to interrupt the process")
        elapsed_minutes = time_elapsed()
        print_info(
            "{} took {:.2f} minutes to get this far\n".format(
                basename(__file__), elapsed_minutes
            )
        )

    except:
        """
        Everything else gets caught here
        """
        print_exception(sys.exc_info()[0])
        elapsed_minutes = time_elapsed()
        print_info(
            "{} took {:.2f} minutes to get this far\n".format(
                basename(__file__), elapsed_minutes
            )
        )
        sys.exit(1)


if __name__ == "__main__":

    try:
        # Grab our arguments from the doc at the top of the script
        arguments = docopt(__doc__, version=basename(__file__) + " " + VERSION)
        # Feed our arguments to the main function, and off we go!
        main(arguments)

    except DocoptExit as e:
        # print 'Exited because options were not specified: {}\n'.format(e)
        print(e.message)
