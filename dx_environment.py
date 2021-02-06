#!/usr/bin/env python
# Corey Brune 08 2016
# This script creates an environment
# requirements
# pip install docopt delphixpy

# The below doc follows the POSIX compliant standards and allows us to use
# this doc to also define our arguments for the script.

"""Create Host Environment

Usage:
  dx_environment.py (--type <name> --env_name <name> --host_user <username> \
--ip <address> [--toolkit <path_to_the_toolkit>] [--ase --ase_user <name> --ase_pw <name>] \
|--update_ase_pw <name> --env_name <name> | --update_ase_user <name> --env_name <name> \
| --delete <env_name> | --refresh <env_name> | --list)
[--logdir <directory>][--debug] [--config <filename>] [--connector_name <name>]
[--pw <password>][--engine <identifier>][--all] [--poll <n>]
  dx_environment.py (--update_host --old_host_address <name> --new_host_address <name>) [--logdir <directory>][--debug] [--config <filename>]
  dx_environment.py ([--enable]|[--disable]) --env_name <name> [--logdir <directory>][--debug] [--config <filename>]
  dx_environment.py -h | --help | -v | --version

Create a Delphix environment. (current support for standalone environments only)

Examples:
  dx_environment.py --engine landsharkengine --type linux --env_name test1 --host_user delphix --pw delphix --ip 182.1.1.1 --toolkit /var/opt/delphix
  dx_environment.py --type linux --env_name test1 --update_ase_pw newPasswd
  dx_environment.py --type linux --env_name test1 --host_user delphix --pw delphix --ip 182.1.1.1 --toolkit /var/opt/delphix
  dx_environment.py --update_host --host_name 10.0.3.60
  dx_environment.py --type linux --env_name test1 --host_user delphix --pw delphix --ip 182.1.1.1 --toolkit /var/opt/delphix --ase --ase_user sa --ase_pw delphixpw
  dx_environment.py --type windows --env_name SOURCE --host_user delphix.local\\administrator --ip 10.0.1.50 --toolkit foo --config dxtools.conf --pw 'myTempPassword123!' --debug --connector_name 10.0.1.60
  dx_environment.py --enable --env_name SOURCE
  dx_environment.py --disable --env_name SOURCE
  dx_environment.py --list

Options:
  --type <name>             The OS type for the environment
  --env_name <name>         The name of the Delphix environment
  --ip <addr>               The IP address of the Delphix environment
  --list                    List all of the environments for a given engine
  --toolkit <path>          Path of the toolkit. Required for Unix/Linux
  --host_user <username>    The username on the Delphix environment
  --delete <environment>    The name of the Delphix environment to delete
  --update_ase_pw <name>    The new ASE DB password
  --refresh <environment>   The name of the Delphix environment to refresh. Specify "all" to refresh all environments
  --pw <password>           Password of the user
  --connector_name <environment>   The name of the Delphix connector to use. Required for Windows source environments
  --update_ase_user <name>  Update the ASE DB username
  --ase                     Flag to enable ASE environments
  --ase_user <name>         The ASE DB username
  --ase_pw <name>           Password of the ASE DB user
  --all                     Run against all engines.
  --debug                   Enable debug logging
  --parallel <n>            Limit number of jobs to maxjob
  --engine <type>           Identifier of Delphix engine in dxtools.conf.

  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./dxtools.conf]
  --logdir <path_to_file>    The path to the logfile you want to use.
                            [default: ./dx_environment.log]
  -h --help                 Show this screen.
  -v --version              Show version.
  --update_host             Update the host address for an environment
  --old_host_address <name> The current name of the host, as registered in Delphix. Required for update_host
  --new_host_address <name> The desired name of the host, as registered in Delphix. Required for update_host
  --enable                  Enable the named environment
  --disable                 Disable the named environment

"""
from __future__ import print_function

import sys
import traceback
from os.path import basename
from time import sleep
from time import time

from docopt import docopt

from delphixpy.v1_8_0.exceptions import HttpError
from delphixpy.v1_8_0.exceptions import JobError
from delphixpy.v1_8_0.exceptions import RequestError
from delphixpy.v1_8_0.web import environment
from delphixpy.v1_8_0.web import host
from delphixpy.v1_8_0.web import job
from delphixpy.v1_8_0.web.vo import ASEHostEnvironmentParameters
from delphixpy.v1_8_0.web.vo import HostEnvironmentCreateParameters
from delphixpy.v1_8_0.web.vo import UnixHost
from delphixpy.v1_8_0.web.vo import UnixHostEnvironment
from delphixpy.v1_8_0.web.vo import WindowsHost
from delphixpy.v1_8_0.web.vo import WindowsHostEnvironment
from lib.DlpxException import DlpxException
from lib.DxLogging import logging_est
from lib.DxLogging import print_debug
from lib.DxLogging import print_exception
from lib.DxLogging import print_info
from lib.GetReferences import find_all_objects
from lib.GetReferences import find_obj_by_name
from lib.GetReferences import find_obj_name
from lib.GetSession import GetSession

VERSION = "v.0.3.612"


def enable_environment(dlpx_obj, env_name):
    """
    Enable the given host
    """
    engine_name = dlpx_obj.dlpx_engines.keys()[0]

    env_obj = find_obj_by_name(dlpx_obj.server_session, environment, env_name)

    try:
        environment.enable(dlpx_obj.server_session, env_obj.reference)
        print("Attempting to enable {}".format(env_name))
    except (DlpxException, RequestError) as e:
        print_exception(
            "\nERROR: Enabling the host {} "
            "encountered an error:\n{}".format(env_name, e)
        )
        sys.exit(1)


def disable_environment(dlpx_obj, env_name):
    """
    Enable the given host
    """
    engine_name = dlpx_obj.dlpx_engines.keys()[0]
    env_obj = find_obj_by_name(dlpx_obj.server_session, environment, env_name)

    try:
        environment.disable(dlpx_obj.server_session, env_obj.reference)
        print("Attempting to disable {}".format(env_name))
    except (DlpxException, RequestError) as e:
        print_exception(
            "\nERROR: Disabling the host {} "
            "encountered an error:\n{}".format(env_name, e)
        )
        sys.exit(1)


def update_host_address(dlpx_obj, old_host_address, new_host_address):
    """
    Update the given host
    """
    engine_name = dlpx_obj.dlpx_engines.keys()[0]
    old_host_obj = find_obj_by_name(dlpx_obj.server_session, host, old_host_address)
    if old_host_obj.type == "WindowsHost":
        host_obj = WindowsHost()
    else:
        host_obj = UnixHost()
    host_obj.address = new_host_address
    try:
        host.update(dlpx_obj.server_session, old_host_obj.reference, host_obj)

        print(
            "Attempting to update {} to {}".format(old_host_address, new_host_address)
        )

    except (DlpxException, RequestError) as e:
        print_exception(
            "\nERROR: Updating the host {} "
            "encountered an error:\n{}".format(env_name, e)
        )
        sys.exit(1)


def list_env(dlpx_obj):
    """
    List all environments for a given engine
    """
    engine_name = dlpx_obj.dlpx_engines.keys()[0]

    all_envs = environment.get_all(dlpx_obj.server_session)
    for env in all_envs:
        env_user = find_obj_name(
            dlpx_obj.server_session, environment.user, env.primary_user
        )
        try:
            env_host = find_obj_name(dlpx_obj.server_session, host, env.host)
        except AttributeError:
            pass

        if env.type == "WindowsHostEnvironment":
            print(
                "Environment Name: {}, Username: {}, Host: {},"
                "Enabled: {}, ".format(env.name, env_user, env_host, env.enabled)
            )
        elif env.type == "WindowsCluster" or env.type == "OracleCluster":
            print(
                "Environment Name: {}, Username: {}"
                "Enabled: {}, ".format(env.name, env_user, env.enabled)
            )
        else:
            print(
                "Environment Name: {}, Username: {}, Host: {}, Enabled: {},"
                " ASE Environment Params: {}".format(
                    env.name,
                    env_user,
                    env_host,
                    env.enabled,
                    env.ase_host_environment_parameters
                    if isinstance(
                        env.ase_host_environment_parameters,
                        ASEHostEnvironmentParameters,
                    )
                    else "Undefined",
                )
            )


def delete_env(dlpx_obj, env_name):
    """
    Deletes an environment

    engine: Dictionary of engines
    env_name: Name of the environment to delete
    """
    engine_name = dlpx_obj.dlpx_engines.keys()[0]

    env_obj = find_obj_by_name(dlpx_obj.server_session, environment, env_name)

    if env_obj:
        environment.delete(dlpx_obj.server_session, env_obj.reference)
        dlpx_obj.jobs[engine_name] = dlpx_obj.server_session.last_job

    elif env_obj is None:
        print("Environment was not found in the Engine: {}".format(env_name))
        sys.exit(1)


def refresh_env(dlpx_obj, env_name):
    """
    Refresh the environment

    engine: Dictionary of engines
    env_name: Name of the environment to refresh
    """
    engine_name = dlpx_obj.dlpx_engines.keys()[0]

    if env_name == "all":
        env_list = find_all_objects(dlpx_obj.server_session, environment)
        for env_obj in env_list:
            try:
                environment.refresh(dlpx_obj.server_session, env_obj.reference)
                dlpx_obj.jobs[engine_name] = dlpx_obj.server_session.last_job

            except (DlpxException, RequestError) as e:
                print_exception(
                    "\nERROR: Refreshing the environment {} "
                    "encountered an error:\n{}".format(env_name, e)
                )
                sys.exit(1)
    else:

        try:
            env_obj = find_obj_by_name(dlpx_obj.server_session, environment, env_name)

            environment.refresh(dlpx_obj.server_session, env_obj.reference)
            dlpx_obj.jobs[engine_name] = dlpx_obj.server_session.last_job

        except (DlpxException, RequestError) as e:
            print_exception(
                "\nERROR: Refreshing the environment {} "
                "encountered an error:\n{}".format(env_name, e)
            )
            sys.exit(1)


def update_ase_username(dlpx_obj):
    """
    Update the ASE database user password
    """
    engine_name = dlpx_obj.dlpx_engines.keys()[0]

    env_obj = UnixHostEnvironment()
    env_obj.ase_host_environment_parameters = ASEHostEnvironmentParameters()
    env_obj.ase_host_environment_parameters.db_user = arguments["--update_ase_user"]

    try:
        environment.update(
            dlpx_obj.server_session,
            find_obj_by_name(
                dlpx_obj.server_session, environment, arguments["--env_name"], env_obj
            ).reference,
            env_obj,
        )

    except (HttpError, RequestError) as e:
        print_exception(
            "\nERROR: Updating the ASE DB password " "failed:\n{}\n".format(e)
        )


def update_ase_pw(dlpx_obj):
    """
    Update the ASE database user password
    """
    engine_name = dlpx_obj.dlpx_engines.keys()[0]
    env_obj = UnixHostEnvironment()
    env_obj.ase_host_environment_parameters = ASEHostEnvironmentParameters()
    env_obj.ase_host_environment_parameters.credentials = {
        "type": "PasswordCredential",
        "password": arguments["--update_ase_pw"],
    }

    try:
        environment.update(
            dlpx_obj.server_session,
            find_obj_by_name(
                dlpx_obj.server_session, environment, arguments["--env_name"], env_obj
            ).reference,
            env_obj,
        )

    except (HttpError, RequestError) as e:
        print_exception(
            "\nERROR: Updating the ASE DB password " "failed:\n{}\n".format(e)
        )


def create_linux_env(dlpx_obj, env_name, host_user, ip_addr, toolkit_path, pw=None):

    """
    Create a Linux environment.

    env_name: The name of the environment
    host_user: The server account used to authenticate
    ip_addr: DNS name or IP address of the environment
    toolkit_path: Path to the toolkit. Note: This directory must be
                  writable by the host_user
    pw: Password of the user. Default: None (use SSH keys instead)
    """
    engine_name = dlpx_obj.dlpx_engines.keys()[0]
    env_params_obj = HostEnvironmentCreateParameters()

    if pw is None:
        print_debug("Creating the environment with SSH Keys")
        env_params_obj.primary_user = {
            "type": "EnvironmentUser",
            "name": host_user,
            "credential": {"type": "SystemKeyCredential"},
        }

    else:
        print_debug("Creating the environment with a password")
        env_params_obj.primary_user = {
            "type": "EnvironmentUser",
            "name": host_user,
            "credential": {"type": "PasswordCredential", "password": pw},
        }

    env_params_obj.host_parameters = {
        "type": "UnixHostCreateParameters",
        "host": {
            "address": ip_addr,
            "type": "UnixHost",
            "name": env_name,
            "toolkitPath": toolkit_path,
        },
    }

    env_params_obj.host_environment = UnixHostEnvironment()
    env_params_obj.host_environment.name = env_name

    if arguments["--ase"]:
        env_params_obj.host_environment.ase_host_environment_parameters = (
            ASEHostEnvironmentParameters()
        )

        try:
            env_params_obj.host_environment.ase_host_environment_parameters.db_user = (
                arguments["--ase_user"]
            )
            env_params_obj.host_environment.ase_host_environment_parameters.credentials = {
                "type": "PasswordCredential",
                "password": arguments["--ase_pw"],
            }
        except KeyError:
            print_exception(
                "The --ase_user and --ase_pw arguments are"
                " required with the --ase flag.\n"
            )

    try:
        environment.create(dlpx_obj.server_session, env_params_obj)
        dlpx_obj.jobs[engine_name] = dlpx_obj.server_session.last_job

    except (DlpxException, RequestError, HttpError) as e:
        print(
            "\nERROR: Encountered an exception while creating the "
            "environment:\n{}".format(e)
        )
    except JobError as e:
        print_exception(
            "JobError while creating environment {}:\n{}".format(e, e.message)
        )


def create_windows_env(
    dlpx_obj, env_name, host_user, ip_addr, pw=None, connector_name=None
):

    """
    Create a Windows environment.

    env_name: The name of the environment
    host_user: The server account used to authenticate
    ip_addr: DNS name or IP address of the environment
    toolkit_path: Path to the toolkit. Note: This directory must be
                  writable by the host_user
    pw: Password of the user. Default: None (use SSH keys instead)
    """
    engine_name = dlpx_obj.dlpx_engines.keys()[0]

    env_params_obj = HostEnvironmentCreateParameters()

    print_debug("Creating the environment with a password")

    env_params_obj.primary_user = {
        "type": "EnvironmentUser",
        "name": host_user,
        "credential": {"type": "PasswordCredential", "password": pw},
    }

    env_params_obj.host_parameters = {
        "type": "WindowsHostCreateParameters",
        "host": {
            "address": ip_addr,
            "type": "WindowsHost",
            "name": env_name,
            "connectorPort": 9100,
        },
    }

    env_params_obj.host_environment = WindowsHostEnvironment()
    env_params_obj.host_environment.name = env_name

    if connector_name:
        env_obj = find_obj_by_name(dlpx_obj.server_session, environment, connector_name)

        if env_obj:
            env_params_obj.host_environment.proxy = env_obj.host
        elif env_obj is None:
            print(
                "Host was not found in the Engine: {}".format(
                    arguments[--connector_name]
                )
            )
            sys.exit(1)

    try:
        environment.create(dlpx_obj.server_session, env_params_obj)
        dlpx_obj.jobs[engine_name] = dlpx_obj.server_session.last_job

    except (DlpxException, RequestError, HttpError) as e:
        print(
            "\nERROR: Encountered an exception while creating the "
            "environment:\n{}".format(e)
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
def main_workflow(engine, dlpx_obj):
    """
    This function is where we create our main workflow.
    Use the @run_async decorator to run this function asynchronously.
    The @run_async decorator allows us to run against multiple Delphix Engine
    simultaneously

    :param engine: Dictionary of engines
    :type engine: dictionary
    :param dlpx_obj: Virtualization Engine session object
    :type dlpx_obj: lib.GetSession.GetSession
    """

    try:
        # Setup the connection to the Delphix Engine
        dlpx_obj.serversess(
            engine["ip_address"], engine["username"], engine["password"]
        )

    except DlpxException as e:
        print_exception(
            "ERROR: Engine {} encountered an error while"
            "{}:\n{}\n".format(engine["hostname"], arguments["--target"], e)
        )
        sys.exit(1)

    thingstodo = ["thingtodo"]
    try:
        with dlpx_obj.job_mode(single_thread):
            while len(dlpx_obj.jobs) > 0 or len(thingstodo) > 0:
                if len(thingstodo) > 0:
                    if (
                        arguments["--type"] == "linux"
                        or arguments["--type"] == "windows"
                    ):
                        env_name = arguments["--env_name"]
                        host_user = arguments["--host_user"]
                        pw = arguments["--pw"]
                        ip_addr = arguments["--ip"]
                        host_name = arguments["--connector_name"]
                        if arguments["--type"] == "linux":
                            toolkit_path = arguments["--toolkit"]
                            create_linux_env(
                                dlpx_obj, env_name, host_user, ip_addr, toolkit_path, pw
                            )
                        else:
                            create_windows_env(
                                dlpx_obj,
                                env_name,
                                host_user,
                                ip_addr,
                                pw,
                                host_name,
                            )

                    elif arguments["--delete"]:
                        delete_env(dlpx_obj, arguments["--delete"])

                    elif arguments["--refresh"]:
                        refresh_env(dlpx_obj, arguments["--refresh"])

                    elif arguments["--update_ase_pw"]:
                        update_ase_pw(dlpx_obj)

                    elif arguments["--update_ase_user"]:
                        update_ase_username(dlpx_obj)
                    elif arguments["--list"]:
                        list_env(dlpx_obj)
                    elif arguments["--update_host"]:
                        update_host_address(
                            dlpx_obj,
                            arguments["--old_host_address"],
                            arguments["--new_host_address"],
                        )
                    elif arguments["--enable"]:
                        enable_environment(dlpx_obj, arguments["--env_name"])
                    elif arguments["--disable"]:
                        disable_environment(dlpx_obj, arguments["--env_name"])

                    thingstodo.pop()
                # get all the jobs, then inspect them
                i = 0
                for j in dlpx_obj.jobs.keys():
                    job_obj = job.get(dlpx_obj.server_session, dlpx_obj.jobs[j])
                    print_debug(job_obj)
                    print_info(
                        "{} Environment: {}".format(
                            engine["hostname"], job_obj.job_state
                        )
                    )
                    if job_obj.job_state in ["CANCELED", "COMPLETED", "FAILED"]:
                        # If the job is in a non-running state, remove it
                        # from the running jobs list.
                        del dlpx_obj.jobs[j]
                    elif job_obj.job_state in "RUNNING":
                        # If the job is in a running state, increment the
                        # running job count.
                        i += 1
                    print_info("{}: {:d} jobs running.".format(engine["hostname"], i))
                    # If we have running jobs, pause before repeating the
                    # checks.
                    if len(dlpx_obj.jobs) > 0:
                        sleep(float(arguments["--poll"]))
    except (DlpxException, RequestError, JobError, HttpError) as e:
        print_exception(
            "Error while creating the environment {}\n{}".format(
                arguments["--env_name"], e
            )
        )
        sys.exit(1)


def run_job(dlpx_obj, config_file_path):
    """
    This function runs the main_workflow aynchronously against all the
    servers specified

    dlpx_obj: Virtualization Engine session object
    config_file_path: filename of the configuration file for virtualization
    engines
    """

    # Create an empty list to store threads we create.
    threads = []
    engine = None

    # If the --all argument was given, run against every engine in dxtools.conf
    if arguments["--all"]:
        print_info("Executing against all Delphix Engines in the dxtools.conf")

        try:
            # For each server in the dxtools.conf...
            for delphix_engine in dlpx_obj.dlpx_engines:
                engine = dlpx_obj.dlpx_engines[delphix_engine]
                # Create a new thread and add it to the list.
                threads.append(main_workflow(engine, dlpx_obj))

        except DlpxException as e:
            print("Error encountered in run_job():\n{}".format(e))
            sys.exit(1)

    elif arguments["--all"] is False:
        # Else if the --engine argument was given, test to see if the engine
        # exists in dxtools.conf
        if arguments["--engine"]:
            try:
                engine = dlpx_obj.dlpx_engines[arguments["--engine"]]
                print_info(
                    "Executing against Delphix Engine: {}\n".format(
                        arguments["--engine"]
                    )
                )
            except (DlpxException, RequestError, KeyError) as e:
                print_exception(
                    "\nERROR: Delphix Engine {} cannot be "
                    "found in {}. Please check your value "
                    "and try again. Exiting.\n".format(
                        arguments["--engine"], config_file_path
                    )
                )
        else:
            # Else search for a default engine in the dxtools.conf
            for delphix_engine in dlpx_obj.dlpx_engines:
                if dlpx_obj.dlpx_engines[delphix_engine]["default"] == "true":
                    engine = dlpx_obj.dlpx_engines[delphix_engine]
                    print_info(
                        "Executing against the default Delphix Engine "
                        "in the dxtools.conf: {}".format(
                            dlpx_obj.dlpx_engines[delphix_engine]["hostname"]
                        )
                    )
                break
            if engine is None:
                raise DlpxException("\nERROR: No default engine found. Exiting")

        # run the job against the engine
        threads.append(main_workflow(engine, dlpx_obj))
    # For each thread in the list...
    for each in threads:
        # join them back together so that we wait for all threads to complete
        # before moving on
        each.join()


def time_elapsed(time_start):
    """
    This function calculates the time elapsed since the beginning of the script.
    Call this anywhere you want to note the progress in terms of time

    :param time_start:  start time of the script.
    :type time_start: float
    """
    return round((time() - time_start) / 60, +1)


def main():
    # We want to be able to call on these variables anywhere in the script.
    global single_thread
    global debug

    time_start = time()
    single_thread = False

    try:
        dx_session_obj = GetSession()
        logging_est(arguments["--logdir"])
        print_debug(arguments)
        config_file_path = arguments["--config"]
        # Parse the dxtools.conf and put it into a dictionary
        dx_session_obj.get_config(config_file_path)

        # This is the function that will handle processing main_workflow for
        # all the servers.
        run_job(dx_session_obj, config_file_path)

        elapsed_minutes = time_elapsed(time_start)
        print_info(
            "script took {:.2f} minutes to get this far.".format(elapsed_minutes)
        )

    # Here we handle what we do when the unexpected happens
    except SystemExit as e:
        # This is what we use to handle our sys.exit(#)
        sys.exit(e)

    except DlpxException as e:
        # We use this exception handler when an error occurs in a function call.
        print_exception(
            "ERROR: Please check the ERROR message below:\n" "{}".format(e.message)
        )
        sys.exit(2)

    except HttpError as e:
        # We use this exception handler when our connection to Delphix fails
        print_exception(
            "ERROR: Connection failed to the Delphix Engine. Please"
            "check the ERROR message below:\n{}".format(e.message)
        )
        sys.exit(2)

    except JobError as e:
        # We use this exception handler when a job fails in Delphix so that we
        # have actionable data
        print_exception("A job failed in the Delphix Engine:\n{}".format(e.job))
        elapsed_minutes = time_elapsed(time_start)
        print_exception(
            "{} took {:.2f} minutes to get this far".format(
                basename(__file__), elapsed_minutes
            )
        )
        sys.exit(3)

    except KeyboardInterrupt:
        # We use this exception handler to gracefully handle ctrl+c exits
        print_debug("You sent a CTRL+C to interrupt the process")
        elapsed_minutes = time_elapsed(time_start)
        print_info(
            "{} took {:.2f} minutes to get this far".format(
                basename(__file__), elapsed_minutes
            )
        )
    except:
        # Everything else gets caught here
        print_exception("{}\n{}".format(sys.exc_info()[0], traceback.format_exc()))
        elapsed_minutes = time_elapsed(time_start)
        print_info(
            "{} took {:.2f} minutes to get this far".format(
                basename(__file__), elapsed_minutes
            )
        )
        sys.exit(1)


if __name__ == "__main__":
    # Grab our arguments from the doc at the top of the script
    arguments = docopt(__doc__, version=basename(__file__) + " " + VERSION)
    # Feed our arguments to the main function, and off we go!
    main()
