#!/usr/bin/env python3
# Corey Brune 08 2016
# This script creates an environment in Delphix
# The below doc follows the POSIX compliant standards and allows us to
# define our ARGUMENTS for the script.

"""Create Host Environment

Usage:
    dx_environment.py ( [--list] | [--create] | [--enable] | [--disable] | [--delete] | [--refresh] | [--update_host])
                    [--type <name>]
                    [--toolkit <toolkit>]
                    [--env_name <name>]
                    [--passwd <passwd>]
                    [--connector_name <name>]
                    [--ip <ipaddress>]
                    [--old_host_address <name> ]
                    [--new_host_address <name>]
 					[--single_thread <bool>]
                    [--host_user <hostuser>]
  					[--logdir <directory>]
  					[--config <filename>]
  					[--engine <identifier>]
  					[--poll <n>]
    dx_environment.py -h | --help | -v | --version

Create a Delphix environment.
- current support for standalone environments only
- does not support password updates.

Examples:
  dx_environment.py --list
  dx_environment.py --create --engine mymask --type Linux --env_name oratgt --host_user delphix --passwd xxxx --ip 10.0.1.30 --toolkit /home/delphix
  dx_environment.py --enable --engine mymask --env_name oratgt
  dx_environment.py --disable --engine mymask --env_name oratgt
  dx_environment.py --refresh --engine mymask --env_name oratgt
  dx_environment.py --delete --engine mymask --env_name oratgt
  dx_environment.py --update_host --engine mymask --old_host_address 10.0.1.20 --new_host_address 10.0.1.30



Options:
  --type <name>                     The OS type for the environment
  --env_name <name>                 The name of the Delphix environment
  --toolkit <path>                  Path of the toolkit. Required for Unix/Linux
  --connector_name <environment>    The name of the Delphix connector to use.
                                    Required for Windows source environments
  --ip <addr>                       The IP address of the Delphix environment
  --host_user <username>            The username on the Delphix environment
  --passwd <password>               Password of the user
  --ase_user <name>                 The ASE DB username
  --ase_pw <name>                   Password of the ASE DB user
  --update_ase_user <name>          Update the ASE DB username
  --update_ase_pw <name>            The new ASE DB password
  --old_host_address <name>         The current name of the host, as registered in
                                    Delphix. Required for update
  --new_host_address <name>         The desired name of the host, as registered in
                                    Delphix. Required for update
  --engine <name>                   Identifier of Delphix engine in dxtools.conf.
                                    [default: default]
  --poll <n>                        The number of seconds to wait between job polls
                                    [default: 10]
  --config <path_to_file>           The path to the dxtools.conf file
                                    [default: ./config/dxtools.conf]
  --logdir <path_to_file>           The path to the logfile you want to use.
                                    [default: ./logs/dx_environment.log]
  -h --help                         Show this screen.
  -v --version                      Show version.
  --single_thread <bool>            Run as a single thread? True or False
                                    [default: False]

"""

from os.path import basename
import sys
import time
import docopt

from delphixpy.v1_10_2 import exceptions
from delphixpy.v1_10_2.web import environment
from delphixpy.v1_10_2.web import host
from delphixpy.v1_10_2.web import vo

from lib import dlpx_exceptions
from lib import get_references
from lib import get_session
from lib import dx_logging
from lib import run_job
from lib.run_async import run_async

VERSION = 'v.0.3.616'


def enable_environment(dlpx_obj, env_name):
    """
    Enable the given host
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param env_name: Environment name in Delphix
    :type env_name: str
    """
    env_obj = get_references.find_obj_by_name(
        dlpx_obj.server_session, environment, env_name)
    try:
        environment.enable(dlpx_obj.server_session, env_obj.reference)
    except (dlpx_exceptions.DlpxException, exceptions.RequestError) as err:
        dx_logging.print_exception(f'ERROR: Enabling the host {env_name} '
                                   f'encountered an error:\n{err}')


def disable_environment(dlpx_obj, env_name):
    """
    Enable a Delphix environment
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param env_name: Environment name in Delphix
    :type env_name: str
    """
    env_obj = get_references.find_obj_by_name(
        dlpx_obj.server_session, environment, env_name)
    try:
        environment.disable(dlpx_obj.server_session, env_obj.reference)
    except (dlpx_exceptions.DlpxException, exceptions.RequestError) as err:
        dx_logging.print_exception(f'ERROR: Disabling the host {env_name} '
                                   f'encountered an error:\n{err}')


def update_host_address(dlpx_obj, old_host_address, new_host_address):
    """
    Update the environment
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param old_host_address: Original IP address of environment
    :type old_host_address: str
    :param new_host_address: New IP address of the environment
    :type new_host_address: str
    """
    old_host_obj = get_references.find_obj_by_name(
        dlpx_obj.server_session, host, old_host_address)
    if old_host_obj.type == 'WindowsHost':
        host_obj = vo.WindowsHost()
    else:
        host_obj = vo.UnixHost()
    host_obj.address = new_host_address
    try:
        host.update(dlpx_obj.server_session, old_host_obj.reference, host_obj)

    except (dlpx_exceptions.DlpxException, exceptions.RequestError) as err:
        dx_logging.print_exception(f'ERROR: Updating the host {host_obj.name} '
                                   f'encountered an error:\n{err}')


def list_env(dlpx_obj):
    """
    List all environments for the engine
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    """
    all_envs = environment.get_all(dlpx_obj.server_session)
    if not all_envs:
        dx_logging.print_info(f'There are no environments on engine:{dlpx_obj.server_session.address}')
        return
    env_host = ''
    for env in all_envs:
        env_user = get_references.find_obj_name(
            dlpx_obj.server_session, environment.user, env.primary_user)
        try:
            env_host = get_references.find_obj_name(dlpx_obj.server_session,
                                                    host, env.host)
        except AttributeError:
            pass
        if env.type == 'WindowsHostEnvironment':
            print(f'Environment Name: {env.name}, Username: {env_user}, '
                  f'Host: {env_host},Enabled: {env.enabled}')
        elif env.type == 'WindowsCluster' or env.type == 'OracleCluster':
            print(f'Environment Name: {env.name}, Username: {env_user}'
                  f'Enabled: {env.enabled}, ')
        else:
            print(f'Environment Name: {env.name}, Username: {env_user}, '
                  f'Host: {env_host}, Enabled: {env.enabled}, '
                  f'ASE Environment Params: '
                  f'{env.ase_host_environment_parameters if isinstance(env.ase_host_environment_parameters,vo.ASEHostEnvironmentParameters) else "Undefined"}')


def delete_env(dlpx_obj, env_name):
    """
    Deletes an environment
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param env_name: Name of the environment to delete
    :type env_name: str
    """
    env_obj = get_references.find_obj_by_name(dlpx_obj.server_session,
                                              environment, env_name)
    if env_obj:
        environment.delete(dlpx_obj.server_session, env_obj.reference)
        dlpx_obj.jobs[
            dlpx_obj.server_session.address
        ] = dlpx_obj.server_session.last_job
    elif env_obj is None:
        dlpx_exceptions.DlpxObjectNotFound(
            f'Environment was not found: {env_name}')


def refresh_env(dlpx_obj, env_name):
    """
    Refresh the environment
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :parm env_name: Name of the environment to refresh
    :type env_name: str
    """
    if env_name == "all":
        env_list = get_references.find_all_objects(
            dlpx_obj.server_session, environment)
        for env_obj in env_list:
            try:
                environment.refresh(dlpx_obj.server_session, env_obj.reference)
                dlpx_obj.jobs[
                    dlpx_obj.server_session.address
                ] = dlpx_obj.server_session.last_job
            except (dlpx_exceptions.DlpxException,
                    exceptions.RequestError) as err:
                dlpx_exceptions.DlpxException(
                    f'Encountered an error while refreshing {env_name}: {err}')
    else:
        try:
            env_obj = get_references.find_obj_by_name(
                dlpx_obj.server_session, environment, env_name)
            environment.refresh(dlpx_obj.server_session, env_obj.reference)
            dlpx_obj.jobs[
                dlpx_obj.server_session.address
            ] = dlpx_obj.server_session.last_job
        except (dlpx_exceptions.DlpxException, exceptions.RequestError) as err:
            raise dlpx_exceptions.DlpxException(
                f'Refreshing {env_name} encountered an error:\n{err}')


def create_linux_env(dlpx_obj, env_name, host_user, ip_addr, toolkit_path,
                     passwd=None, ase_user=None, ase_pw=None):
    """
    Create a Linux environment.
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param env_name: The name of the environment
    :type env_name: str
    :param host_user: The server account used to authenticate
    :type host_user: str
    :param ip_addr: DNS name or IP address of the environment
    :type ip_addr: str
    :param toolkit_path: Path to the toolkit. Note: This directory must be
    writable by the host_user
    :type toolkit_path: str
    :param passwd: Password of the user. Default: None (use SSH keys instead)
    :type passwd: str or bool
    :param ase_user: username for ASE DB
    :type ase_user: str
    :param ase_pw: password for the ASE DB user
    :type ase_pw: str
    """

    env_params_obj = vo.HostEnvironmentCreateParameters()
    # setting host env params
    env_params_obj.host_environment = vo.UnixHostEnvironment()
    env_params_obj.host_environment.name = env_name
    # setting host creation params
    env_params_obj.host_parameters = vo.UnixHostCreateParameters()
    env_params_obj.host_parameters.host = vo.UnixHost()
    env_params_obj.host_parameters.host.address = ip_addr
    env_params_obj.host_parameters.name = env_name
    env_params_obj.host_parameters.host.toolkit_path = toolkit_path
    # setting user credentials
    env_params_obj.primary_user = vo.EnvironmentUser()
    env_params_obj.primary_user.name = host_user
    if passwd is None:
        env_params_obj.primary_user.credential = vo.SystemKeyCredential()
    else:
        env_params_obj.primary_user.credential = vo.PasswordCredential()
        env_params_obj.primary_user.credential.password = passwd
    if ase_user:
        env_params_obj.host_environment.ase_host_environment_parameters = \
            vo.ASEHostEnvironmentParameters()
        env_params_obj.host_environment.ase_host_environment_parameters.db_user = \
            ase_user
        env_params_obj.host_environment.ase_host_environment_parameters.credentials = \
            vo.PasswordCredential()
        env_params_obj.host_environment.ase_host_environment_parameters.credentials.password = \
            ase_pw
    try:
        environment.create(dlpx_obj.server_session, env_params_obj)
        dlpx_obj.jobs[
            dlpx_obj.server_session.address
        ] = dlpx_obj.server_session.last_job
    except (dlpx_exceptions.DlpxException, exceptions.RequestError,
            exceptions.HttpError) as err:
        raise dlpx_exceptions.DlpxException(
            f'ERROR: Encountered an exception while creating the '
            f'environment:\n{err}')
    except exceptions.JobError as err:
        raise dlpx_exceptions.DlpxException(
            f'JobError while creating environment:\n{err}') from err


def create_windows_env(dlpx_obj, env_name, host_user, ip_addr, passwd=None,
                       connector_name=None):
    """
    Create a Windows environment.
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param env_name: The name of the environment
    :type env_name: str
    :param host_user: The server account used to authenticate
    :type host_user: str
    :param ip_addr: DNS name or IP address of the environment
    :type ip_addr: str
    :param passwd: Password of the user. Default: None (use SSH keys instead)
    :type passwd: str
    :param connector_name: Name of the Delphix connector
    :type connector_name: str
    """
    env_params_obj = vo.HostEnvironmentCreateParameters()
    env_params_obj.primary_user = vo.EnvironmentUser()
    env_params_obj.primary_user.name = host_user
    env_params_obj.primary_user.credential = vo.PasswordCredential()
    env_params_obj.primary_user.credential.password = passwd
    env_params_obj.host_parameters = vo.WindowsHostCreateParameters()
    env_params_obj.host_parameters.host = vo.WindowsHost()
    env_params_obj.host_parameters.name = env_name
    env_params_obj.host_parameters.host.connector_port = 9100
    env_params_obj.host_parameters.host.address = ip_addr
    env_params_obj.host_environment = vo.WindowsHostEnvironment()
    env_params_obj.host_environment.name = env_name
    env_obj = None
    if connector_name:
        env_obj = get_references.find_obj_by_name(
            dlpx_obj.server_session, environment, connector_name)
    if env_obj:
       env_params_obj.host_environment.proxy = env_obj.host

    elif env_obj is None:
        raise dlpx_exceptions.DlpxObjectNotFound(
            f'Host was not found in the Engine: {connector_name}')
    try:
        environment.create(dlpx_obj.server_session, env_params_obj)
        dlpx_obj.jobs[
            dlpx_obj.server_session.address
        ] = dlpx_obj.server_session.last_job
    except (dlpx_exceptions.DlpxException, exceptions.RequestError,
            exceptions.HttpError) as err:
        raise dlpx_exceptions.DlpxException(
            f'ERROR: Encountered an exception while creating the '
            f'environment:\n{err}')


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
            f'ERROR: dx_environment encountered an error authenticating to '
            f' {engine["ip_address"]} :\n{err}')
    try:
        with dlpx_obj.job_mode(single_thread):
            if ARGUMENTS['--list']:
                list_env(dlpx_obj)
            elif ARGUMENTS['--create']:
                env_name = ARGUMENTS['--env_name']
                host_user = ARGUMENTS['--host_user']
                passwd = ARGUMENTS['--passwd']
                ip_addr = ARGUMENTS['--ip']
                type = ARGUMENTS['--type']
                toolkit_path = ARGUMENTS['--toolkit']
                if type is None:
                    raise dlpx_exceptions.DlpxException(
                        '--type parameter is required for environment creation')

                type = type.lower()
                if type == 'windows':
                    host_name = ARGUMENTS['--connector_name']
                    create_windows_env(dlpx_obj, env_name, host_user,
                                       ip_addr, passwd, host_name)
                elif type == 'linux':
                    if toolkit_path is None:
                        raise dlpx_exceptions.DlpxException(
                            '--toolkit parameter is required for environment '
                            'creation')
                    create_linux_env(dlpx_obj, env_name, host_user,
                                     ip_addr, toolkit_path, passwd)
            elif ARGUMENTS['--enable']:
                enable_environment(dlpx_obj, ARGUMENTS['--env_name'])
            elif ARGUMENTS['--disable']:
                disable_environment(dlpx_obj, ARGUMENTS['--env_name'])
            elif ARGUMENTS['--delete']:
                delete_env(dlpx_obj, ARGUMENTS['--env_name'])
            elif ARGUMENTS['--refresh']:
                refresh_env(dlpx_obj, ARGUMENTS['--env_name'])
            elif ARGUMENTS['--update_host']:
                update_host_address(
                    dlpx_obj, ARGUMENTS['--old_host_address'],
                    ARGUMENTS['--new_host_address'])
            run_job.find_job_state_by_jobid(engine, dlpx_obj.job_id, 1)
    except (dlpx_exceptions.DlpxException, exceptions.RequestError,
            exceptions.JobError, exceptions.HttpError) as err:
        dx_logging.print_exception(f'Error in dx_environment for engine:'
                                   f'{engine["ip_address"]}: Error Message: {err}')


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
        for each in run_job.run_job(main_workflow, dx_session_obj, engine,
                                    single_thread):
            each.join()
        elapsed_minutes = run_job.time_elapsed(time_start)
        dx_logging.print_info(f'de_environment took {elapsed_minutes} minutes to '
                              f'complete.')
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
            f'{basename(__file__)} took {elapsed_minutes} minutes to '
            f'complete')
        sys.exit(3)

    except KeyboardInterrupt:
        # We use this exception handler to gracefully handle ctrl+c exits
        dx_logging.print_debug('You sent a CTRL+C to interrupt the process')
        elapsed_minutes = run_job.time_elapsed(time_start)
        dx_logging.print_info(f'{basename(__file__)} took {elapsed_minutes} '
                              f'minutes to complete.')


if __name__ == "__main__":
    # Grab our ARGUMENTS from the doc at the top of the script
    ARGUMENTS = docopt.docopt(__doc__,
                              version=basename(__file__) + " " + VERSION)
    # Feed our ARGUMENTS to the main function, and off we go!
    main()
