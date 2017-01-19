#!/usr/bin/env python
# Corey Brune - Oct 2016
#Creates an authorization object
#requirements
#pip install docopt delphixpy

#The below doc follows the POSIX compliant standards and allows us to use
#this doc to also define our arguments for the script.
"""List, create or remove authorizations for a Virtualization Engine
Usage:
  dx_authorization.py --create --role <name> --target_type <name> --target <name> --user <name> | --list
                  [--engine <identifier> | --all]
                  [--debug] [--parallel <n>] [--poll <n>]
                  [--config <path_to_file>] [--logdir <path_to_file>]
  dx_authorization.py -h | --help | -v | --version
List and create authentication objects

Examples:
  dx_authorization.py --engine landsharkengine --create --role Data --user dev_user --target_type database --target test_vdb
  dx_authorization.py --engine landsharkengine --create --role Data --user dev_user --target_type group --target Sources
  dx_authorization.py --list

Options:
  --create                  Create an authorization
  --role <name>             Role for authorization. Valid Roles are Data,
                             Read, Jet Stream User, OWNER, PROVISIONER
  --target <name>           Target object for authorization
  --target_type <name>      Target type. Valid target types are snapshot,
                             group, database
  --user <name>             User for the authorization
  --list                    List all authorizations
  --engine <type>           Alt Identifier of Delphix engine in dxtools.conf.
  --all                     Run against all engines.
  --debug                   Enable debug logging
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./dxtools.conf]
  --logdir <path_to_file>    The path to the logfile you want to use.
                            [default: ./dx_authorization.log]
  -h --help                 Show this screen.
  -v --version              Show version.
"""

VERSION = 'v.0.0.001'

from docopt import docopt
import logging
from os.path import basename
import sys
import time
import traceback
import json
from multiprocessing import Process
from time import sleep, time

from delphixpy.delphix_engine import DelphixEngine
from delphixpy.exceptions import JobError
from delphixpy.exceptions import RequestError
from delphixpy.exceptions import HttpError
from delphixpy import job_context
from delphixpy.web import database
from delphixpy.web import source
from delphixpy.web import host
from delphixpy.web import job
from delphixpy.web import role
from delphixpy.web import authorization
from delphixpy.web import user
from delphixpy.web import snapshot
from delphixpy.web import group
from delphixpy.web.vo import User
from delphixpy.web.vo import Authorization

from lib.DlpxException import DlpxException
from lib.GetSession import GetSession
from lib.GetReferences import find_obj_by_name
from lib.DxLogging import logging_est
from lib.DxLogging import print_info
from lib.DxLogging import print_debug


def create_authorization(role_name, target_type, target_name, user_name):
    """
    Function to start, stop, enable or disable a VDB

    role_name: Name of the role
    target_type: Supports snapshot, group and database target types
    target_name: Name of the target
    user_name: User for the authorization
    """
    target_obj = None
    authorization_obj = Authorization()

    print_debug('Searching for %s, %s and %s references.\n' %
                (role_name, target_name, user_name))

    role_obj = find_obj_by_name(dx_session_obj.server_session, role,
                                role_name)

    if target_type.lower() == 'group':
        target_obj = find_obj_by_name(dx_session_obj.server_session,
                                      group, target_name)
    elif target_type.lower() == 'database':
        target_obj = find_obj_by_name(dx_session_obj.server_session,
                                      database, target_name)
    elif target_type.lower() == 'snapshot':
        target_obj = find_obj_by_name(dx_session_obj.server_session,
                                      snapshot, target_name)

    if not target_obj:
        raise DlpxException('Could not find target type %s' % (target_type))

    user_obj = find_obj_by_name(dx_session_obj.server_session, user,
                                user_name)

    try:
        authorization_obj.role = role_obj.reference
        authorization_obj.target = target_obj.reference
        authorization_obj.user = user_obj.reference

        authorization.create(dx_session_obj.server_session,
                             authorization_obj)

    except (RequestError, HttpError, JobError, AttributeError) as e:
        raise DlpxException('An error occurred while creating an '
                            'authorization on %s.:%s\n' % (target_name, e))

def list_authorization(username=None):
    """
    Function to list authorizations for a given engine

    username: Filter list results by user
    """

    print 'USER,\t ROLE,\t TARGET'

    try:
        auth_objs = authorization.get_all(dx_session_obj.server_session)

        for auth_obj in auth_objs:
             role_obj = role.get(dx_session_obj.server_session, auth_obj.role)
             user_obj = user.get(dx_session_obj.server_session, auth_obj.user)

             if auth_obj.target.startswith('USER'):
                 target_obj = user.get(dx_session_obj.server_session,
                                       auth_obj.target)

             elif auth_obj.target.startswith('GROUP'):
                 target_obj = group.get(dx_session_obj.server_session,
                                       auth_obj.target)

             elif auth_obj.target.startswith('DOMAIN'):
                 target_obj = User()
                 target_obj.name = 'DOMAIN'

             print '%s, %s, %s' % (user_obj.name, role_obj.name,
                                   target_obj.name)

    except (RequestError, HttpError, JobError, AttributeError) as e:
        print('An error occurred while listing authorizations.:\n%s\n' %
              (e))


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
        func_hl = Thread(target = func, args = args, kwargs = kwargs)
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
        #Setup the connection to the Delphix Engine
        dx_session_obj.serversess(engine['ip_address'], engine['username'],
                                  engine['password'])

    except DlpxException as e:
        print('\nERROR: Engine %s encountered an error' 
              '%s\n' % (dx_session_obj.engine['hostname'], e))
        sys.exit(1)

    thingstodo = ["thingtodo"]
    #reset the running job count before we begin
    i = 0
    with dx_session_obj.job_mode(single_thread):
        while (len(jobs) > 0 or len(thingstodo)> 0):
            if len(thingstodo)> 0:

                try:
                    if arguments['--create']:
                        create_authorization(arguments['--role'],
                                             arguments['--target_type'],
                                             arguments['--target'],
                                             arguments['--user'])

                    elif arguments['--list']:
                        list_authorization()

                except DlpxException as e:
                    print('\nERROR: Encountered an error with ' 
                          'authorizations:\n%s\n'% (e))
                    sys.exit(1)

                thingstodo.pop()

            #get all the jobs, then inspect them
            i = 0
            for j in jobs.keys():
                job_obj = job.get(server, jobs[j])
                print_debug(job_obj)
                print_info(engine["hostname"] + ": VDB Operations: " +
                           job_obj.job_state)

                if job_obj.job_state in ["CANCELED", "COMPLETED", "FAILED"]:
                    #If the job is in a non-running state, remove it from the
                    # running jobs list.
                    del jobs[j]
                else:
                    #If the job is in a running state, increment the running
                    # job count.
                    i += 1

            print_info(engine["hostname"] + ": " + str(i) + " jobs running. ")
            #If we have running jobs, pause before repeating the checks.
            if len(jobs) > 0:
                sleep(float(arguments['--poll']))


def run_job():
    """
    This function runs the main_workflow aynchronously against all the 
    servers specified
    """
    #Create an empty list to store threads we create.
    threads = []

    #If the --all argument was given, run against every engine in dxtools.conf
    if arguments['--all']:
        print_info("Executing against all Delphix Engines in the dxtools.conf")

        try:
            #For each server in the dxtools.conf...
            for delphix_engine in dx_session_obj.dlpx_engines:
                engine = dx_session_obj[delphix_engine]
                #Create a new thread and add it to the list.
                threads.append(main_workflow(engine))

        except DlpxException as e:
            print 'Error encountered in run_job():\n%s' % (e)
            sys.exit(1)

    elif arguments['--all'] is False:
        #Else if the --engine argument was given, test to see if the engine
        # exists in dxtools.conf
      if arguments['--engine']:
            try:
                engine = dx_session_obj.dlpx_engines[arguments['--engine']]
                print_info('Executing against Delphix Engine: %s\n' %
                           (arguments['--engine']))

            except (DlpxException, RequestError, KeyError) as e:
                raise DlpxException('\nERROR: Delphix Engine %s cannot be '                                         'found in %s. Please check your value '
                                    'and try again. Exiting.\n' % (
                                    arguments['--engine'], config_file_path))

      else:
          #Else search for a default engine in the dxtools.conf
          for delphix_engine in dx_session_obj.dlpx_engines:
              if dx_session_obj.dlpx_engines[delphix_engine]['default'] == \
                 'true':

                  engine = dx_session_obj.dlpx_engines[delphix_engine]
                  print_info('Executing against the default Delphix Engine '
                       'in the dxtools.conf: %s' % (
                       dx_session_obj.dlpx_engines[delphix_engine]['hostname']))

              break

          if engine == None:
              raise DlpxException("\nERROR: No default engine found. Exiting")

    #run the job against the engine
    threads.append(main_workflow(engine))

    #For each thread in the list...
    for each in threads:
        #join them back together so that we wait for all threads to complete
        # before moving on
        each.join()


def time_elapsed():
    """
    This function calculates the time elapsed since the beginning of the script.
    Call this anywhere you want to note the progress in terms of time
    """
    elapsed_minutes = round((time() - time_start)/60, +1)
    return elapsed_minutes


def main(argv):
    #We want to be able to call on these variables anywhere in the script.
    global single_thread
    global usebackup
    global time_start
    global config_file_path
    global database_name
    global dx_session_obj
    global debug

    if arguments['--debug']:
        debug = True

#    import pdb;pdb.set_trace()
    try:
        dx_session_obj = GetSession()
        logging_est(arguments['--logdir'])
        print_debug(arguments)
        time_start = time()
        engine = None
        single_thread = False
        config_file_path = arguments['--config']
        #Parse the dxtools.conf and put it into a dictionary
        dx_session_obj.get_config(config_file_path)

        #This is the function that will handle processing main_workflow for
        # all the servers.
        run_job()

        elapsed_minutes = time_elapsed()
        print_info("script took " + str(elapsed_minutes) +
                   " minutes to get this far.")

    #Here we handle what we do when the unexpected happens
    except SystemExit as e:
        """
        This is what we use to handle our sys.exit(#)
        """
        sys.exit(e)

    except HttpError as e:
        """
        We use this exception handler when our connection to Delphix fails
        """
        print('Connection failed to the Delphix Engine Please check the '
              'ERROR message below')
        sys.exit(1)

    except JobError as e:
        """
        We use this exception handler when a job fails in Delphix so that
        we have actionable data
        """
        elapsed_minutes = time_elapsed()
        print('A job failed in the Delphix Engine')
        print_info('%s took %s minutes to get this far\n' %
                   (basename(__file__), str(elapsed_minutes)))
        sys.exit(3)

    except KeyboardInterrupt:
        """
        We use this exception handler to gracefully handle ctrl+c exits
        """
        print_debug("You sent a CTRL+C to interrupt the process")
        elapsed_minutes = time_elapsed()
        print_info('%s took %s minutes to get this far\n' %
                   (basename(__file__), str(elapsed_minutes)))

    except:
        """
        Everything else gets caught here
        """
        print(sys.exc_info()[0])
        elapsed_minutes = time_elapsed()
        print_info('%s took %s minutes to get this far\n' %
                   (basename(__file__), str(elapsed_minutes)))
        sys.exit(1)

if __name__ == "__main__":
    #Grab our arguments from the doc at the top of the script
    arguments = docopt(__doc__, version=basename(__file__) + " " + VERSION)
    #Feed our arguments to the main function, and off we go!
    main(arguments)
