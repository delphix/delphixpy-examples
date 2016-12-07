#!/usr/bin/env python
# Program Name : js_branch.py
# Description  : Delphix implementation script
# Author       : Corey Brune
# Created: March 4 2016
#
# Copyright (c) 2016 by Delphix.
# All rights reserved.
# See http://docs.delphix.com/display/PS/Copyright+Statement for details
#
# Delphix Support statement available at
# See http://docs.delphix.com/display/PS/PS+Script+Support+Policy for details
#
# Warranty details provided in external file
# for customers who have purchased support.
#
"""Creates, updates, deletes, activates and lists branches
Usage:
  js_branch.py (--create_branch <name> --container_name <name> --template_name <name>| --list_branches | --delete_branch <name> | --activate_branch <name> | --update_branch <name>)
                   [--engine <identifier> | --all] [--parallel <n>]
                   [--poll <n>] [--debug]
                   [--config <path_to_file>] [--logdir <path_to_file>]
  js_branch.py -h | --help | -v | --version

Creates, Lists, Removes a Jet Stream Branch

Examples:
  js_branch.py --list_branches
  js_branch.py --create_branch jsbranch1 --container_name jscontainer --template_name jstemplate1
  js_branch.py --activate_branch jsbranch1
  js_branch.py --delete_branch jsbranch1
  js_branch.py --update_branch jsbranch1

Options:
  --create_branch <name>    Name of the new JS Branch
  --container_name <name>   Name of the container to use
  --update_branch <name>    Name of the branch to update
  --template_name <name>    Name of the template to use
  --activate_branch <name>  Name of the branch to activate
  --delete_branch <name>    Delete the JS Branch
  --list_branches           List the branchs on a given engine
  --engine <type>           Alt Identifier of Delphix engine in dxtools.conf.
  --all                     Run against all engines.
  --debug                   Enable debug logging
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./dxtools.conf]
  --logdir <path_to_file>   The path to the logfile you want to use.
                            [default: ./js_branch.log]
  -h --help                 Show this screen.
  -v --version              Show version.
"""

VERSION="v.0.0.002"

from docopt import docopt
import logging
from os.path import basename
import signal
import sys
import time
import traceback
import json
import re
from multiprocessing import Process
from time import sleep
from time import time

from delphixpy.delphix_engine import DelphixEngine
from delphixpy.web.jetstream import branch
from delphixpy.web.jetstream import container
from delphixpy.web.jetstream import template
from delphixpy.web import database
from delphixpy.web.vo import JSBranchCreateParameters
from delphixpy.web.vo import JSDataSourceCreateParameters
from delphixpy.web.vo import JSBranch
from delphixpy.exceptions import RequestError
from delphixpy.exceptions import JobError
from delphixpy.exceptions import HttpError

from lib.DxTimeflow import DxTimeflow
from lib.DlpxException import DlpxException
from lib.GetSession import GetSession
from lib.GetReferences import find_obj_by_name
from lib.GetReferences import convert_timestamp
from lib.DxLogging import logging_est
from lib.DxLogging import print_info
from lib.DxLogging import print_debug


def create_branch(branch_name, template_name, container_name):
    """
    Create the JS Branch

    branch_name: Name of the branch to create
    template_name: Name of the template to use
    container_name: Name of the container to use
    """

    js_branch_params = JSBranchCreateParameters()

    try:
        data_container_obj = find_obj_by_name(dx_session_obj.server_session,
                                              container, container_name)

        source_layout_obj = find_obj_by_name(dx_session_obj.server_session,
                                             template, template_name)

        js_branch_params.name = branch_name
        js_branch_params.data_container = data_container_obj.reference
        js_branch_params.timeline_point_parameters = {
                                              'sourceDataLayout':
                                              source_layout_obj.reference,
                                              'type':
                                              'JSTimelinePointLatestTimeInput'}
            
        branch.create(dx_session_obj.server_session, js_branch_params)

    except (DlpxException, RequestError, HttpError) as e:
        print('\nThe branch %s was not created. The error was:\n\n%s' %
              (branch_name, e))
        sys.exit(1)


def list_branches():
    """
    List all branches on a given engine

    No args required
    """

    try:
        header = '\nName\tReference\tJSBranch Name'
        js_branches = branch.get_all(dx_session_obj.server_session)
            
        print header
        for js_branch in js_branches:
            print('%s, %s, %s' % (js_branch.name, js_branch.reference,
                                    js_branch._name[0]))
        print '\n'

    except (DlpxException, HttpError, RequestError) as e:
        raise DlpxException('\nERROR: JS Branches could not be listed. The '
                            'error was:\n\n%s' % (e))


def update_branch(branch_name):
    """
    Updates a branch

    branch_name: Name of the branch to update
    """

    js_branch_obj = JSBranch()
    
    try:
        branch_obj = find_obj_by_name(dx_session_obj.server_session,
                                      branch, branch_name)
        branch.update(dx_session_obj.server_session, branch_obj.reference,
                      js_branch_obj)

    except (DlpxException, HttpError, RequestError) as e:
        print('\nERROR: The branch %s could not be updated. The error was'
              ':\n\n%s' % (branch_name, e))


def activate_branch(branch_name):
    """
    Activates a branch

    branch_name: Name of the branch to activate
    """

    try:
        branch_obj = find_obj_by_name(dx_session_obj.server_session,
                                      branch, branch_name)

        branch.activate(dx_session_obj.server_session, branch_obj.reference)

    except RequestError as e:
        print('\nAn error occurred updating the branch:\n%s' % (e))
        sys.exit(1)


def delete_branch(branch_name):
    """
    Deletes a branch

    branch_name: Branch to delete
    """

    try:
        branch_obj = find_obj_by_name(dx_session_obj.server_session,
                                      branch, branch_name)

        branch.delete(dx_session_obj.server_session, branch_obj.reference)

    except (DlpxException, HttpError, RequestError) as e:
        raise DlpxException('\nERROR: The branch %s was not deleted. The '
                            'error was:\n\n%s' % (branch_name, e.message))


def build_ds_params(engine, obj, db):
    """
    Builds the datasource parameters

    engine: Dictionary of engines
    obj: object type to use when finding db
    db: Name of the database to use when building the parameters
    """

    try:
        db_obj = find_obj_by_name(dx_session_obj.server_session,
                                  obj, db)

        ds_params = JSDataSourceCreateParameters()
        ds_params.source = {'type':'JSDataSource', 'name': db}
        ds_params.container = db_obj.reference
        return(ds_params)

    except RequestError as e:
        print('\nCould not find %s\n%s' % (db, e.message))
        sys.exit(1)


def updateJSObject(obj_name, obj_type, vo_object, err_message):
    try:
        obj_ref = find_obj_by_name(dx_session_obj.server_session,
                                   obj_type, obj_name)
        obj_type.update(engine, obj_ref, vo_object)
        print '%s was updated successfully.\n' % (obj_name)


    except (DlpxException, HttpError, RequestError) as e:
        print('\nERROR: An error occurred while updating branch %s:\n%s' %
              (engine['hostname'], e))
        sys.exit(1)


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


def time_elapsed():
    """
    This function calculates the time elapsed since the beginning of the script.
    Call this anywhere you want to note the progress in terms of time
    """
    elapsed_minutes = round((time() - time_start)/60, +1)
    return elapsed_minutes


@run_async
def main_workflow(engine):
    """
    This function is where we create our main workflow.
    Use the @run_async decorator to run this function asynchronously.
    The @run_async decorator allows us to run against multiple Delphix Engine
    simultaneously

    engine: Dictionary of engines
    """

    #Establish these variables as empty for use later
    environment_obj = None
    source_objs = None

    try:
        #Setup the connection to the Delphix Engine
        dx_session_obj.serversess(engine['ip_address'], engine['username'],
                                  engine['password'])

    except DlpxException as e:
        print('\nERROR: Engine %s encountered an error while provisioning '
              '%s:\n%s\n' % (dx_session_obj.engine['hostname'],
              arguments['--target'], e))
        sys.exit(1)

    thingstodo = ["thingtodo"]

    try:
        with dx_session_obj.job_mode(single_thread):
            while len(thingstodo)> 0:
            #while (len(dx_session_obj.jobs) > 0 or len(thingstodo)> 0):
                if len(thingstodo) > 0:

                    if arguments['--create_branch']:
                        create_branch(arguments['--create_branch'],
                                        arguments['--template_name'],
                                        arguments['--container_name'])

                    elif arguments['--delete_branch']:
                        delete_branch(arguments['--delete_branch'])

                    elif arguments['--update_branch']:
                        update_branch(arguments['--update_branch'])

                    elif arguments['--activate_branch']:
                        activate_branch(arguments['--activate_branch'])

                    elif arguments['--list_branches']:
                        list_branches()

                    thingstodo.pop()

    except (DlpxException, RequestError, JobError, HttpError) as e:
        print('\nError in js_branch: %s:\n%s' %
              (engine['hostname'], e))
        sys.exit(1)


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


def main(argv):
    #We want to be able to call on these variables anywhere in the script.
    global single_thread
    global usebackup
    global time_start
    global database_name
    global config_file_path
    global dx_session_obj
    global debug

    try:
        dx_session_obj = GetSession()
        logging_est(arguments['--logdir'])
        print_debug(arguments)
        time_start = time()
        config_file_path = arguments['--config']


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

    except DlpxException as e:
        """
        We use this exception handler when an error occurs in a function call.
        """

        print('\nERROR: Please check the ERROR message below:\n%s' %
              (e.message))
        sys.exit(2)

    except HttpError as e:
        """
        We use this exception handler when our connection to Delphix fails
        """
        print('\nERROR: Connection failed to the Delphix Engine. Please '
              'check the ERROR message below:\n%s' % (e.message))
        sys.exit(2)

    except JobError as e:
        """
        We use this exception handler when a job fails in Delphix so that we
        have actionable data
        """
        print('A job failed in the Delphix Engine:\n%s' % (e.job))
        elapsed_minutes = time_elapsed()
        print_info(basename(__file__) + " took " + str(elapsed_minutes) +
                   " minutes to get this far.")
        sys.exit(3)

    except KeyboardInterrupt:
        """
        We use this exception handler to gracefully handle ctrl+c exits
        """
        print_debug("You sent a CTRL+C to interrupt the process")
        elapsed_minutes = time_elapsed()
        print_info(basename(__file__) + " took " + str(elapsed_minutes) +
                   " minutes to get this far.")

    except:
        """
        Everything else gets caught here
        """
        print(sys.exc_info()[0])
        print(traceback.format_exc())
        elapsed_minutes = time_elapsed()
        print_info('%s took %s minutes to get this far' % (basename(__file__),
                   str(elapsed_minutes)))
        sys.exit(1)


if __name__ == "__main__":
    #Grab our arguments from the doc at the top of the script
    arguments = docopt(__doc__, version=basename(__file__) + " " + VERSION)

    #Feed our arguments to the main function, and off we go!
    main(arguments)
