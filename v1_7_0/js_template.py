#!/usr/bin/env python
# Program Name : js_template.py
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
"""Creates, deletes and lists JS templates.
Usage:
  js_template.py (--create_template <name> --database <name> | --list_templates | --delete_template <name>)
                   [--engine <identifier> | --all] [--parallel <n>]
                   [--poll <n>] [--debug]
                   [--config <path_to_file>] [--logdir <path_to_file>]
  js_template.py -h | --help | -v | --version

Creates, Lists, Removes a Jet Stream Template

Examples:
  js_template.py --list_templates
  js_template.py --create_template jstemplate1 --database <name>
  js_template.py --create_template jstemplate2 --database <name:name:name>
  js_template.py --delete_template jstemplate1

Options:
  --create_template <name>  Name of the new JS Template
  --delete_template <name>  Delete the JS Template
  --database <name>         Name of the database(s) to use for the JS Template
                                Note: If adding multiple template DBs, use a
                                comma (:) to delineate between the DB names.
  --list_templates          List the templates on a given engine
  --engine <type>           Alt Identifier of Delphix engine in dxtools.conf.
  --all                     Run against all engines.
  --debug                   Enable debug logging
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./dxtools.conf]
  --logdir <path_to_file>   The path to the logfile you want to use.
                            [default: ./js_template.log]
  -h --help                 Show this screen.
  -v --version              Show version.
"""

VERSION="v.0.0.001"

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

from delphixpy.v1_7_0.delphix_engine import DelphixEngine
from delphixpy.v1_7_0.web.jetstream import template
from delphixpy.v1_7_0.web import database
from delphixpy.v1_7_0.web.vo import JSDataTemplateCreateParameters
from delphixpy.v1_7_0.web.vo import JSDataSourceCreateParameters
from delphixpy.v1_7_0.exceptions import RequestError
from delphixpy.v1_7_0.exceptions import JobError
from delphixpy.v1_7_0.exceptions import HttpError

from lib.DxTimeflow import DxTimeflow
from lib.DlpxException import DlpxException
from lib.GetSession import GetSession
from lib.GetReferences import find_obj_by_name
from lib.GetReferences import convert_timestamp
from lib.DxLogging import logging_est
from lib.DxLogging import print_info
from lib.DxLogging import print_debug


def create_template(template_name, database_name):
    """
    Create the JS Template

    template_name: Name of the template to create
    database_name: Name of the database(s) to use in the template
    """

    js_template_params = JSDataTemplateCreateParameters()

    template_ds_lst = []
    for db in database_name.split(':'):
            template_ds_lst.append(build_ds_params(database, db))

    try:
        js_template_params.data_sources = template_ds_lst
        js_template_params.name = template_name
        js_template_params.type = 'JSDataTemplateCreateParameters'

        template_ret_val = template.create(dx_session_obj.server_session,
                                           js_template_params)

        print('Template %s was created successfully with reference %s\n' %
              (template_name, template_ret_val))

    except (DlpxException, RequestError, HttpError) as e:
        print('\nThe template %s was not created. The error was:\n\n%s' %
              (template_name, e))
        sys.exit(1)


def list_template():
    """
    List all templates on a given engine

    """

    js_list = ['name','reference','active_branch','last_updated']
    header = 'Name\t\t\tReference\t\tActive Branch\tLast Updated'
    print header

    try:
        js_templates = template.get_all(dx_session_obj.server_session)

        for js_template in js_templates:
            last_updated = convert_timestamp(dx_session_obj.server_session,
                                             js_template.last_updated[:-5])

            print('%s, %s, %s, %s\n' % (js_template.name,
                                        js_template.reference,
                                        js_template.active_branch,
                                        last_updated))

    except (DlpxException, HttpError, RequestError) as e:
        raise DlpxException('\nERROR: The templates on engine %s could not '
                            'be listed. The error was:\n\n%s' %
                            (engine['hostname'], e.message), debug)


def delete_template(template_name):
    """
    Deletes a template

    template_name: Template to delete
    """

    try:
        template_obj = find_obj_by_name(dx_session_obj.server_session,
                                        template, template_name)
        template.delete(dx_session_obj.server_session,
                        template_obj.reference)

        print 'Template %s is deleted.\n' % (template_name)

    except (DlpxException, HttpError, RequestError) as e:
        raise DlpxException('\nERROR: The template %s was not deleted. The '
                            'error was:\n\n%s' % (template_name, e.message))


def build_ds_params(obj, db):
    """
    Builds the datasource parameters

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

                    if arguments['--create_template']:
                        create_template(arguments['--create_template'],
                                        arguments['--database'])

                    elif arguments['--delete_template']:
                        delete_template(arguments['--delete_template'])

                    elif arguments['--list_templates']:
                        list_template()

                    thingstodo.pop()

    except (DlpxException, RequestError, JobError, HttpError) as e:
        print('\nError in js_template: %s:\n%s' %
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
        database_name = arguments['--database']
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
