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
"""Refresh a vdb
Usage:
  js_template.py (--create_template <template_name> --database <name> | --list_templates | --delete_template <template_name>)
                   [--engine <identifier> | --all] [--parallel <n>]
                   [--poll <n>] [--debug]
                   [--config <path_to_file>] [--logdir <path_to_file>]
  js_template.py -h | --help | -v | --version

Creates, Lists, Removes a Jet Stream Template

Examples:
  js_template.py --list-template
  js_template.py --create-template <template_name> --database <name>
  js_template.py --create-template <template_name> --database <name:name:name>
  js_template.py --delete-template <template_name>

Options:
  --create-template <name>  Name of the new JS Template
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
from datetime import datetime
from dateutil import tz

from multiprocessing import Process
from time import sleep, time
from delphixpy.v1_6_0.delphix_engine import DelphixEngine
from delphixpy.v1_6_0.web.jetstream import template
from delphixpy.v1_6_0 import job_context
from delphixpy.v1_6_0.web import database
from delphixpy.v1_6_0.web.service import time
from delphixpy.v1_6_0.web.vo import JSDataTemplateCreateParameters
from delphixpy.v1_6_0.exceptions import RequestError
from delphixpy.v1_6_0.exceptions import JobError
from delphixpy.v1_6_0.exceptions import HttpError

from lib.DlpxException import DlpxException
from lib.DxTimeflow import DxTimeflow
from lib.GetReferences import convert_timestamp
from lib.GetReferences import find_obj_by_name
from lib.DxLogging import print_error
from lib.GetSession import GetSession


def create_template(template_name, database_name):
    assert template_name and database_name is not '', 'The template ' \
            'and database names are required for creating a JS Template.\n'

    js_template_params = JSDataTemplateCreateParameters()

    template_ds_lst = []
    for db in database_name.split(','):
            template_ds_lst.append(build_ds_params(engine, db))

    try:
        js_template_params.data_sources = template_ds_lst
        js_template_params.name = template_name
        js_template_params.type = 'JSDataTemplateCreateParameters'

        template_ret_val = template.create(dx_session_obj.server_session,
                                           js_template_params)

        print('Template %s was created successfully with reference %s\n' %
              (template_name, template_ret_val))

    except DlpxException, e:
        print('\nThe template %s was not created. The error was:\n\n%s' %
              (template_name, e.errors))
        sys.exit(1)

    except AssertionError, e:
        print('\nAn error occurred creating the container, %s:\n\n%s' %
              (container_name, e))


def list_template(engine):
    """
    List all templates on a given engine

    engine: Delphix Engine session object
    """

    js_list = ['name','reference','active_branch','last_updated']
    header = 'Name\t\t\tReference\t\tActive Branch\tLast Updated'
    print header
    
    try:
        js_templates = template.get_all(engine)

        for js_template in js_templates:
            last_updated = convertTimestamp(engine, 
                                            js_template.last_updated[:-5])

        print js_template.name, js_template.reference, \
            js_template.active_branch, last_updated

    except (DlpxException, HttpError, RequestError) as e:
        raise DlpxException('\nERROR: The templates on engine %s could not '
                            'be listed. The error was:\n\n%s' % 
                            (engine.ip_addr, e.message))


def delete_template(engine, template_name):
    """
    Deletes a template

    engine: Delphix Engine session object
    template_name: Template to delete
    """

    try:
        template_obj = find_obj_by_name(engine, template, template_name)
        template.delete(engine, template_obj.reference)

        print 'Template %s is deleted.' % (template_name)

    except (DlpxException, HttpError, RequestError) as e:
        raise DlpxException('\nERROR: The template %s was not deleted. The '
                            'error was:\n\n%s' % (template_name, e.message))


def build_ds_params(engine, db):
    try:
        db_obj = find_obj_by_name(engine, db)
        return({'type': 'JSDataSourceCreateParameters', 'source': {'type':
                'JSDataSource', 'name': db}, 'container': db_obj.reference})

    except RequestError as e:
        print('\nCould not find %s\n%s' % (db, e.message))
        sys.exit(1)


def job_mode(server):
    """
    This function tells Delphix how to execute jobs, based on the 
    single_thread variable at the beginning of the file
    """
    #Synchronously (one at a time)
    if single_thread == True:
        job_m = job_context.sync(server)
        print_debug("These jobs will be executed synchronously")
    #Or asynchronously
    else:
        job_m = job_context.async(server)
        print_debug("These jobs will be executed asynchronously")
    return job_m


@run_async
def main_workflow(engine):
    """
    This function is where we create our main workflow.
    Use the @run_async decorator to run this function asynchronously.
    The @run_async decorator allows us to run against multiple Delphix Engine 
    simultaneously

    engine: Dictionary containing engine IP, username, password
    """

    #Establish these variables as empty for use later
    environment_obj = None
    source_objs = None
    jobs = {}

    try:
        #Setup the connection to the Delphix Engine
        dx_session_obj.serversess(engine['ip_address'], engine['username'],
                                  engine['password'])

    except DlpxException as e:
        raise DlpxException(e)

    thingstodo = ["thingtodo"]
    #reset the running job count before we begin
    i = 0

    try:
        with job_mode(server):
            while (len(jobs) > 0 or len(thingstodo)> 0):
                if len(thingstodo)> 0:

                    if arguments['--create_template']:
                        create_template(arguments['--create_template'],
                                        arguments['--database'])
                    elif arguments['--delete_template']:
                    elif arguments['--list_template']:

                thingstodo.pop()
                #get all the jobs, then inspect them 
                i = 0
                for j in jobs.keys():
                    job_obj = job.get(server, jobs[j])
                    print_debug(job_obj)
                    if job_obj.job_state in ["CANCELED", "COMPLETED", "FAILED"]:
                        #If the job is in a non-running state, remove it 
                        #from the running jobs list.
                        del jobs[j]
                    else:
                        #If the job is in a running state, increment the 
                        #running job count.
                        i += 1

            print_info('\n%s: %s jobs running.\n' % (engine['hostname'], 
                       str(i)))

            #If we have running jobs, pause before repeating the checks.
            if len(jobs) > 0:
                sleep(float(arguments['--poll']))

    except (DlpxException, JobError) as e:
        print('\nERROR: %s engine encountered an error:\n%s' % 
              (database_name, e.message))
        sys.exit(1)


def run_job():
    """
    This function runs the main_workflow aynchronously against all the
    servers specified

    No arguments. run_job() uses global variables instead of args.
    """

    #Create an empty list to store threads we create.
    threads = []

    #Else if the --engine argument was given, test to see if the engine
    # exists in dxtools.conf
    if arguments['--engine']:
        try:
            engine = dx_session_obj.dlpx_engines[arguments['--engine']]
            print_info("Executing against Delphix Engine: " +
                       arguments['--engine'])

        except KeyError:
            raise DlpxException('\nERROR: Delphix Engine %s cannot be '
                                'found in %s.\nPlease check your values '
                                'and try again. Exiting\n' %
                                (arguments['--engine'],
                                config_file_path))

    elif arguments['--engine'] is None:
        #search for a default engine in the dxtools.conf
        for delphix_engine in dx_session_obj.dlpx_engines:

            if dx_session_obj.dlpx_engines[delphix_engine]['default'] == 'true':
                engine = dx_session_obj.dlpx_engines[delphix_engine]
                print_info('Executing against the default Delphix Engine in '
                       'the dxtools.conf: %s' % (
                       dx_session_obj.dlpx_engines[delphix_engine]['hostname']))

                break

        if engine is None:
            raise DlpxException("\nNo engine specified with the --engine arg '
                                ' and no default engine found. Exiting.\n")

        #run the job against the engine
        main_workflow(engine)


def main(argv):
    #We want to be able to call on these variables anywhere in the script.
    global single_thread
    global usebackup
    global time_start
    global host_name
    global database_name
    global config_file_path
    global dx_session_obj

    try:
        dx_session_obj = GetSession()
        logging_est(arguments['--logdir'])
        print_debug(arguments)
        time_start = time()
        database_name = arguments['--vdb']
        config_file_path = arguments['--config']


        logging_est(arguments['--logdir'])
        print_debug(arguments)
        time_start = time()
        engine = None
        single_thread = False
        database_name = arguments['--name']
        host_name = arguments['--host']
        config_file_path = arguments['--config']
        #Parse the dxtools.conf and put it into a dictionary
        dxtools_objects = get_config(config_file_path)

        #This is the function that will handle processing main_workflow for
        # all the servers.
        run_job(engine)

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
        print_error("Connection failed to the Delphix Engine")
        print_error( "Please check the ERROR message below")
        print_error(e.message)
        sys.exit(2)
    except JobError as e:
        """
        We use this exception handler when a job fails in Delphix so that we
        have actionable data
        """
        print_error("A job failed in the Delphix Engine")
        print_error(e.job)
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
        print_error(sys.exc_info()[0])
        print_error(traceback.format_exc())
        elapsed_minutes = time_elapsed()
        print_info(basename(__file__) + " took " + str(elapsed_minutes) +
                   " minutes to get this far.")
        sys.exit(1)


if __name__ == "__main__":
    #Grab our arguments from the doc at the top of the script
    arguments = docopt(__doc__, version=basename(__file__) + " " + VERSION)

    #Feed our arguments to the main function, and off we go!
    main(arguments)
