#!/usr/bin/env python
# Program Name : js_bookmark.py
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
"""Creates, lists, removes a Jet Stream Bookmark
Usage:
  js_bookmark.py (--create_bookmark <name> --template_name <name> [--branch_name <name]| --list_bookmarks | --delete_bookmark <name> | --activate_bookmark <name> | --update_bookmark <name> | --share_bookmark <name> | --unshare_bookmark <name>)
                   [--engine <identifier> | --all] [--parallel <n>]
                   [--poll <n>] [--debug]
                   [--config <path_to_file>] [--logdir <path_to_file>]
  js_bookmark.py -h | --help | -v | --version

Creates, Lists, Removes a Jet Stream Bookmark

Examples:
  js_bookmark.py --list_bookmarks
  js_bookmark.py --create_bookmark jsbookmark1 --template_name jstemplate1
  js_bookmark.py --create_bookmark jsbookmark1 --template_name jstemplate1 --branch_name jsbranch1
  js_bookmark.py --activate_bookmark jsbookmark1
  js_bookmark.py --update_bookmark jsbookmark1
  js_bookmark.py --delete_bookmark jsbookmark1
  js_bookmark.py --share_bookmark jsbookmark1
  js_bookmark.py --unshare_bookmark jsbookmark1

Options:
  --create_bookmark <name>    Name of the new JS Bookmark
  --container_name <name>     Name of the container to use
  --update_bookmark <name>    Name of the bookmark to update
  --share_bookmark <name>     Name of the bookmark to share
  --unshare_bookmark <name>   Name of the bookmark to unshare
  --branch_name <name>        Optional: Name of the branch to use
  --template_name <name>      Name of the template to use
  --activate_bookmark <name>  Name of the bookmark to activate
  --delete_bookmark <name>    Delete the JS Bookmark
  --list_bookmarks            List the bookmarks on a given engine
  --engine <type>             Alt Identifier of Delphix engine in dxtools.conf.
  --all                       Run against all engines.
  --debug                     Enable debug logging
  --parallel <n>              Limit number of jobs to maxjob
  --poll <n>                  The number of seconds to wait between job polls
                              [default: 10]
  --config <path_to_file>     The path to the dxtools.conf file
                              [default: ./dxtools.conf]
  --logdir <path_to_file>     The path to the logfile you want to use.
                              [default: ./js_bookmark.log]
  -h --help                   Show this screen.
  -v --version                Show version.
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
from delphixpy.v1_7_0.web.jetstream import bookmark
from delphixpy.v1_7_0.web.jetstream import container
from delphixpy.v1_7_0.web.jetstream import branch
from delphixpy.v1_7_0.web.jetstream import template
from delphixpy.v1_7_0.web import database
from delphixpy.v1_7_0.web.vo import JSBookmarkCreateParameters
from delphixpy.v1_7_0.web.vo import JSDataSourceCreateParameters
from delphixpy.v1_7_0.web.vo import JSBookmark
from delphixpy.v1_7_0.exceptions import RequestError
from delphixpy.v1_7_0.exceptions import JobError
from delphixpy.v1_7_0.exceptions import HttpError

from lib.DxTimeflow import DxTimeflow
from lib.DlpxException import DlpxException
from lib.GetSession import GetSession
from lib.GetReferences import find_obj_by_name
from lib.GetReferences import convert_timestamp
from lib.GetReferences import get_obj_reference
from lib.DxLogging import logging_est
from lib.DxLogging import print_info
from lib.DxLogging import print_debug


def create_bookmark(bookmark_name, template_name, branch_name=None):
    """
    Create the JS Bookmark

    bookmark_name: Name of the bookmark to create
    template_name: Name of the template to use
    branch_name: Name of the branch to use
    """

    js_bookmark_params = JSBookmarkCreateParameters()

    try:
        if branch_name:
            branch_obj = find_obj_by_name(dx_session_obj.server_session,
                                          branch, branch_name)
            branch_ref = branch_obj.reference

            source_layout_obj = find_obj_by_name(dx_session_obj.server_session,
                                                 template, template_name)
            source_layout_ref = source_layout.reference

        elif branch_name is None:
            (source_layout_ref, branch_ref) = find_obj_by_name(
                                              dx_session_obj.server_session,
                                              template, template_name, True)

        js_bookmark_params.bookmark = {'name': bookmark_name,
                                       'branch': branch_ref,
                                       'type': 'JSBookmark'}

        js_bookmark_params.timeline_point_parameters = {
                                              'sourceDataLayout':
                                              source_layout_ref,
                                              'type':
                                              'JSTimelinePointLatestTimeInput'}
            
        bookmark.create(dx_session_obj.server_session, js_bookmark_params)

    except (DlpxException, RequestError, HttpError) as e:
        print('\nThe bookmark %s was not created. The error was:\n\n%s' %
              (bookmark_name, e))
        sys.exit(1)


def list_bookmarks():
    """
    List all bookmarks on a given engine

    No args required
    """

    try:
        header = '\nName\tReference\tJSBookmark Name'
        js_bookmarks = bookmark.get_all(dx_session_obj.server_session)
            
        print header
        for js_bookmark in js_bookmarks:
            print('%s, %s, %s' % (js_bookmark.name, js_bookmark.reference,
                                    js_bookmark._name[0]))
        print '\n'

    except (DlpxException, HttpError, RequestError) as e:
        print('\nERROR: The bookmarks on could not be listed. The error '
              'was:\n\n%s' % (e))
        sys.exit(1)


def unshare_bookmark(bookmark_name):
    """
    Unshare a bookmark

    bookmark_name: Name of the bookmark to share
    """

    try:
        bookmark.unshare(dx_session_obj.server_session,
                         get_obj_reference(dx_session_obj.server_session,
                         bookmark, bookmark_name).pop())

    except (DlpxException, HttpError, RequestError) as e:
        print('\nERROR: The bookmark %s could not be unshared. The error was'
              ':\n\n%s' % (bookmark_name, e))


def share_bookmark(bookmark_name):
    """
    Share a bookmark

    bookmark_name: Name of the bookmark to share
    """

    try:
        bookmark.share(dx_session_obj.server_session,
                        get_obj_reference(dx_session_obj.server_session,
                        bookmark, bookmark_name).pop())

    except (DlpxException, HttpError, RequestError) as e:
        print('\nERROR: The bookmark %s could not be shared. The error was'
              ':\n\n%s' % (bookmark_name, e))


def update_bookmark(bookmark_name):
    """
    Updates a bookmark

    bookmark_name: Name of the bookmark to update
    """

    js_bookmark_obj = JSBookmark()
    
    try:
        bookmark.update(dx_session_obj.server_session,
                        get_obj_reference(dx_session_obj.server_session,
                        bookmark, bookmark_name).pop(), js_bookmark_obj)

    except (DlpxException, HttpError, RequestError) as e:
        print('\nERROR: The bookmark %s could not be updated. The error was'
              ':\n\n%s' % (bookmark_name, e))


def delete_bookmark(bookmark_name):
    """
    Deletes a bookmark

    bookmark_name: Bookmark to delete
    """

    try:
        bookmark.delete(dx_session_obj.server_session,
                        get_obj_reference(dx_session_obj.server_session,
                                          bookmark, bookmark_name).pop())

    except (DlpxException, HttpError, RequestError) as e:
        raise DlpxException('\nERROR: The bookmark %s was not deleted. The '
                            'error was:\n\n%s' % (bookmark_name, e.message))


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
        print('\nERROR: An error occurred while updating bookmark %s:\n%s' %
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

                    if arguments['--create_bookmark']:
                        create_bookmark(arguments['--create_bookmark'],
                                        arguments['--template_name'])

                    elif arguments['--delete_bookmark']:
                        delete_bookmark(arguments['--delete_bookmark'])

                    elif arguments['--update_bookmark']:
                        update_bookmark(arguments['--update_bookmark'])

                    elif arguments['--activate_bookmark']:
                        activate_bookmark(arguments['--activate_bookmark'])

                    elif arguments['--share_bookmark']:
                        share_bookmark(arguments['--share_bookmark'])

                    elif arguments['--unshare_bookmark']:
                        unshare_bookmark(arguments['--unshare_bookmark'])

                    elif arguments['--list_bookmarks']:
                        list_bookmarks()

                    thingstodo.pop()

    except (DlpxException, RequestError, JobError, HttpError) as e:
        print('\nError in js_bookmark: %s:\n%s' %
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
