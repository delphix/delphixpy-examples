#!/usr/bin/env python
#Adam Bowen - Apr 2016
#This script refreshes a vdb
# Updated by Corey Brune Oct 2016
#requirements
#TODO
#  Update print statements with format()
# Check if environment is enabled/valid

#pip install --upgrade setuptools pip docopt delphixpy

#The below doc follows the POSIX compliant standards and allows us to use 
#this doc to also define our arguments for the script. This thing is brilliant.
"""Refresh a vdb
Usage:
  dx_refresh_db.py (--name <name> | --dsource <name> | --all_vdbs [--group_name <name>]| --host <name> | --list-timeflows | --list-snapshots)
                   [--timestamp_type <type>]
                   [--timestamp <timepoint_semantic> --timeflow <timeflow>]
                   [-d <identifier> | --engine <identifier> | --all]
                   [--debug] [--parallel <n>] [--poll <n>]
                   [--config <path_to_file>] [--logdir <path_to_file>]
  dx_refresh_db.py -h | --help | -v | --version
Refresh a Delphix VDB
Examples:
  dx_refresh_db.py --name "aseTest" --group_name "Analytics"
  dx_refresh_db.py --dsource "dlpxdb1"
  dx_refresh_db.py --all_vdbs --host LINUXSOURCE --parallel 4 --debug -d landsharkengine
  dx_refresh_db.py --all_vdbs --group_name "Analytics" --all
Options:
  --name <name>             Name of the object you are refreshing.
  --all_vdbs                Refresh all VDBs that meet the filter criteria.
  --dsource <name>          Name of dsource in Delphix to execute against.
  --group_name <name>       Name of the group to execute against.
  --list-timeflows          List all timeflows
  --list-snapshots          List all snapshots
  --host <name>             Name of environment in Delphix to execute against.
  --timestamp_type <type>   The type of timestamp you are specifying.
                            Acceptable Values: TIME, SNAPSHOT
                            [default: SNAPSHOT]
  --timestamp <timepoint_semantic>
                            The Delphix semantic for the point in time on
                            the source from which you want to refresh your VDB.
                            Formats:
                            latest point in time or snapshot: LATEST
                            point in time: "YYYY-MM-DD HH24:MI:SS"
                            snapshot name: "@YYYY-MM-DDTHH24:MI:SS.ZZZ"
                            snapshot time from GUI: "YYYY-MM-DD HH24:MI"
                            [default: LATEST]
  --timeflow <name>         Name of the timeflow to refresh a VDB
  -d <identifier>           Identifier of Delphix engine in dxtools.conf.
  --engine <type>           Alt Identifier of Delphix engine in dxtools.conf.
  --all                     Run against all engines.
  --debug                   Enable debug logging
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./dxtools.conf]
  --logdir <path_to_file>   The path to the logfile you want to use.
                            [default: ./dx_refresh_db.log]
  -h --help                 Show this screen.
  -v --version              Show version.
"""

VERSION = 'v.0.1.602'


from docopt import docopt
import logging
from os.path import basename
import sys
import time
import traceback
from time import sleep, time

from delphixpy.v1_8_2.exceptions import HttpError
from delphixpy.v1_8_2.exceptions import JobError
from delphixpy.v1_8_2.exceptions import RequestError
from delphixpy.v1_8_2.web import database
from delphixpy.v1_8_2.web import environment
from delphixpy.v1_8_2.web import group
from delphixpy.v1_8_2.web import job
from delphixpy.v1_8_2.web import source
from delphixpy.v1_8_2.web import timeflow
from delphixpy.v1_8_2.web.snapshot import snapshot
from delphixpy.v1_8_2.web.vo import OracleRefreshParameters
from delphixpy.v1_8_2.web.vo import RefreshParameters
from delphixpy.v1_8_2.web.vo import TimeflowPointLocation
from delphixpy.v1_8_2.web.vo import TimeflowPointSemantic
from delphixpy.v1_8_2.web.vo import TimeflowPointTimestamp

from lib.DlpxException import DlpxException
from lib.DxLogging import logging_est
from lib.DxLogging import print_debug
from lib.DxLogging import print_info
from lib.DxLogging import print_exception
from lib.GetReferences import find_obj_by_name
from lib.GetSession import GetSession


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
    

def find_all_databases_by_dsource_name(engine, server, dsource_name, 
                                       exclude_js_container=True):
    """
    Easy way to quickly find databases by dSource
    """

    #First search for the dSource name specified and return its reference
    dsource_obj = find_obj_by_name(engine, server, database, dsource_name)

    if dsource_obj:
        return(database.get_all(server, 
                        provision_container=dsource_obj.reference, 
                        no_js_container_data_source=exclude_js_container))


def find_all_databases_by_group_name(engine, server, group_name, 
                                     exclude_js_container=True):
    """
    Easy way to quickly find databases by group name
    """

    #First search groups for the name specified and return its reference
    group_obj = find_obj_by_name(engine, server, group, group_name)
    if group_obj:
        return(database.get_all(server, group=group_obj.reference, 
                           no_js_container_data_source=exclude_js_container))


def find_database_by_name_and_group_name(engine, server, group_name, 
                                         database_name):

    databases = find_all_databases_by_group_name(engine, server, group_name)

    for each in databases:
        if each.name == database_name:
            print_debug(engine["hostname"] + ": Found a match " + 
                        str(each.reference))
            return each

    print_info(engine["hostname"] + ": Unable to find \"" + 
               database_name + "\" in " + group_name)


def find_snapshot_by_database_and_name(engine, server, database_obj, snap_name):
    snapshots = snapshot.get_all(server, database=database_obj.reference)
    matches = []
    for snapshot_obj in snapshots:
        if str(snapshot_obj.name).startswith(arguments['--timestamp']):
            matches.append(snapshot_obj)

    if len(matches) == 1:
        print_debug(engine["hostname"] + 
                    ": Found one and only one match. This is good.")
        print_debug(engine["hostname"] + ": " + matches[0])

        return matches[0]

    elif len(matches) > 1:
        print_error("The name specified was not specific enough. "
                    "More than one match found.")

        for each in matches:
            print_debug(engine["hostname"] + ": " + each.name)
    else:
        print_error("No matches found for the time specified")
    print_error("No matching snapshot found")


def find_snapshot_by_database_and_time(engine, server, database_obj, snap_time):
    """
    Find snapshot object by database name and timetamp
    engine: 
    server: A Delphix engine object.
    database_obj: The database reference to retrieve the snapshot
    snap_time: timstamp of the snapshot
    """
    snapshots = snapshot.get_all(server, database=database_obj.reference)
    matches = []

    for snapshot_obj in snapshots:
        if str(snapshot_obj.latest_change_point.timestamp) == snap_time \
               or str(snapshot_obj.first_change_point.timestamp) == snap_time:

            matches.append(snapshot_obj)

    if len(matches) == 1:
        snap_match = get_obj_name(server, database, matches[0].container)
        print_debug(engine['hostname'] + 
                    ': Found one and only one match. This is good.')
        print_debug(engine['hostname'] + ': ' + snap_match)

        return matches[0]

    elif len(matches) > 1:
        print_debug(engine["hostname"] + ": " + matches)
        raise DlpxException('The time specified was not specific enough.'
                    ' More than one match found.\n')
    else:
        raise DlpxException('No matches found for the time specified.\n')


def find_source_by_database(engine, server, database_obj):
    #The source tells us if the database is enabled/disables, virtual, 
    # vdb/dSource, or is a staging database.
    source_obj = source.get_all(server, database=database_obj.reference)

    #We'll just do a little sanity check here to ensure we only have a 
    # 1:1 result.
    if len(source_obj) == 0:
        print_error(engine["hostname"] + ": Did not find a source for " + 
                    database_obj.name + ". Exiting")
        sys.exit(1)

    elif len(source_obj) > 1:
        print_error(engine["hostname"] + 
                    ": More than one source returned for " + 
                    database_obj.name + ". Exiting")
        print_error(source_obj)
        sys.exit(1)

    return source_obj


def get_obj_name(server, f_object, obj_reference):
    """
    Return the object name from obj_reference
                
    engine: A Delphix engine object.
    obj_reference: The object reference to retrieve the name
    """

    try:
        obj_name = f_object.get(server, obj_reference)
        return(obj_name.name)

    except (HttpError, RequestError) as e:
        raise DlpxException(e)


def list_snapshots(server):
    """
    List all snapshots with timestamps
    """
    container_name = ''

    header = 'Snapshot Name, First Change Point, Latest Change Point'
    snapshots = snapshot.get_all(server)

    print header
    for snap in snapshots:
        try:
            container_name = get_obj_name(server, database, snap.container)
            snap_range = snapshot.timeflow_range(server, snap.reference)
        except (DlpxException, RequestError, HttpError) as e:
            print_exception('Encountered exception when listing snapshots:\n'
                            '{}'.format(e))

        print '{}, {}, {}, {}'.format(str(snap.name), container_name,
                                      snap_range.start_point.timestamp,
                                      snap_range.end_point.timestamp)


@run_async
def main_workflow(engine):
    """
    This function is where we create our main workflow.
    Use the @run_async decorator to run this function asynchronously.
    The @run_async decorator allows us to run against multiple Delphix Engine 
    simultaneously
    """

    #Pull out the values from the dictionary for this engine
    try:
        #Setup the connection to the Delphix Engine
        dx_session_obj.serversess(engine['ip_address'], engine['username'],
                                  engine['password'])

        if arguments['--vdb']:
            #Get the database reference we are copying from the database name
            database_obj = find_obj_by_name(dx_session_obj.server_session,
                                            database, arguments['--vdb'])

    except DlpxException as e:
        print_exception('\nERROR: Engine {} encountered an error while' 
                        '{}:\n{}\n'.format(engine['hostname'],
                        arguments['--target'], e))
        sys.exit(1)

    #Establish these variables as empty for use later
    databases = []
    environment_obj = None
    source_objs = None
    jobs = {}

    #If an environment/server was specified
    if host_name:
        print_debug('{}: Getting environment for {}'.format(engine['hostname'],
                    host_name))
        #Get the environment object by the hostname
        environment_obj = find_obj_by_name(dx_session_obj.server_session,
                                           environment, host_name)

        if environment_obj != None:
            #Get all the sources running on the server
            env_source_objs = source.get_all(dx_session_obj.server_session,
                                     environment=environment_obj.reference)

            #If the server doesn't have any objects, exit.
            if env_source_objs is None:
                print_error(host_name + "does not have any objects. Exiting")
                sys.exit(1)

            #If we are only filtering by the server, then put those objects in 
            # the main list for processing
            if not(arguments['--group_name'] and database_name):
                source_objs = env_source_objs
                all_dbs = database.get_all(server, 
                                           no_js_container_data_source=True)
                databases = []
                for source_obj in source_objs:
                    if source_obj.staging == False and \
                                  source_obj.virtual == True:

                        database_obj = database.get(server, 
                                                    source_obj.container)

                        if database_obj in all_dbs:
                            databases.append(database_obj)
        else:
            print_error(engine["hostname"] + ":No environment found for " + 
                        host_name + ". Exiting")
            sys.exit(1)

    #If we specified a specific database by name....
    if arguments['--name']:
        #Get the database object from the name

        database_obj = find_obj_by_name(engine, server, database,
                                        arguments['--name'])
        if database_obj:
            databases.append(database_obj)

    #Else if we specified a group to filter by....
    elif arguments['--group_name']:
        print_debug(engine["hostname"] + ":Getting databases in group " + 
                    arguments['--group_name'])
        #Get all the database objects in a group.
        databases = find_all_databases_by_group_name(engine, server, 
                                                     arguments['--group_name'])

    #Else if we specified a dSource to filter by....
    elif arguments['--dsource']:
        print_debug(engine["hostname"] + ":Getting databases for dSource" + 
                    arguments['--dsource'])

        #Get all the database objects in a group.
        databases = find_all_databases_by_dsource_name(engine, server, 
                                                       arguments['--dsource'])

    #Else, if we said all vdbs ...
    elif arguments['--all_vdbs'] and not arguments['--host'] :
        print_debug(engine['hostname'] + ':Getting all VDBs ')

        #Grab all databases, but filter out the database that are in JetStream 
        #containers, because we can't refresh those this way.
        databases = database.get_all(server, no_js_container_data_source=True)

    elif arguments['--list-timeflows']:
        list_timeflows(server)

    elif arguments['--list-snapshots']:
        list_snapshots(server)

    #reset the running job count before we begin
    i = 0
    try:
        with dx_session_obj.job_mode(single_thread):
            while (len(dx_session_obj.jobs) > 0 or len(thingstodo)> 0):
                if len(thingstodo) > 0:

            #While there are databases still to process and we are still under 
            #the max simultaneous jobs threshold (if specified)
            while len(databases) > 0 and (arguments['--parallel'] == None or \
                  i < int(arguments['--parallel'])):

                #Give us the next database in the list, and then remove it
                database_obj = databases.pop()
                #Get the source of the database.
                source_obj = find_source_by_database(engine, server, 
                                                     database_obj)

               #If we applied the environment/server filter AND group filter, 
               # find the intersecting matches
                if environment_obj != None and (arguments['--group_name']):
                    match = False

                    for env_source_obj in env_source_objs:
                        if source_obj[0].reference in env_source_obj.reference:
                            match = True
                            break
                    if match == False:
                        print_error(engine["hostname"] + ": " + 
                                    database_obj.name + " does not exist on " +
                                    host_name + ". Exiting")
                        return

                #Refresh the database
                refresh_job = refresh_database(engine,
                                               dx_session_obj.server_session,
                                               jobs,  source_obj[0],
                                               database_obj)
                #If refresh_job has any value, then we know that a job was 
                # initiated.

                if refresh_job:
                    #increment the running job count
                    i += 1
            #Check to see if we are running at max parallel processes, and 
            # report if so.
            if (arguments['--parallel'] != None and \
                            i >= int(arguments['--parallel'])):

                print_info(engine["hostname"] + ": Max jobs reached (" + 
                           str(i) + ")")

            i = update_jobs_dictionary(engine, dx_session_obj.server_session,
                                       jobs)
            print_info('{}: {:.2f} jobs running. {:d} jobs waiting to '
                       'run.'.format(engine['hostname'], i, len(databases)))

            #If we have running jobs, pause before repeating the checks.
            if len(jobs) > 0:
                sleep(float(arguments['--poll']))


def on_exit(sig, func=None):
    """
    This function helps us end cleanly and with exit codes
    """
    print_info("Shutdown Command Received")
    print_info("Shutting down " + basename(__file__))
    sys.exit(0)


def print_error(print_obj):
    """
    Call this function with a log message to prefix the message with ERROR
    """
    print "ERROR: " + str(print_obj)
    logging.error(str(print_obj))


def print_warning(print_obj):
    """
    Call this function with a log message to prefix the message with WARNING
    """
    print "WARNING: " + str(print_obj)
    logging.warning(str(print_obj))


def refresh_database(engine, server, jobs, source_obj, container_obj):
    """
    This function actually performs the refresh
    engine:
    server: Engine object
    jobs: list containing running jobs
    source_obj: source object used to refresh from snapshot or timeflow
    container_obj: VDB container
    """

    #Sanity check to make sure our source object has a reference
    if source_obj.reference:
        #We can only refresh VDB's
        if source_obj.virtual != True:
            print_warning(engine["hostname"] + ": " + container_obj.name + 
                          " is not a virtual object. Skipping.")

        #Ensure this source is not a staging database. We can't act upon those.
        elif source_obj.staging == True:
            print_warning(engine["hostname"] + ": " + container_obj.name + 
                          " is a staging database. Skipping.")

        #Ensure the source is enabled. We can't refresh disabled databases.
        elif source_obj.runtime.enabled == "ENABLED" :
            source_db = database.get(server, container_obj.provision_container)
            if not source_db:
                print_error(engine["hostname"] + 
                            ":Was unable to retrieve the source container for "
                            + container_obj.name)
            print_info(engine["hostname"] + ": Refreshing " + 
                       container_obj.name + " from " + source_db.name)
            print_debug(engine["hostname"] + ": Type: " + source_obj.type )
            print_debug(engine["hostname"] + ":" + source_obj.type)

            #If the vdb is a Oracle type, we need to use a 
            # OracleRefreshParameters

            if str(container_obj.reference).startswith("ORACLE"):
                refresh_params = OracleRefreshParameters()
            else:
                refresh_params = RefreshParameters()
            
            try:
                refresh_params.timeflow_point_parameters = set_timeflow_point(
                                                           engine, server, 
                                                           source_db)
                print_debug(engine["hostname"] + ":" + str(refresh_params))

                #Sync it
                database.refresh(server, container_obj.reference, 
                                 refresh_params)
                jobs[container_obj] = server.last_job

            except RequestError as e:
                print '\nERROR: Could not set timeflow point:\n%s\n' % (
                      e.message.action)
                sys.exit(1)

            except DlpxException as e:
                print 'ERROR: Could not set timeflow point:\n%s\n' % (e.message)
                sys.exit(1)


            #return the job object to the calling statement so that we can 
            # tell if a job was created or not (will return None, if no job)
            return server.last_job

        #Don't do anything if the database is disabled
        else:
            print_warning(engine["hostname"] + ": " + container_obj.name + 
                          " is not enabled. Skipping sync")


def run_job(engine):
    """
    This function runs the main_workflow aynchronously against all the 
    servers specified
    """

    #Create an empty list to store threads we create.
    threads = []
    #If the --all argument was given, run against every engine in dxtools.conf

    if arguments['--all']:
        print_info("Executing against all Delphix Engines in the dxtools.conf")

        #For each server in the dxtools.conf...
        for delphix_engine in dxtools_objects:
            engine = dxtools_objects[delphix_engine]
            #Create a new thread and add it to the list.
            threads.append(main_workflow(engine))

    else:
        #Else if the --engine argument was given, test to see if the engine 
        # exists in dxtools.conf
        if arguments['--engine']:
            try:
                engine = dxtools_objects[arguments['--engine']]
                print_info("Executing against Delphix Engine: " + 
                           arguments['--engine'])
            except:
                print_error("Delphix Engine \"" + arguments['--engine'] + "\" \
                            cannot be found in " + config_file_path)
                print_error("Please check your value and try again. Exiting")
                sys.exit(1)

        #Else if the -d argument was given, test to see if the engine exists 
        # in dxtools.conf
        elif arguments['-d']:
            try:
                engine = dxtools_objects[arguments['-d']]
                print_info("Executing against Delphix Engine: " + 
                           arguments['-d'])
            except:
                print_error("Delphix Engine \"" + arguments['-d'] + 
                            "\" cannot be found in " + config_file_path)
                print_error("Please check your value and try again. Exiting")
                sys.exit(1)

        else:
            #Else search for a default engine in the dxtools.conf
            for delphix_engine in dxtools_objects:
                if dxtools_objects[delphix_engine]['default'] == 'true':
                    engine = dxtools_objects[delphix_engine]
                    print_info("Executing against the default Delphix Engine" \
                               " in the dxtools.conf: " + 
                               dxtools_objects[delphix_engine]['hostname'])
                    break
            if engine == None:
                print_error("No default engine found. Exiting")
                sys.exit(1)
        #run the job against the engine
        threads.append(main_workflow(engine))

    #For each thread in the list...
    for each in threads:
        #join them back together so that we wait for all threads to complete 
        # before moving on
        each.join()


def list_timeflows(server):
    """
    Retrieve and print all timeflows for a given engine
    """

    ret_timeflow_dct = {}
    all_timeflows = timeflow.get_all(server)

    print 'DB Name, Timeflow Name, Timestamp'

    for tfbm_lst in all_timeflows:
        try:

            db_name = get_obj_name(server, database, tfbm_lst.container)
            print '%s, %s, %s\n' % (str(db_name), str(tfbm_lst.name),
                                    str(tfbm_lst.parent_point.timestamp))

        except AttributeError:
            print '%s, %s\n' % (str(tfbm_lst.name), str(db_name))

        except TypeError as e:
           raise DlpxException('Listing Timeflows encountered an error:\n%s' %
                               (e.message))

        except RequestError, e:
            dlpx_err = e.message
            raise DlpxException(dlpx_err.action)


def set_timeflow_point(engine, server, container_obj):
    """
    This returns the reference of the timestamp specified.
    engine:
    server: Delphix Engine object
    container_obj: VDB object
    """

    if arguments['--timestamp_type'].upper() == "SNAPSHOT":
        if arguments['--timestamp'].upper() == "LATEST":
            print_debug(engine["hostname"] + ": Using the latest Snapshot")
            timeflow_point_parameters = TimeflowPointSemantic()
            timeflow_point_parameters.location = "LATEST_SNAPSHOT"

        elif arguments['--timestamp'].startswith("@"):
            print_debug(engine["hostname"] + ": Using a named snapshot")
            snapshot_obj = find_snapshot_by_database_and_name(engine, server, 
                                                     container_obj, 
                                                     arguments['--timestamp'])

            if snapshot_obj:
                timeflow_point_parameters=TimeflowPointLocation()
                timeflow_point_parameters.timeflow = snapshot_obj.timeflow
                timeflow_point_parameters.location = \
                               snapshot_obj.latest_change_point.location

            else:
                raise DlpxException('ERROR: Was unable to use the specified '
                                    'snapshot {} for database {}.\n'.format(
                                    arguments['--timestamp'],
                                    container_obj.name))

        elif arguments['--timestamp']:
            print_debug(engine["hostname"] + 
                        ": Using a time-designated snapshot")
            snapshot_obj = find_snapshot_by_database_and_time(
                           engine, server, container_obj, 
                           arguments['--timestamp'])

            if snapshot_obj:
                timeflow_point_parameters=TimeflowPointTimestamp()
                timeflow_point_parameters.timeflow = snapshot_obj.timeflow
                timeflow_point_parameters.timestamp = \
                               snapshot_obj.latest_change_point.timestamp

            else:
                raise DlpxException('Was unable to find a suitable time'
                                    '  for %s for database %s' %
                                    (arguments['--timestamp'],
                                    container_obj.name))

    elif arguments['--timestamp_type'].upper() == "TIME":

        if arguments['--timestamp'].upper() == "LATEST":
            timeflow_point_parameters = TimeflowPointSemantic()
            timeflow_point_parameters.location = "LATEST_POINT"

        elif arguments['--timestamp']:
            timeflow_point_parameters = TimeflowPointTimestamp()
            timeflow_point_parameters.type = 'TimeflowPointTimestamp'
            timeflow_obj = find_obj_by_name(engine, server, timeflow, 
                               arguments['--timeflow'])

            timeflow_point_parameters.timeflow = timeflow_obj.reference
            timeflow_point_parameters.timestamp = arguments['--timestamp']
            return timeflow_point_parameters
    else:
        raise DlpxException(arguments['--timestamp_type'] + 
                    " is not a valied timestamp_type. Exiting")

    timeflow_point_parameters.container = container_obj.reference
    return timeflow_point_parameters


def time_elapsed():
    """
    This function calculates the time elapsed since the beginning of the script.
    Call this anywhere you want to note the progress in terms of time 
    """
    elapsed_minutes = round((time() - time_start)/60, +1)
    return elapsed_minutes


def update_jobs_dictionary(engine, server, jobs):
    """
    This function checks each job in the dictionary and updates its status or 
    removes it if the job is complete.
    Return the number of jobs still running.
    """
    #Establish the running jobs counter, as we are about to update the count 
    # from the jobs report.
    i = 0
    #get all the jobs, then inspect them
    for j in jobs.keys():
        job_obj = job.get(server, jobs[j])
        print_debug(engine["hostname"] + ": " + str(job_obj))
        print_info(engine["hostname"] + ": " + j.name + ": " + 
                   job_obj.job_state)
        
        if job_obj.job_state in ["CANCELED", "COMPLETED", "FAILED"]:
            #If the job is in a non-running state, remove it from the running 
            # jobs list.
            del jobs[j]
        else:
            #If the job is in a running state, increment the running job count.
            i += 1
    return i


def main(argv):
    #We want to be able to call on these variables anywhere in the script.
    global single_thread
    global usebackup
    global time_start
    global host_name
    global database_name
    global config_file_path
    global dxtools_objects
    global dx_session_obj

    try:
        #Declare globals that will be used throughout the script.
        dx_session_obj = GetSession()
        logging_est(arguments['--logdir'])
        print_debug(arguments)
        time_start = time()
        engine = None
        single_thread = False
        database_name = arguments['--name']
        host_name = arguments['--host']
        dx_session_obj.get_config(arguments['--config'])
        
        #This is the function that will handle processing main_workflow for 
        # all the servers.
        run_job()
        
        elapsed_minutes = time_elapsed()
        print_info('Script took {:d} minutes to get this far.'.format(
            elapsed_minutes))

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
        print_exception('Connection failed to the Delphix Engine. Please '
                        'check the error message below:\n{}'.format(e.message))
        sys.exit(2)

    except JobError as e:
        """
        We use this exception handler when a job fails in Delphix so that we 
        have actionable data
        """
        print_exception('A job failed in the Delphix Engine:\n{}'.format(e.job))
        elapsed_minutes = time_elapsed()
        print_info('{} took {:d} minutes to get this far'.format(
            basename(__file__), elapsed_minutes))
        sys.exit(3)

    except KeyboardInterrupt:
        """
        We use this exception handler to gracefully handle ctrl+c exits
        """
        print_debug('You sent a CTRL+C to interrupt the process')
        elapsed_minutes = time_elapsed()
        print_info('{} took {:d} minutes to get this far'.format(
            basename(__file__), elapsed_minutes))

    except:
        """
        Everything else gets caught here
        """
        print_exception(sys.exc_info()[0])
        print_exception(traceback.format_exc())
        elapsed_minutes = time_elapsed()
        print_info('{} took {:d} minutes to get this far'.format(
            basename(__file__), elapsed_minutes))
        sys.exit(1)

if __name__ == "__main__":
    #Grab our arguments from the doc at the top of the script
    arguments = docopt(__doc__, version=basename(__file__) + " " + VERSION)

    #Feed our arguments to the main function, and off we go!
    main(arguments)
