#!/home/delphix/landshark/bin/python
#Adam Bowen - Apr 2016
#This script refreshes a vdb
#requirements
#pip install --upgrade setuptools pip docopt delphixpy

#The below doc follows the POSIX compliant standards and allows us to use 
#this doc to also define our arguments for the script. This thing is brilliant.
"""Refresh a vdb

Usage:
  dx_refresh_db.py (--group <name> [--name <name>] | --all_vdbs | --host <name> [--group <name>])
                   [--timestamp_type <type>] [--timestamp <timepoint_semantic>]
                   [-d <identifier> | --engine <identifier> | --all]
                   [--debug] [--parallel <n>] [--poll <n>]
                   [--config <path_to_file>] [--logdir <path_to_file>]
  dx_refresh_db.py -h | --help | -v | --version

Refresh a Delphix VDB

Examples:
  dx_refresh_db.py --name "aseTest" --group "Analytics"
  dx_refresh_db.py --all_vdbs --host LINUXSOURCE --parallel 4 --debug -d landsharkengine
  dx_refresh_db.py --all_vdbs --group "Analytics" --all


Options:
  --name <name>             Name of the object you are refreshing.
  --all_vdbs                Refresh all VDBs that meet the filter criteria.
  --group <name>            Name of group in Delphix to execute against.
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
   -d <identifier>          Identifier of Delphix engine in dxtools.conf.
  --engine <type>           Alt Identifier of Delphix engine in dxtools.conf.
  --all                     Run against all engines.
  --debug                   Enable debug logging
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./dxtools.conf]
  --logdir <path_to_file>    The path to the logfile you want to use.
                            [default: ./dx_refresh_db.log]
  -h --help                 Show this screen.
  -v --version              Show version.

"""

VERSION="v.0.0.004"


from docopt import docopt
import logging
from os.path import basename
import signal
import sys
import time
import traceback
import json

from multiprocessing import Process
from time import sleep, time

from delphixpy.v1_6_0.delphix_engine import DelphixEngine
from delphixpy.v1_6_0.exceptions import HttpError, JobError
from delphixpy.v1_6_0 import job_context
from delphixpy.v1_6_0.web import database, environment, group, job, source, user
from delphixpy.v1_6_0.web.vo import OracleRefreshParameters, RefreshParameters, TimeflowPointLocation, TimeflowPointSemantic, TimeflowPointTimestamp

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
    
def find_all_databases_by_group_name(engine, server, group_name, exclude_js_container=True):
    """
    Easy way to quickly find databases by group name
    """

    #First search groups for the name specified and return its reference
    group_obj = find_obj_by_name(engine, server, group, group_name)
    if group_obj:
        databases=database.get_all(server, group=group_obj.reference, no_js_container_data_source=exclude_js_container)
        return databases

def find_database_by_name_and_group_name(engine, server, group_name, database_name):

    databases = find_all_databases_by_group_name(engine, server, group_name)

    for each in databases:
        if each.name == database_name:
            print_debug(engine["hostname"] + ": Found a match " + str(each.reference))
            return each
    print_info(engine["hostname"] + ": "Unable to find \"" + database_name + "\" in " + group_name)

def find_obj_by_name(engine, server, f_class, obj_name):
    """
    Function to find objects by name and object class, and return object's reference as a string
    You might use this function to find objects like groups.
    """
    print_debug(engine["hostname"] + ": Searching objects in the " + f_class.__name__ + " class\n   for one named \"" + obj_name +"\"")
    obj_ref = ''

    all_objs = f_class.get_all(server)
    for obj in all_objs:
        if obj.name == obj_name:
            print_debug(engine["hostname"] + ": Found a match " + str(obj.reference))
            return obj
    print_info(engine["hostname"] + ": "Unable to find a match")

def find_snapshot_by_database_and_name(engine, server, database_obj, snap_name):
    snapshots = snapshot.get_all(server, database=database_obj.reference)
    matches = []
    for snapshot_obj in snapshots:
        if str(snapshot_obj.name).startswith(arguments['--timestamp']):
            matches.append(snapshot_obj)
    if len(matches) == 1:
        print_debug(engine["hostname"] + ": Found one and only one match. This is good.")
        print_debug(engine["hostname"] + ": " + matches[0])
        return matches[0]
    elif len(matches) > 1:
        print_error("The name specified was not specific enough. More than one match found.")
        for each in matches:
            print_debug(engine["hostname"] + ": " + each.name)
    else:
        print_error("No matches found for the time specified")
    print_error("No matching snapshot found")

def find_snapshot_by_database_and_time(engine, server, database_obj, snap_time):
    snapshots = snapshot.get_all(server, database=database_obj.reference)
    matches = []
    for snapshot_obj in snapshots:
        if str(snapshot_obj.latest_change_point.timestamp).startswith(arguments['--timestamp']):
            matches.append(snapshot_obj)
    if len(matches) == 1:
        print_debug(engine["hostname"] + ": Found one and only one match. This is good.")
        print_debug(engine["hostname"] + ": " + matches[0])
        return matches[0]
    elif len(matches) > 1:
        print_error("The time specified was not specific enough. More than one match found.")
        print_debug(engine["hostname"] + ": " + matches)
    else:
        print_error("No matches found for the time specified")

def find_source_by_database(engine, server, database_obj):
    #The source tells us if the database is enabled/disables, virtual, vdb/dSource, or is a staging database.
    source_obj = source.get_all(server, database=database_obj.reference)
    #We'll just do a little sanity check here to ensure we only have a 1:1 result.
    if len(source_obj) == 0:
        print_error(engine["hostname"] + ": Did not find a source for " + database_obj.name + ". Exiting")
        sys.exit(1)
    elif len(source_obj) > 1:
        print_error(engine["hostname"] + ": More than one source returned for " + database_obj.name + ". Exiting")
        print_error(source_obj)
        sys.exit(1)
    return source_obj

def get_config(config_file_path):
    """
    This function reads in the dxtools.conf file
    """
    #First test to see that the file is there and we can open it
    try:
        config_file = open(config_file_path).read()
    except:
        print_error("Was unable to open " + config_file_path + ". Please check the path and permissions, then try again.")
        sys.exit(1)
    #Now parse the file contents as json and turn them into a python dictionary, throw an error if it isn't proper json
    try:
        config = json.loads(config_file)
    except:
        print_error("Was unable to read " + config_file_path + " as json. Please check file in a json formatter and try again.")
        sys.exit(1)
    #Create a dictionary of engines (removing the data node from the dxtools.json, for easier parsing)
    delphix_engines = {}
    for each in config['data']:
        delphix_engines[each['hostname']] = each
    print_debug(delphix_engines)
    return delphix_engines

def logging_est(logfile_path):
    """
    Establish Logging
    """
    global debug
    logging.basicConfig(filename=logfile_path,format='%(levelname)s:%(asctime)s:%(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    print_info("Welcome to " + basename(__file__) + ", version " + VERSION)
    global logger
    debug = arguments['--debug']
    logger = logging.getLogger()
    if debug == True:
        logger.setLevel(10)
        print_info("Debug Logging is enabled.")

def job_mode(server):
    """
    This function tells Delphix how to execute jobs, based on the single_thread variable at the beginning of the file
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

def job_wait():
    """
    This job stops all work in the thread/process until all jobs  on the engine are completed.
    """
    #Grab all the jos on the server (the last 25, be default)
    all_jobs = job.get_all(server)
    #For each job in the list, check to see if it is running (not ended)
    for jobobj in all_jobs:
        if not (jobobj.job_state in ["CANCELED", "COMPLETED", "FAILED"]):
            print_debug("Waiting for " + jobobj.reference + " (currently: " + jobobj.job_state+ ") to finish running against the container")
            #If so, wait
            job_context.wait(server,jobobj.reference)

@run_async
def main_workflow(engine):
    """
    This function is where we create our main workflow.
    Use the @run_async decorator to run this function asynchronously.
    The @run_async decorator allows us to run against multiple Delphix Engine simultaneously
    """

    #Pull out the values from the dictionary for this engine
    engine_address = engine["ip_address"]
    engine_username = engine["username"]
    engine_password = engine["password"]
    #Establish these variables as empty for use later
    databases = []
    environment_obj = None
    source_objs = None
    jobs = {}
    

    #Setup the connection to the Delphix Engine
    server = serversess(engine_address, engine_username, engine_password)

    #If an environment/server was specified
    if host_name:
        print_debug(engine["hostname"] + ": Getting environment for " + host_name)
        #Get the environment object by the hostname
        environment_obj = find_obj_by_name(engine, server, environment, host_name)
        if environment_obj != None:
            #Get all the sources running on the server
            env_source_objs = source.get_all(server, environment=environment_obj.reference)
            #If the server doesn't have any objects, exit.
            if env_source_objs == None:
                print_error(host_name + "does not have any objects. Exiting")
                sys.exit(1)
            #If we are only filtering by the server, then put those objects in the main list for processing
            if not(arguments['--group'] and database_name):
                source_objs = env_source_objs
                all_dbs = database.get_all(server, no_js_container_data_source=True)
                databases = []
                for source_obj in source_objs:
                    if source_obj.staging == False and source_obj.virtual == True:
                        database_obj = database.get(server, source_obj.container)
                        if database_obj in all_dbs:
                            databases.append(database_obj)
        else:
            print_error(engine["hostname"] + ":No environment found for " + host_name + ". Exiting")
            sys.exit(1)
    #If we specified a specific database by name....
    if arguments['--name']:
        #Get the database object from the name
        database_obj = find_database_by_name_and_group_name(engine, server, arguments['--group'], arguments['--name'])
        if database_obj:
            databases.append(database_obj)
    #Else if we specified a group to filter by....
    elif arguments['--group']:
        print_debug(engine["hostname"] + ":Getting databases in group " + arguments['--group'])
        #Get all the database objects in a group.
        databases = find_all_databases_by_group_name(engine, server, arguments['--group'])
    #Else, if we said all vdbs ...
    elif arguments['--all_vdbs'] and not arguments['--host'] :
        #Grab all databases, but filter out the database that are in JetStream containers,
        #because we can't refresh those this way.
        databases = database.get_all(server, no_js_container_data_source=True)
    if not databases or len(databases) == 0:
        print_error("No databases found with the criterion specified")
        return
    #reset the running job count before we begin
    i = 0
    with job_mode(server):
        #While there are still running jobs or databases still to process....
        while (len(jobs) > 0 or len(databases) > 0):
            #While there are databases still to process and we are still under 
            #the max simultaneous jobs threshold (if specified)
            while len(databases) > 0 and (arguments['--parallel'] == None or i < int(arguments['--parallel'])):
                #Give us the next database in the list, and remove it from the list
                database_obj = databases.pop()
                #Get the source of the database.
                source_obj = find_source_by_database(engine, server, database_obj)
               #If we applied the environment/server filter AND group filter, find the intersecting matches
                if environment_obj != None and (arguments['--group']):
                    match = False
                    for env_source_obj in env_source_objs:
                        if source_obj[0].reference in env_source_obj.reference:
                            match = True
                            break
                    if match == False:
                        print_error(engine["hostname"] + ": " + database_obj.name + " does not exist on " + host_name + ". Exiting")
                        return
                #Refresh the database
                refresh_job = refresh_database(engine, server, jobs, source_obj[0], database_obj)
                #If refresh_job has any value, then we know that a job was initiated.
                if refresh_job:
                    #increment the running job count
                    i += 1
            #Check to see if we are running at max parallel processes, and report if so.
            if ( arguments['--parallel'] != None and i >= int(arguments['--parallel'])):
                print_info(engine["hostname"] + ": Max jobs reached (" + str(i) + ")")

            i = update_jobs_dictionary(engine, server, jobs)
            print_info(engine["hostname"] + ": " + str(i) + " jobs running. " + str(len(databases)) + " jobs waiting to run")
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

def print_debug(print_obj):
    """
    Call this function with a log message to prefix the message with DEBUG
    """
    try:
        if debug == True:
            print "DEBUG: " + str(print_obj)
            logging.debug(str(print_obj))
    except:
        pass

def print_error(print_obj):
    """
    Call this function with a log message to prefix the message with ERROR
    """
    print "ERROR: " + str(print_obj)
    logging.error(str(print_obj))

def print_info(print_obj):
    """
    Call this function with a log message to prefix the message with INFO
    """
    print "INFO: " + str(print_obj)
    logging.info(str(print_obj))

def print_warning(print_obj):
    """
    Call this function with a log message to prefix the message with WARNING
    """
    print "WARNING: " + str(print_obj)
    logging.warning(str(print_obj))

def refresh_database(engine, server, jobs, source_obj, container_obj):
    """
    This function actually performs the refresh
    """
    #Sanity check to make sure our source object has a reference
    if source_obj.reference:
        #We can only refresh VDB's
        if source_obj.virtual != True:
            print_warning(engine["hostname"] + ": " + container_obj.name + " is not a virtual object. Skipping.")
        #Ensure this source is not a staging database. We can't act upon those.
        elif source_obj.staging == True:
            print_warning(engine["hostname"] + ": " + container_obj.name + " is a staging database. Skipping.")
        #Ensure the source is enabled. We can't refresh disabled databases.
        elif source_obj.runtime.enabled == "ENABLED" :
            source_db = database.get(server, container_obj.provision_container)
            if not source_db:
                print_error(engine["hostname"] + ":Was unable to retrieve the source container for " + container_obj.name)
            print_info(engine["hostname"] + ": Refreshing " + container_obj.name + " from " + source_db.name)
            print_debug(engine["hostname"] + ": Type: " + source_obj.type )
            print_debug(engine["hostname"] + ":" + source_obj.type)            #If the vdb is a Oracle type, we need to use a OracleRefreshParameters
            if str(container_obj.reference).startswith("ORACLE"):
                refresh_params = OracleRefreshParameters()
            else:
                refresh_params = RefreshParameters()
            
            refresh_params.timeflow_point_parameters = set_timeflow_point(engine, server, source_db)
            print_debug(engine["hostname"] + ":" + str(refresh_params))
            #Sync it
            database.refresh(server, container_obj.reference, refresh_params)
            jobs[container_obj] = server.last_job
            #return the job object to the calling statement so that we can tell if a job was created or not (will return None, if no job)
            return server.last_job
        #Don't do anything if the database is disabled
        else:
            print_warning(engine["hostname"] + ": " + container_obj.name + " is not enabled. Skipping sync")

def run_job(engine):
    """
    This function runs the main_workflow aynchronously against all the servers specified
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
        #Else if the --engine argument was given, test to see if the engine exists in dxtools.conf
        if arguments['--engine']:
            try:
                engine = dxtools_objects[arguments['--engine']]
                print_info("Executing against Delphix Engine: " + arguments['--engine'])
            except:
                print_error("Delphix Engine \"" + arguments['--engine'] + "\" cannot be found in " + config_file_path)
                print_error("Please check your value and try again. Exiting")
                sys.exit(1)
        #Else if the -d argument was given, test to see if the engine exists in dxtools.conf
        elif arguments['-d']:
            try:
                engine = dxtools_objects[arguments['-d']]
                print_info("Executing against Delphix Engine: " + arguments['-d'])
            except:
                print_error("Delphix Engine \"" + arguments['-d'] + "\" cannot be found in " + config_file_path)
                print_error("Please check your value and try again. Exiting")
                sys.exit(1)
        else:
            #Else search for a default engine in the dxtools.conf
            for delphix_engine in dxtools_objects:
                if dxtools_objects[delphix_engine]['default'] == 'true':
                    engine = dxtools_objects[delphix_engine]
                    print_info("Executing against the default Delphix Engine in the dxtools.conf: " + dxtools_objects[delphix_engine]['hostname'])
                    break
            if engine == None:
                print_error("No default engine found. Exiting")
                sys.exit(1)
        #run the job against the engine
        threads.append(main_workflow(engine))

    #For each thread in the list...
    for each in threads:
        #join them back together so that we wait for all threads to complete before moving on
        each.join()

def serversess(f_engine_address, f_engine_username, f_engine_password):
    """
    Function to setup the session with the Delphix Engine
    """
    server_session= DelphixEngine(f_engine_address, f_engine_username, f_engine_password, "DOMAIN")
    return server_session

def set_exit_handler(func):
    """
    This function helps us set the correct exit code
    """
    signal.signal(signal.SIGTERM, func)

def set_timeflow_point(engine, server, container_obj):
    """
    This returns the reference of the timestamp specified.
    """
    if arguments['--timestamp_type'].upper() == "SNAPSHOT":
        if arguments['--timestamp'].upper() == "LATEST":
            print_debug(engine["hostname"] + ": Using the latest Snapshot")
            timeflow_point_parameters = TimeflowPointSemantic()
            timeflow_point_parameters.location = "LATEST_SNAPSHOT"
        elif arguments['--timestamp'].startswith("@"):
            print_debug(engine["hostname"] + ": Using a named snapshot")
            snapshot_obj = find_snapshot_by_database_and_name(engine, server, container_obj, arguments['--timestamp'])
            if snapshot_obj != None:
                timeflow_point_parameters=TimeflowPointLocation()
                timeflow_point_parameters.timeflow = snapshot_obj.timeflow
                timeflow_point_parameters.location = snapshot_obj.latest_change_point.location
            else:
                print_error("Was unable to use the specified snapshot\"" + arguments['--timestamp'] + "\" for database \"" + container_obj.name + "\"")
                return
        else:
            print_debug(engine["hostname"] + ": Using a time-designated snapshot")
            snapshot_obj = find_snapshot_by_database_and_time(engine, server, container_obj, arguments['--timestamp'])
            if snapshot_obj != None:
                timeflow_point_parameters=TimeflowPointTimestamp()
                timeflow_point_parameters.timeflow = snapshot_obj.timeflow
                timeflow_point_parameters.timestamp = snapshot_obj.latest_change_point.timestamp
            else:
                print_error("Was unable to find a suitable time for " + arguments['--timestamp'] + " for database " + container_obj.name)
                return
    elif arguments['--timestamp_type'].upper() == "TIME":
        if arguments['--timestamp'].upper() == "LATEST":
            timeflow_point_parameters = TimeflowPointSemantic()
            timeflow_point_parameters.location = "LATEST_POINT"
        else:
            print_error("Only support a --timestamp value of \"latest\" when used with timestamp_type of time")
            return
    else:
        print_error(arguments['--timestamp_type'] + " is not a valied timestamp_type. Exiting")
        sys.exit(1)
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
    This function checks each job in the dictionary and updates its status or removes it if the job is complete.
    Return the number of jobs still running.
    """
    #Establish the running jobs counter, as we are about to update the count from the jobs report.
    i = 0
    #get all the jobs, then inspect them
    for j in jobs.keys():
        job_obj = job.get(server, jobs[j])
        print_debug(engine["hostname"] + ": " + str(job_obj))
        print_info(engine["hostname"] + ": " + j.name + ": " + job_obj.job_state)
        
        if job_obj.job_state in ["CANCELED", "COMPLETED", "FAILED"]:
            #If the job is in a non-running state, remove it from the running jobs list.
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

    

    try:
        #Declare globals that will be used throughout the script.
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
        
        #This is the function that will handle processing main_workflow for all the servers.
        run_job(engine)
        
        elapsed_minutes = time_elapsed()
        print_info("script took " + str(elapsed_minutes) + " minutes to get this far.")


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
        We use this exception handler when a job fails in Delphix so that we have actionable data
        """
        print_error("A job failed in the Delphix Engine")
        print_error(e.job)
        elapsed_minutes = time_elapsed()
        print_info(basename(__file__) + " took " + str(elapsed_minutes) + " minutes to get this far.")
        sys.exit(3)
    except KeyboardInterrupt:
        """
        We use this exception handler to gracefully handle ctrl+c exits
        """
        print_debug("You sent a CTRL+C to interrupt the process")
        elapsed_minutes = time_elapsed()
        print_info(basename(__file__) + " took " + str(elapsed_minutes) + " minutes to get this far.")
    except:
        """
        Everything else gets caught here
        """
        print_error(sys.exc_info()[0])
        print_error(traceback.format_exc())
        elapsed_minutes = time_elapsed()
        print_info(basename(__file__) + " took " + str(elapsed_minutes) + " minutes to get this far.")
        sys.exit(1)

if __name__ == "__main__":
    #Grab our arguments from the doc at the top of the script
    arguments = docopt(__doc__, version=basename(__file__) + " " + VERSION)

    #Feed our arguments to the main function, and off we go!
    print arguments
    main(arguments)