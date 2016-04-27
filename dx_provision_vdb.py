#!/usr/bin/env python
#Adam Bowen - Apr 2016
#This script provisions a vdb or dSource
#requirements
#pip install docopt delphixpy

#The below doc follows the POSIX compliant standards and allows us to use 
#this doc to also define our arguments for the script. This thing is brilliant.
"""Provision VDB's

Usage:
  dx_provision_db.py --source_grp <name> --source <name> --target_grp <name> --target <name>
                  (--db <name> | --vfiles_path <path>) [--no_truncate_log]
                  (--environment <name> --type <type> --envinst <name>)
                  [--sourcegroup <name>] [--template <name>] [--mapfile <file>]
                  [--timestamp_type <type>] [--timestamp <timepoint_semantic>]
                  [--instname <sid>] [--mntpoint <path>] [--noopen]
                  [--uniqname <name>] [--post_refresh_script <path>]
                  [--pre_refresh_script <path>] [--postscript <path>]
                  [--configure_clone_script <path>] [--prescript <path>]
                  [-d <identifier> | --engine <identifier> | --all]
                  [--debug] [--parallel <n>] [--poll <n>]
                  [--config <path_to_file>] [--logdir <path_to_file>]
  dx_provision_db.py -h | --help | -v | --version

Provision VDB from a defined source on the defined target environment.

Examples:
  dx_provision_vdb.py -d landsharkengine --source_grp Sources --source "ASE pubs3 DB" --db \
      vase --target testASE --target_grp Analytics --environment \
      LINUXTARGET --type ase --envinst "LINUXTARGET"
  dx_provision_vdb.py --source_grp Sources --source "Employee Oracle 11G DB" --instname \
      autod --uniqname autoprod --db autoprod --target autoprod --target_grp \
      Analytics --environment LINUXTARGET --type oracle --envinst \
      "/u01/app/oracle/product/11.2.0/dbhome_1"
  dx_provision_vdb.py --source_grp Sources --source "AdventureWorksLT2008R2" --db \
      vAW --target testAW --target_grp Analytics --environment \
      WINDOWSTARGET --type mssql --envinst MSSQLSERVER --all


Options:
  --source_grp <name>       The group where the source resides.
  --source <name>           The name of the source object from which you are creating
                            your vdb.
  --target_grp <name>       The group into which Delphix will place the VDB.
  --target <name>           The unique name that you want to call this object
                            in Delphix
  --db <name>               The name you want to give the database (Oracle Only)
  --vfiles_path <path>      The full path on the Target server where Delphix
                            will provision the vFiles
  --no_truncate_log         Don't truncate log on checkpoint (ASE only)
  --environment <name>      The name of the Target environment in Delphix
  --type <type>             The type of VDB this is.
                            oracle | mssql | ase | vfiles
  --envinst <name>          The identifier of the instance in Delphix.
                            ex. "/u01/app/oracle/product/11.2.0/dbhome_1"
                            ex. LINUXTARGET
  --sourcegroup <name>      The group the source from which you are creating
                            your vdb.
  --timestamp_type <type>   The type of timestamp you are specifying.
                            Acceptable Values: TIME, SNAPSHOT
                            [default: SNAPSHOT]
  --timestamp <timepoint_semantic>
                            The Delphix semantic for the point in time from
                            which you want to provision your VDB.
                            Formats:
                            latest point in time or snapshot: LATEST
                            point in time: "YYYY-MM-DD HH24:MI:SS"
                            snapshot name: "@YYYY-MM-DDTHH24:MI:SS.ZZZ"
                            snapshot time from GUI: "YYYY-MM-DD HH24:MI"
                            [default: LATEST]
  --template <name>         Target VDB Template name (Oracle Only)
  --mapfile <file>          Target VDB mapping file (Oracle Only)
  --instname <sid>          Target VDB SID name (Oracle Only)
  --uniqname <name>         Target VDB db_unique_name (Oracle Only)
  --mntpoint <path>         Mount point for the VDB
                            [default: /mnt/provision]
  --noopen                  Don't open database after provision (Oracle Only)
  --post_refresh_script <path>  Add script from path as postrefresh hook 
                                (*nix only)
  --pre_refresh_script <path>  prerefresh Add script from path as prerefresh
                               hook (*nix only)
  --configure_clone_script <path>  Add script from path as configureclone
                                   hook (*nix only)
  --prescript <path>        Path to pre script on target machine (Windows Only)
  --postscript <path>       Path to post script on target machine (Windows Only)
  -d <identifier>           Identifier of Delphix engine in dxtools.conf.
  --engine <type>           Alt Identifier of Delphix engine in dxtools.conf.
  --all                     Run against all engines.
  --debug                   Enable debug logging
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./dxtools.conf]
  --logdir <path_to_file>    The path to the logfile you want to use.
                            [default: ./dx_provision_db.log]
  -h --help                 Show this screen.
  -v --version              Show version.

"""

VERSION="v.0.0.006"

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
from delphixpy.v1_6_0.web import database, environment, group, host, job, repository, snapshot, source, user
from delphixpy.v1_6_0.web.vo import OracleDatabaseContainer, OracleInstance, OracleProvisionParameters, OracleSIConfig, OracleVirtualSource, \
TimeflowPointLocation, TimeflowPointSemantic, TimeflowPointTimestamp, ASEDBContainer, ASEInstanceConfig, ASEProvisionParameters, \
ASESIConfig, ASEVirtualSource, MSSqlProvisionParameters, MSSqlDatabaseContainer, MSSqlVirtualSource, MSSqlInstanceConfig, MSSqlInstance, MSSqlSIConfig

def create_ase_vdb(engine, server, jobs, vdb_group, vdb_name, environment_obj, container_obj):
    '''
    Create a Sybase ASE VDB
    '''
    vdb_obj = find_database_by_name_and_group_name(engine, server, vdb_group.name, vdb_name)
    if vdb_obj == None:
        vdb_params = ASEProvisionParameters()
        vdb_params.container = ASEDBContainer()
        if arguments['--no_truncate_log']:
            vdb_params.truncate_log_on_checkpoint = False
        else:
            vdb_params.truncate_log_on_checkpoint = True
        vdb_params.container.group = vdb_group.reference
        vdb_params.container.name = vdb_name
        vdb_params.source = ASEVirtualSource()
        vdb_params.source_config = ASESIConfig()
        vdb_params.source_config.database_name = arguments['--db']
        vdb_params.source_config.instance = ASEInstanceConfig()
        vdb_params.source_config.instance.host = environment_obj.host

        vdb_repo = find_dbrepo_by_environment_ref_and_name(engine, server, "ASEInstance", environment_obj.reference, arguments['--envinst'])
        vdb_params.source_config.repository = vdb_repo.reference

        vdb_params.timeflow_point_parameters = set_timeflow_point(engine, server, container_obj)
        vdb_params.timeflow_point_parameters.container = container_obj.reference
        print_info("Provisioning " + vdb_name)
        database.provision(server, vdb_params)
        #Add the job into the jobs dictionary so we can track its progress
        jobs[engine["hostname"]] = server.last_job
        #return the job object to the calling statement so that we can tell if a job was created or not (will return None, if no job)
        return server.last_job
    else:
        print_info(engine["hostname"] + ": " + vdb_name + " already exists.")
        return vdb_obj.reference

def create_mssql_vdb(engine, server, jobs, vdb_group, vdb_name, environment_obj, container_obj):
    '''
    Create a MSSQL VDB
    '''
    vdb_obj = find_database_by_name_and_group_name(engine, server, vdb_group.name, vdb_name)
    if vdb_obj == None:
        vdb_params = MSSqlProvisionParameters()
        vdb_params.container = MSSqlDatabaseContainer()
        vdb_params.container.group = vdb_group.reference
        vdb_params.container.name = vdb_name
        vdb_params.source = MSSqlVirtualSource()
        vdb_params.source_config = MSSqlSIConfig()
        vdb_params.source_config.database_name = arguments['--db']
        vdb_params.source_config.instance = MSSqlInstanceConfig()
        vdb_params.source_config.instance.host = environment_obj.host

        vdb_repo = find_dbrepo_by_environment_ref_and_name(engine, server, "MSSqlInstance", environment_obj.reference, arguments['--envinst'])
        vdb_params.source_config.repository = vdb_repo.reference

        vdb_params.timeflow_point_parameters = set_timeflow_point(engine, server, container_obj)
        if not vdb_params.timeflow_point_parameters:
            return
        vdb_params.timeflow_point_parameters.container = container_obj.reference
        print_info(engine["hostname"] + ":Provisioning " + vdb_name)
        database.provision(server, vdb_params)
        #Add the job into the jobs dictionary so we can track its progress
        jobs[engine["hostname"]] = server.last_job
        #return the job object to the calling statement so that we can tell if a job was created or not (will return None, if no job)
        return server.last_job
    else:
        print_info(engine["hostname"] + ": " + vdb_name + " already exists.")
        return vdb_obj.reference

def create_oracle_si_vdb(engine, server, jobs, vdb_group, vdb_name, environment_obj, container_obj):
    '''
    Create an Oracle SI VDB
    '''
    vdb_obj = find_database_by_name_and_group_name(engine, server, vdb_group.name, vdb_name)
    if vdb_obj == None:
        vdb_params = OracleProvisionParameters()
        if arguments['--noopen']:
            vdb_params.open_resetlogs = False
        vdb_params.container = OracleDatabaseContainer()
        vdb_params.container.group = vdb_group.reference
        vdb_params.container.name = vdb_name
        vdb_params.source = OracleVirtualSource()
        vdb_params.source.mount_base = arguments['--mntpoint']
        if arguments['--mapfile']:
            vdb_params.source.file_mapping_rules = arguments['--mapfile']
        if arguments['--template']:
                template_obj = find_obj_by_name(engine, server, database.template, arguments['--template'])
                vdb_params.source.config_template = template_obj.reference
        vdb_params.source_config = OracleSIConfig()
    

        vdb_repo = find_dbrepo_by_environment_ref_and_install_path(engine, server, "OracleInstall", environment_obj.reference, arguments['--envinst'])
        vdb_params.source_config.database_name = arguments['--db']
        vdb_params.source_config.unique_name = arguments['--uniqname']
        vdb_params.source_config.instance = OracleInstance()
        vdb_params.source_config.instance.instance_name = arguments['--instname']
        vdb_params.source_config.instance.instance_number = 1
        vdb_params.source_config.repository = vdb_repo.reference

        vdb_params.timeflow_point_parameters = set_timeflow_point(engine, server, container_obj)
        vdb_params.timeflow_point_parameters.container = container_obj.reference
        print_info(engine["hostname"] + ": Provisioning " + vdb_name)
        database.provision(server, vdb_params)
        #Add the job into the jobs dictionary so we can track its progress
        jobs[engine["hostname"]] = server.last_job
        #return the job object to the calling statement so that we can tell if a job was created or not (will return None, if no job)
        return server.last_job
    else:
        print_info(engine["hostname"] + ":" + vdb_name + " already exists.")
        return vdb_obj.reference

def find_all_databases_by_group_name(engine, server, group_name, exclude_js_container=False):
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
    print_info(engine["hostname"] + ": Unable to find \"" + database_name + "\" in " + group_name)

def find_dbrepo_by_environment_ref_and_install_path(engine, server, install_type, f_environment_ref, f_install_path):
    '''
    Function to find database repository objects by environment reference and install path, and return the object's reference as a string
    You might use this function to find Oracle and PostGreSQL database repos.
    '''
    print_debug(engine["hostname"] + ": Searching objects in the " + install_type + " class for one with the environment reference of \"" + f_environment_ref +"\"\n   and an install path of \"" + f_install_path + "\"")
    obj_ref = ''
    all_objs = repository.get_all(server, environment=f_environment_ref)
    for obj in all_objs:
        if install_type == "PgSQLInstall":
            if (obj.type == install_type and obj.installation_path == f_install_path):
                print_debug(engine["hostname"] + ": Found a match " + str(obj.reference))
                return obj
        elif install_type == "OracleInstall":
            if (obj.type == install_type and obj.installation_home == f_install_path):
                print_debug(engine["hostname"] + ": Found a match " + str(obj.reference))
                return obj
        elif install_type == "MySQLInstall":
            if (obj.type == install_type and obj.installation_path == f_install_path):
                print_debug(engine["hostname"] + ": Found a match " + str(obj.reference))
                return obj
        else:
            print_error(engine["hostname"] + ": No Repo match found for type " + install_type)

def find_dbrepo_by_environment_ref_and_name(engine, server, repo_type, f_environment_ref, f_name):
    '''
    Function to find database repository objects by environment reference and name, and return the object's reference as a string
    You might use this function to find MSSQL database repos.
    '''
    print_debug(engine["hostname"] + ": Searching objects in the " + repo_type + " class for one with the environment reference of \"" + f_environment_ref +"\"\n   and a name of \"" + f_name + "\"")
    obj_ref = ''
    all_objs = repository.get_all(server, environment=f_environment_ref)
    for obj in all_objs:
        if repo_type == "MSSqlInstance" or repo_type == "ASEInstance":
            if (obj.type == repo_type and obj.name == f_name):
                print_debug(engine["hostname"] + ": Found a match: " + str(obj.reference))
                return obj
    print_error(engine["hostname"] + ": No Repo match found for type " + repo_type)

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

def find_snapshot_by_database_and_name(engine, server, database_obj, snap_name):
    snapshots = snapshot.get_all(server, database=database_obj.reference)
    matches = []
    for snapshot_obj in snapshots:
        if str(snapshot_obj.name).startswith(arguments['--timestamp']):
            matches.append(snapshot_obj)
    if len(matches) == 1:
        print_debug(engine["hostname"] + ": Found one and only one match. This is good.")
        print_debug(matches[0])
        return matches[0]
    elif len(matches) > 1:
        print_error(engine["hostname"] + ": The name specified was not specific enough. More than one match found.")
        for each in matches:
            print_debug(each.name)
    else:
        print_error(engine["hostname"] + ": No matches found for the time specified")
    print_error(engine["hostname"] + ": No matching snapshot found")

def find_snapshot_by_database_and_time(engine, server, database_obj, snap_time):
    snapshots = snapshot.get_all(server, database=database_obj.reference)
    matches = []
    for snapshot_obj in snapshots:
        if str(snapshot_obj.latest_change_point.timestamp).startswith(arguments['--timestamp']):
            matches.append(snapshot_obj)
    if len(matches) == 1:
        print_debug(engine["hostname"] + ": Found one and only one match. This is good.")
        print_debug(matches[0])
        return matches[0]
    elif len(matches) > 1:
        print_error(engine["hostname"] + ": The time specified was not specific enough. More than one match found.")
        print_debug(matches)
    else:
        print_error(engine["hostname"] + ": No matches found for the time specified")

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
    This job stops all work in the thread/process until jobs are completed.
    """
    #Grab all the jos on the server (the last 25, be default)
    all_jobs = job.get_all(server)
    #For each job in the list, check to see if it is running (not ended)
    for jobobj in all_jobs:
        if not (jobobj.job_state in ["CANCELED", "COMPLETED", "FAILED"]):
            print_debug("Waiting for " + jobobj.reference + " (currently: " + jobobj.job_state+ ") to finish running against the container")
            #If so, wait
            job_context.wait(server,jobobj.reference)

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
    """

    #Pull out the values from the dictionary for this engine
    engine_address = engine["ip_address"]
    engine_username = engine["username"]
    engine_password = engine["password"]
    #Establish these variables as empty for use later
    environment_obj = None
    source_objs = None
    jobs = {}
    
    #Setup the connection to the Delphix Engine
    server = serversess(engine_address, engine_username, engine_password)

    #Get the group by name
    group_obj = find_obj_by_name(engine, server, group, arguments['--target_grp'])

    #Get the reference of the target environment.
    print_debug("Getting environment for " + host_name)
    #Get the environment object by the hostname
    environment_obj = find_obj_by_name(engine, server, environment, host_name)
    if environment_obj == None:
        print_error(engine["hostname"] + ":No environment found for " + host_name + ". Exiting")
        sys.exit(1)

    #Get the database reference we are copying from the database name
    database_obj = find_database_by_name_and_group_name(engine, server, arguments['--source_grp'], arguments['--source'])
    if database_obj == None:
        return
    thingstodo = ["thingtodo"]
    #reset the running job count before we begin
    i = 0
    with job_mode(server):
        while (len(jobs) > 0 or len(thingstodo)> 0):
            if len(thingstodo)> 0:
                if arguments['--type'].lower() == "oracle":
                    create_oracle_si_vdb(engine, server, jobs, group_obj, database_name, environment_obj , database_obj)
                elif arguments['--type'].lower() == "ase":
                    create_ase_vdb(engine, server, jobs, group_obj, database_name, environment_obj , database_obj)
                elif arguments['--type'].lower() == "mssql":
                    create_mssql_vdb(engine, server, jobs, group_obj, database_name, environment_obj , database_obj)
                thingstodo.pop()
            #get all the jobs, then inspect them
            i = 0
            for j in jobs.keys():
                job_obj = job.get(server, jobs[j])
                print_debug(job_obj)
                print_info(engine["hostname"] + ": VDB Provision: " + job_obj.job_state)
                
                if job_obj.job_state in ["CANCELED", "COMPLETED", "FAILED"]:
                    #If the job is in a non-running state, remove it from the running jobs list.
                    del jobs[j]
                else:
                    #If the job is in a running state, increment the running job count.
                    i += 1
            print_info(engine["hostname"] + ": " + str(i) + " jobs running. ")
            #If we have running jobs, pause before repeating the checks.
            if len(jobs) > 0:
                sleep(float(arguments['--poll']))

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
                print_error(engine["hostname"] + ": Was unable to use the specified snapshot\"" + arguments['--timestamp'] + "\" for database \"" + container_obj.name + "\"")
                return
        else:
            print_debug(engine["hostname"] + ": Using a time-designated snapshot")
            snapshot_obj = find_snapshot_by_database_and_time(engine, server, container_obj, arguments['--timestamp'])
            if snapshot_obj != None:
                timeflow_point_parameters=TimeflowPointTimestamp()
                timeflow_point_parameters.timeflow = snapshot_obj.timeflow
                timeflow_point_parameters.timestamp = snapshot_obj.latest_change_point.timestamp
            else:
                print_error(engine["hostname"] + ": Was unable to find a suitable time for " + arguments['--timestamp'] + " for database " + container_obj.name)
                return
    elif arguments['--timestamp_type'].upper() == "TIME":
        if arguments['--timestamp'].upper() == "LATEST":
            timeflow_point_parameters = TimeflowPointSemantic()
            timeflow_point_parameters.location = "LATEST_POINT"
        else:
            print_error(engine["hostname"] + ": Only support a --timestamp value of \"latest\" when used with timestamp_type of time")
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
    global config_file_path
    global database_name
    global host_name
    global dxtools_objects

    

    try:
        logging_est(arguments['--logdir'])
        print_debug(arguments)
        time_start = time()
        engine = None
        single_thread = False
        config_file_path = arguments['--config']
        #Parse the dxtools.conf and put it into a dictionary
        dxtools_objects = get_config(config_file_path)

        database_name = arguments['--target']
        host_name = arguments['--environment']


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
    main(arguments)