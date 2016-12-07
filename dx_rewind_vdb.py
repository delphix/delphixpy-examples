#!/usr/bin/env python
#Corey Brune - Sep 2016
#This script performs a rewind of a vdb
#requirements
#pip install --upgrade setuptools pip docopt delphixpy

#The below doc follows the POSIX compliant standards and allows us to use 
#this doc to also define our arguments for the script.

"""Rewinds a vdb
Usage:
  dx_rewind_vdb.py (--vdb <name> [--timestamp_type <type>] [--timestamp <timepoint_semantic>])
                   [--bookmark <type>] 
                   [ --engine <identifier>]
                   [--debug] [--parallel <n>] [--poll <n>]
                   [--config <path_to_file>] [--logdir <path_to_file>]
  dx_rewind_vdb.py -h | --help | -v | --version

Rewinds a Delphix VDB
Examples:
  dx_rewind_vdb.py --vdb testVdbUF --timestamp_type snapshot --timestamp 2016-11-15T11:30:17.857Z
  

Options:
  --vdb <name>              Name of VDB to rewind
  --type <database_type>    Type of database: oracle, mssql, ase
  --timestamp_type <type>   The type of timestamp you are specifying.
                            Acceptable Values: TIME, SNAPSHOT
                            [default: SNAPSHOT]
  --timestamp <timepoint_semantic>
                            The Delphix semantic for the point in time on
                            the source from which you want to rewind your VDB.
                            Formats:
                            latest point in time or snapshot: LATEST
                            point in time: "YYYY-MM-DD HH24:MI:SS"
                            snapshot name: "@YYYY-MM-DDTHH24:MI:SS.ZZZ"
                            snapshot time from GUI: "YYYY-MM-DD HH24:MI"
                            [default: LATEST]
  --engine <type>           Alt Identifier of Delphix engine in dxtools.conf.
  --debug                   Enable debug logging
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./dxtools.conf]
  --logdir <path_to_file>    The path to the logfile you want to use.
                            [default: ./dx_rewind_vdb.log]
  -h --help                 Show this screen.
  -v --version              Show version.
"""

VERSION = "v.0.2.000"


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

from delphixpy.exceptions import HttpError
from delphixpy.exceptions import JobError
from delphixpy.exceptions import RequestError
from delphixpy import job_context
from delphixpy.web import database
from delphixpy.web import environment
from delphixpy.web import group
from delphixpy.web import job
from delphixpy.web import source
from delphixpy.web import user
from delphixpy.web.vo import RollbackParameters
from delphixpy.web.vo import OracleRollbackParameters

from lib.DlpxException import DlpxException
from lib.DxTimeflow import DxTimeflow
from lib.GetReferences import find_obj_by_name
from lib.GetSession import GetSession
from lib.DxLogging import logging_est
from lib.DxLogging import print_info
from lib.DxLogging import print_debug
from lib.DxLogging import print_warning


def main_workflow(engine):
    """
    This function is where we create our main workflow.
    Use the @run_async decorator to run this function asynchronously.
    The @run_async decorator allows us to run against multiple Delphix Engine 
    simultaneously

    engine: Dictionary containing engine(s) and threads.
    """

    #Establish these variables as empty for use later
    environment_obj = None
    source_objs = None
    jobs = {}
    
    try:
        #Setup the connection to the Delphix Engine
        dx_session_obj.serversess(engine['ip_address'], engine['username'], 
                                  engine['password'])

        database_obj = find_obj_by_name(dx_session_obj.server_session,
                                        database, arguments['--vdb'])

        rewind_database(engine, dx_session_obj.server_session, jobs, 
                        database_obj)

    except DlpxException as e:
        raise DlpxException(e)


def rewind_database(engine, server, jobs, container_obj):
    """
    This function performs the rewind (rollback)

    engine: Dictionary containing engine(s) and threads
    server: Delphix session object
    jobs: Dictionary containing a list of running jobs
    container_obj: VDB object to be rewound
    """

    dx_timeflow_obj = DxTimeflow(server)

    #Sanity check to make sure our container object has a reference
    if container_obj.reference:

        try:
            if container_obj.virtual is not True:
                raise DlpxException('%s in engine %s not a virtual object. '
                                    'Skipping.\n' % (container_obj.name,
                                    engine['hostname']))

            elif container_obj.staging is True:
                raise DlpxException('%s in engine %s is a virtual object. '
                                    'Skipping.\n' % (container_obj.name,
                                    engine['hostname']))

            elif container_obj.runtime.enabled == "ENABLED":
                print_info('\nINFO: %s Rewinding %s to %s\n' %
                           (engine["hostname"], container_obj.name,
                           arguments['--timestamp']))

        #This exception is raised if rewinding a vFiles VDB
        # since AppDataContainer does not have virtual, staging or
        # enabled attributes.
        except AttributeError:
            pass

            print_debug(engine["hostname"] + ": Type: " + container_obj.type )
            print_debug(engine["hostname"] + ":" + container_obj.type)

            #If the vdb is a Oracle type, we need to use a
            # OracleRollbackParameters
            if str(container_obj.reference).startswith("ORACLE"):
                rewind_params = OracleRollbackParameters()
            else:
                rewind_params = RollbackParameters()

            rewind_params.timeflow_point_parameters = \
                          dx_timeflow_obj.set_timeflow_point(container_obj,
                                              arguments['--timestamp_type'],
                                              arguments['--timestamp'])

            print_debug(engine["hostname"] + ":" + str(rewind_params))

            try:
                #Rewind the VDB
                database.rollback(server, container_obj.reference,
                                  rewind_params)
                jobs[container_obj] = server.last_job

            except (RequestError, HttpError, JobError) as e:
                raise DlpxException('\nERROR: %s encountered an error on %s'
                                    ' during the rewind process:\n%s\n' %
                                    (engine['hostname'], container_obj.name,
                                    e))

            #return the job object to the calling statement so that we can
            # tell if a job was created or not (will return None, if no job)
            return server.last_job

        #Don't do anything if the database is disabled
        else:
            print_warning(engine["hostname"] + ": " + container_obj.name +
                          " is not enabled. Skipping sync")


def run_job():
    """
    This function runs the main_workflow aynchronously against all the
    servers specified

    No arguments required for run_job().
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

        if engine == None:
            raise DlpxException("\nERROR: No default engine found. Exiting.\n")

        #run the job against the engine
        main_workflow(engine)


def time_elapsed():
    """
    This function calculates the time elapsed since the beginning of the script.
    Call this anywhere you want to note the progress in terms of time
    """
    elapsed_minutes = round((time() - time_start)/60, +1)
    return elapsed_minutes


def main(argv):
    #We want to be able to call on these variables anywhere in the script.
    global time_start
    global host_name
    global database_name
    global config_file_path
    global dx_session_obj

    try:
        #Declare globals that will be used throughout the script.
        dx_session_obj = GetSession()
        logging_est(arguments['--logdir'])
        print_debug(arguments)
        time_start = time()
        database_name = arguments['--vdb']
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
        sys.exit(e)

    except DlpxException as e:
        print('\nERROR: Encountered DlpxException during rewind. '
              'Reason:\n%s' % (e))
        sys.exit(1)

    except RequestError as e:
        raise DlpxException('\nERROR: RequestError while executing rewind.'
                            ' Reason:\n%s' % (e))

    except HttpError as e:
        raise DlpxException('\nERROR: Connection failed to the '
                            'Delphix Engine. Reason:\n%s' % (e))

    except JobError as e:
        elapsed_minutes = time_elapsed()
        print_info(basename(__file__) + " took " + str(elapsed_minutes) + 
                   " minutes to get this far.")
        raise DlpxException('\nERROR: A job failed in the Delphix Engine:\n'
                            '%s\n' % (e.job))

    except KeyboardInterrupt:
        """
        We use this exception handler to gracefully handle ctrl+c exits
        """
        print_debug("You sent a CTRL+C to interrupt the process")
        elapsed_minutes = time_elapsed()
        print_info(basename(__file__) + " took " + str(elapsed_minutes) + 
                   " minutes to get this far.")


if __name__ == "__main__":
    #Grab our arguments from the doc at the top of the script
    arguments = docopt(__doc__, version=basename(__file__) + " " + VERSION)

    #Feed our arguments to the main function, and off we go!
    main(arguments)
