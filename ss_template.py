#!/usr/bin/env python3
# Program Name : ss_template.py
# Description  : Delphix implementation script
# Author       : Corey Brune
#
# Copyright (c) 2019 by Delphix.
# All rights reserved.
# See http://docs.delphix.com/display/PS/Copyright+Statement for details
#
# Delphix Support statement available at
# See http://docs.delphix.com/display/PS/PS+Script+Support+Policy for details
#
# Warranty details provided in external file
# for customers who have purchased support.
#
"""Creates, deletes and lists SS templates.
Usage:
  ss_template.py (--create_template <name> --database <name> | --list_templates
  | --delete_template <name>)
  [--engine <identifier> | --all] [--parallel <n>]
  [--poll <n>] [--debug][--single_thread <bool>]
  [--config <path_to_file>] [--logdir <path_to_file>]
  ss_template.py -h | --help | -v | --version

Creates, Lists, Removes a Self-Service Template

Examples:
  ss_template.py --list_templates
  ss_template.py --create_template jstemplate1 --database <name>
  ss_template.py --create_template jstemplate2 --database <name:name:name>
  ss_template.py --delete_template jstemplate1

Options:
  --create_template <name>  Name of the new SS Template
  --delete_template <name>  Delete the SS Template
  --database <name>         Name of the database(s) to use for the SS Template
                                Note: If adding multiple template DBs, use a
                                comma (:) to delineate between the DB names.
  --list_templates          List the templates on a given engine
  --single_thread <boolean> Run as a single thread. Use True if there are
                            multiple engines and the operation needs to run
                            in parallel.
                            [default: True]
  --engine <type>           Alt Identifier of Delphix engine in dxtools.conf.
                            [default: default]
  --all                     Run against all engines.
  --debug                   Enable debug logging
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./config/dxtools.conf]
  --logdir <path_to_file>   The path to the logfile you want to use.
                            [default: ./logs/ss_template.log]
  -h --help                 Show this screen.
  -v --version              Show version.
"""

import sys
import time
from os.path import basename

from docopt import docopt

from delphixpy.v1_10_2 import exceptions
from delphixpy.v1_10_2.web import database
from delphixpy.v1_10_2.web import selfservice
from delphixpy.v1_10_2.web import vo
from lib import dlpx_exceptions
from lib import dx_logging
from lib import get_references
from lib import get_session
from lib import run_job
from lib.run_async import run_async

VERSION = "v.0.3.001"


def create_template(engine, dlpx_obj, template_name, database_name):
    """
    Create the SS Template
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param template_name: Name of the template to create
    :type template_name: str
    :param database_name: Name of the database(s) to use in the template
    :type database_name: str
    """
    ss_template_params = vo.JSDataTemplateCreateParameters()
    ss_template_params.name = template_name
    template_ds_lst = []
    template_ref = ""
    # engine_name = dlpx_obj.dlpx_ddps["engine_name"]
    for data_set in database_name.split(":"):
        template_ds_lst.append(
            get_references.build_data_source_params(dlpx_obj, database, data_set)
        )
    ss_template_params.data_sources = template_ds_lst
    try:
        template_ref = selfservice.template.create(
            dlpx_obj.server_session, ss_template_params
        )
    except (
        dlpx_exceptions.DlpxException,
        exceptions.RequestError,
        exceptions.HttpError,
    ) as err:
        dx_logging.print_exception(
            f"The template {template_name} was not created:\n{err}"
        )
        raise dlpx_exceptions.DlpxException(err)
    dlpx_obj.jobs[dlpx_obj.server_session.address] = dlpx_obj.server_session.last_job
    return template_ref


def list_templates(dlpx_obj):
    """
    List all templates on a given engine
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    """
    header = "Name, Reference, Active Branch, Last Updated"
    try:
        ss_templates = selfservice.template.get_all(dlpx_obj.server_session)
        if not ss_templates:
            dx_logging.print_info(f"No Self Service templates on engine")
        else:
            dx_logging.print_info(header)
            for ss_template in ss_templates:
                last_updated = get_references.convert_timestamp(
                    dlpx_obj.server_session, ss_template.last_updated[:-5]
                )
                dx_logging.print_info(
                    f"{ss_template.name}, {ss_template.reference},"
                    f"{ss_template.active_branch},{last_updated}"
                )
    except (
        dlpx_exceptions.DlpxException,
        exceptions.HttpError,
        exceptions.RequestError,
    ) as err:
        raise dlpx_exceptions.DlpxException(
            f"ERROR: The templates could not be listed:\n{err}"
        )


def delete_template(dlpx_obj, template_name):
    """
    Deletes a template
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param template_name: Name of the template to delete
    :type template_name: str
    """
    try:
        template_ref = get_references.find_obj_by_name(
            dlpx_obj.server_session, selfservice.template, template_name
        ).reference
        selfservice.template.delete(dlpx_obj.server_session, template_ref)
    except (dlpx_exceptions.DlpxObjectNotFound) as err:
        dx_logging.print_exception(f"The template {template_name} not found")
        raise dlpx_exceptions.DlpxObjectNotFound(
            f"The template {template_name} not found"
        )
    except (
        dlpx_exceptions.DlpxException,
        exceptions.HttpError,
        exceptions.RequestError,
    ) as err:
        dx_logging.print_exception(
            f"\nERROR: The template {template_name} " f"was not deleted:\n{err}"
        )
        raise dlpx_exceptions.DlpxException(err)


@run_async
def main_workflow(engine, dlpx_obj, single_thread):
    """
    This function is where we create our main workflow.
    Use the @run_async decorator to run this function asynchronously.
    The @run_async decorator allows us to run against multiple Delphix Engine
    simultaneously
    :param engine: Dictionary of engines in the config file
    :type engine: dict
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param single_thread: True - run single threaded, False - run multi-thread
    :type single_thread: bool
    """
    try:
        # Setup the connection to the Delphix Engine
        dlpx_obj.dlpx_session(
            engine["ip_address"], engine["username"], engine["password"]
        )
    except dlpx_exceptions.DlpxObjectNotFound as err:
        dx_logging.print_exception(
            f'ERROR: Delphix Engine {engine["ip_address"]} encountered '
            f"an error while creating the session:\n{err}\n"
        )
    try:
        with dlpx_obj.job_mode(single_thread):
            if ARGUMENTS["--create_template"]:
                template_ref = create_template(
                    engine,
                    dlpx_obj,
                    ARGUMENTS["--create_template"],
                    ARGUMENTS["--database"],
                )
                dx_logging.print_info(
                    f'Template {ARGUMENTS["--create_template"]} '
                    f"was created successfully. Reference: "
                    f"{template_ref}\n"
                )
            elif ARGUMENTS["--delete_template"]:
                delete_template(dlpx_obj, ARGUMENTS["--delete_template"])
                print(f'Template {ARGUMENTS["--delete_template"]} ' f"is deleted.")
            elif ARGUMENTS["--list_templates"]:
                list_templates(dlpx_obj)
    except (
        dlpx_exceptions.DlpxException,
        exceptions.RequestError,
        exceptions.JobError,
        exceptions.HttpError,
        dlpx_exceptions.DlpxObjectNotFound,
    ) as err:
        dx_logging.print_exception(
            f"Error in ss_template: " f'{engine["ip_address"]}:\n{err}'
        )


def main():
    """
    Main function - setup global variables and timer
    """
    time_start = time.time()
    try:
        dx_session_obj = get_session.GetSession()
        dx_logging.logging_est(ARGUMENTS["--logdir"])
        config_file_path = ARGUMENTS["--config"]
        single_thread = ARGUMENTS["--single_thread"]
        engine = ARGUMENTS["--engine"]
        dx_session_obj.get_config(config_file_path)
        dx_session_obj.get_config(config_file_path)
        for each in run_job.run_job_mt(
            main_workflow, dx_session_obj, engine, single_thread
        ):
            each.join()
        elapsed_minutes = run_job.time_elapsed(time_start)
        dx_logging.print_info(f"ss_template took {elapsed_minutes} minutes to complete")
    # Here we handle what we do when the unexpected happens
    except SystemExit as err:
        # This is what we use to handle our sys.exit(#)
        sys.exit(err)
    except dlpx_exceptions.DlpxException as err:
        # We use this exception handler when an error occurs in a function.
        dx_logging.print_exception(
            f"\nERROR: Please check the ERROR message " f"below:\n{err}"
        )
        sys.exit(2)
    except exceptions.HttpError as err:
        # We use this exception handler when our connection to Delphix fails
        print(
            f"\nERROR: Connection failed to the Delphix Engine. Please "
            f"check the error message below:\n{err}"
        )
        sys.exit(2)
    except exceptions.JobError as err:
        # We use this exception handler when a job fails in Delphix so that we
        # have actionable data
        print(f"A job failed in the Delphix Engine:\n{err.job}")
        elapsed_minutes = run_job.time_elapsed(time_start)
        dx_logging.print_info(
            f"{basename(__file__)} took {elapsed_minutes}" f" minutes to get this far"
        )
        sys.exit(3)
    except KeyboardInterrupt:
        # We use this exception handler to gracefully handle ctrl+c exits
        dx_logging.print_debug("You sent a CTRL+C to interrupt the process")
        elapsed_minutes = run_job.time_elapsed(time_start)
        dx_logging.print_info(
            f"{basename(__file__)} took {elapsed_minutes} minutes to complete"
        )


if __name__ == "__main__":
    # Grab our ARGUMENTS from the doc at the top of the script
    ARGUMENTS = docopt(__doc__, version=basename(__file__) + " " + VERSION)

    # Feed our ARGUMENTS to the main function, and off we go!
    main()
