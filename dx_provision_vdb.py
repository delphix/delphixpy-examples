#!/usr/bin/env python
# Adam Bowen - Apr 2016
# This script provisions a vdb or dSource
# Updated by Corey Brune Aug 2016
# --- Create vFiles VDB
# requirements
# pip install docopt delphixpy

# The below doc follows the POSIX compliant standards and allows us to use
# this doc to also define our arguments for the script.

# TODO:
# Refactor provisioning functions
# Documentation

"""Provision VDB's
Usage:
  dx_provision_db.py --source <name> --target_grp <name> --target <name>
                  (--db <name> | --vfiles_path <path>) [--no_truncate_log]
                  (--environment <name> --type <type>) [ --envinst <name>]
                  [--template <name>] [--mapfile <file>]
                  [--timestamp_type <type>] [--timestamp <timepoint_semantic>]
                  [--timeflow <name>]
                  [--instname <sid>] [--mntpoint <path>] [--noopen]
                  [--uniqname <name>][--source_grp <name>] 
                  [--engine <identifier> | --all]
                  [--vdb_restart <bool> ]
                  [--debug] [--parallel <n>] [--poll <n>]
                  [--config <path_to_file>] [--logdir <path_to_file>]
                  [--postrefresh <name>] [--prerefresh <name>]
                  [--configure-clone <name>]
                  [--prerollback <name>] [--postrollback <name>]
  dx_provision_db.py -h | --help | -v | --version
Provision VDB from a defined source on the defined target environment.

Examples:
  dx_provision_vdb.py --engine landsharkengine --source_grp Sources --source "ASE pubs3 DB" --db vase --target testASE --target_grp Analytics --environment LINUXTARGET --type ase --envinst "LINUXTARGET"

  dx_provision_vdb.py --source_grp Sources --source "Employee Oracle 11G DB" --instname autod --uniqname autoprod --db autoprod --target autoprod --target_grp Analytics --environment LINUXTARGET --type oracle --envinst "/u01/app/oracle/product/11.2.0/dbhome_1"

  dx_provision_vdb.py --source_grp Sources --source "AdventureWorksLT2008R2" --db vAW --target testAW --target_grp Analytics --environment WINDOWSTARGET --type mssql --envinst MSSQLSERVER --all

  dx_provision_vdb.py --source UF_Source --target appDataVDB --target_grp Untitled --environment LinuxTarget --type vfiles --vfiles_path /mnt/provision/appDataVDB --prerollback "/u01/app/oracle/product/scripts/PreRollback.sh" --postrollback "/u01/app/oracle/product/scripts/PostRollback.sh" --vdb_restart true

Options:
  --source_grp <name>       The group where the source resides.
  --source <name>           Name of the source object 
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
  --prerefresh <name>       Pre-Hook commands
  --postrefresh <name>      Post-Hook commands
  --prerollback <name>      Post-Hook commands
  --postrollback <name>     Post-Hook commands
  --configure-clone <name>  Configure Clone commands
  --vdb_restart <bool>      Either True or False. Default: False
  --envinst <name>          The identifier of the instance in Delphix.
                            ex. "/u01/app/oracle/product/11.2.0/dbhome_1"
                            ex. LINUXTARGET
  --timeflow <name>         Name of the timeflow from which you are provisioning
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
  --engine <type>           Alt Identifier of Delphix engine in dxtools.conf.
  --all                     Run against all engines.
  --debug                   Enable debug logging
  --parallel <n>            Limit number of jobs to maxjob
  --poll <n>                The number of seconds to wait between job polls
                            [default: 10]
  --config <path_to_file>   The path to the dxtools.conf file
                            [default: ./dxtools.conf]
  --logdir <path_to_file>    The path to the logfile you want to use.
                            [default: ./dx_provision_vdb.log]
  -h --help                 Show this screen.
  -v --version              Show version.
"""
from __future__ import print_function

import re
import signal
import sys
import time
import traceback
from os.path import basename
from time import sleep
from time import time

from docopt import docopt

from delphixpy.v1_8_0.delphix_engine import DelphixEngine
from delphixpy.v1_8_0.exceptions import HttpError
from delphixpy.v1_8_0.exceptions import JobError
from delphixpy.v1_8_0.exceptions import RequestError
from delphixpy.v1_8_0.web import database
from delphixpy.v1_8_0.web import environment
from delphixpy.v1_8_0.web import group
from delphixpy.v1_8_0.web import job
from delphixpy.v1_8_0.web import repository
from delphixpy.v1_8_0.web import snapshot
from delphixpy.v1_8_0.web import source
from delphixpy.v1_8_0.web.database import template
from delphixpy.v1_8_0.web.vo import AppDataDirectSourceConfig
from delphixpy.v1_8_0.web.vo import AppDataProvisionParameters
from delphixpy.v1_8_0.web.vo import AppDataVirtualSource
from delphixpy.v1_8_0.web.vo import ASEDBContainer
from delphixpy.v1_8_0.web.vo import ASEInstanceConfig
from delphixpy.v1_8_0.web.vo import ASEProvisionParameters
from delphixpy.v1_8_0.web.vo import ASESIConfig
from delphixpy.v1_8_0.web.vo import ASEVirtualSource
from delphixpy.v1_8_0.web.vo import MSSqlDatabaseContainer
from delphixpy.v1_8_0.web.vo import MSSqlProvisionParameters
from delphixpy.v1_8_0.web.vo import MSSqlSIConfig
from delphixpy.v1_8_0.web.vo import MSSqlVirtualSource
from delphixpy.v1_8_0.web.vo import OracleDatabaseContainer
from delphixpy.v1_8_0.web.vo import OracleInstance
from delphixpy.v1_8_0.web.vo import OracleProvisionParameters
from delphixpy.v1_8_0.web.vo import OracleSIConfig
from delphixpy.v1_8_0.web.vo import OracleVirtualSource
from delphixpy.v1_8_0.web.vo import TimeflowPointLocation
from delphixpy.v1_8_0.web.vo import TimeflowPointSemantic
from delphixpy.v1_8_0.web.vo import TimeflowPointTimestamp
from delphixpy.v1_8_0.web.vo import VirtualSourceOperations
from lib.DlpxException import DlpxException
from lib.DxLogging import logging_est
from lib.DxLogging import print_debug
from lib.DxLogging import print_info
from lib.DxTimeflow import DxTimeflow
from lib.GetReferences import find_dbrepo
from lib.GetReferences import find_obj_by_name
from lib.GetSession import GetSession

VERSION = "v.0.2.305"


def create_ase_vdb(
    engine, server, jobs, vdb_group, vdb_name, environment_obj, container_obj
):
    """
    Create a Sybase ASE VDB
    """
    vdb_obj = find_database_by_name_and_group_name(
        engine, server, vdb_group.name, vdb_name
    )
    if vdb_obj == None:
        vdb_params = ASEProvisionParameters()
        vdb_params.container = ASEDBContainer()
        if arguments["--no_truncate_log"]:
            vdb_params.truncate_log_on_checkpoint = False
        else:
            vdb_params.truncate_log_on_checkpoint = True
        vdb_params.container.group = vdb_group.reference
        vdb_params.container.name = vdb_name
        vdb_params.source = ASEVirtualSource()
        vdb_params.source_config = ASESIConfig()
        vdb_params.source_config.database_name = arguments["--db"]
        vdb_params.source_config.instance = ASEInstanceConfig()
        vdb_params.source_config.instance.host = environment_obj.host

        vdb_repo = find_dbrepo_by_environment_ref_and_name(
            engine,
            server,
            "ASEInstance",
            environment_obj.reference,
            arguments["--envinst"],
        )

        vdb_params.source_config.repository = vdb_repo.reference
        vdb_params.timeflow_point_parameters = set_timeflow_point(
            engine, server, container_obj
        )

        vdb_params.timeflow_point_parameters.container = container_obj.reference
        print_info("Provisioning " + vdb_name)
        database.provision(server, vdb_params)

        # Add the job into the jobs dictionary so we can track its progress
        jobs[engine["hostname"]] = server.last_job
        # return the job object to the calling statement so that we can tell if
        # a job was created or not (will return None, if no job)
        return server.last_job
    else:
        print_info(engine["hostname"] + ": " + vdb_name + " already exists.")
        return vdb_obj.reference


def create_mssql_vdb(engine, jobs, vdb_group, vdb_name, environment_obj, container_obj):
    """
    Create a MSSQL VDB
    engine:
    jobs:
    vdb_group:
    vdb_name,
    environment_obj:
    container_obj:

    """
    vdb_obj = find_database_by_name_and_group_name(
        engine, dx_session_obj.server_session, vdb_group.name, vdb_name
    )
    if vdb_obj == None:
        vdb_params = MSSqlProvisionParameters()
        vdb_params.container = MSSqlDatabaseContainer()
        vdb_params.container.group = vdb_group.reference
        vdb_params.container.name = vdb_name
        vdb_params.source = MSSqlVirtualSource()
        vdb_params.source.allow_auto_vdb_restart_on_host_reboot = False
        vdb_params.source_config = MSSqlSIConfig()
        vdb_params.source_config.database_name = arguments["--db"]

        vdb_params.source_config.repository = find_dbrepo(
            dx_session_obj.server_session,
            "MSSqlInstance",
            environment_obj.reference,
            arguments["--envinst"],
        ).reference

        vdb_params.timeflow_point_parameters = set_timeflow_point(
            engine, dx_session_obj.server_session, container_obj
        )
        if not vdb_params.timeflow_point_parameters:
            return
        vdb_params.timeflow_point_parameters.container = container_obj.reference
        print_info(engine["hostname"] + ":Provisioning " + vdb_name)
        database.provision(dx_session_obj.server_session, vdb_params)
        # Add the job into the jobs dictionary so we can track its progress
        jobs[engine["hostname"]] = dx_session_obj.server_session.last_job
        # return the job object to the calling statement so that we can tell if
        # a job was created or not (will return None, if no job)
        return dx_session_obj.server_session.last_job
    else:
        print_info(engine["hostname"] + ": " + vdb_name + " already exists.")
        return vdb_obj.reference


def create_vfiles_vdb(
    engine,
    jobs,
    vfiles_group,
    vfiles_name,
    environment_obj,
    container_obj,
    pre_refresh=None,
    post_refresh=None,
    pre_rollback=None,
    post_rollback=None,
    configure_clone=None,
):
    """
    Create a Vfiles VDB
    """

    vfiles_obj = None

    try:
        vfiles_obj = find_obj_by_name(
            dx_session_obj.server_session, database, vfiles_name
        )
    except DlpxException:
        pass

    if vfiles_obj is None:
        vfiles_repo = find_repo_by_environment_ref(
            engine, "Unstructured Files", environment_obj.reference
        )

        vfiles_params = AppDataProvisionParameters()
        vfiles_params.source = AppDataVirtualSource()
        vfiles_params.source_config = AppDataDirectSourceConfig()

        vdb_restart_reobj = re.compile("true", re.IGNORECASE)

        if vdb_restart_reobj.search(str(arguments["--vdb_restart"])):
            vfiles_params.source.allow_auto_vdb_restart_on_host_reboot = True

        elif vdb_restart_reobj.search(str(arguments["--vdb_restart"])) is None:
            vfiles_params.source.allow_auto_vdb_restart_on_host_reboot = False

        vfiles_params.container = {
            "type": "AppDataContainer",
            "group": vfiles_group.reference,
            "name": vfiles_name,
        }

        vfiles_params.source_config.name = arguments["--target"]
        vfiles_params.source_config.path = arguments["--vfiles_path"]
        vfiles_params.source_config.environment_user = environment_obj.primary_user
        vfiles_params.source_config.repository = vfiles_repo.reference

        vfiles_params.source.parameters = {}
        vfiles_params.source.name = vfiles_name
        vfiles_params.source.name = vfiles_name
        vfiles_params.source.operations = VirtualSourceOperations()

        if pre_refresh:
            vfiles_params.source.operations.pre_refresh = [
                {"type": "RunCommandOnSourceOperation", "command": pre_refresh}
            ]

        if post_refresh:
            vfiles_params.source.operations.post_refresh = [
                {"type": "RunCommandOnSourceOperation", "command": post_refresh}
            ]

        if pre_rollback:
            vfiles_params.source.operations.pre_rollback = [
                {"type": "RunCommandOnSourceOperation", "command": pre_rollback}
            ]

        if post_rollback:
            vfiles_params.source.operations.post_rollback = [
                {"type": "RunCommandOnSourceOperation", "command": post_rollback}
            ]

        if configure_clone:
            vfiles_params.source.operations.configure_clone = [
                {"type": "RunCommandOnSourceOperation", "command": configure_clone}
            ]

        if arguments["--timestamp_type"] is None:
            vfiles_params.timeflow_point_parameters = {
                "type": "TimeflowPointSemantic",
                "container": container_obj.reference,
                "location": "LATEST_POINT",
            }

        elif arguments["--timestamp_type"].upper() == "SNAPSHOT":

            try:
                dx_timeflow_obj = DxTimeflow(dx_session_obj.server_session)
                dx_snap_params = dx_timeflow_obj.set_timeflow_point(
                    container_obj,
                    arguments["--timestamp_type"],
                    arguments["--timestamp"],
                    arguments["--timeflow"],
                )

            except RequestError as e:
                raise DlpxException("Could not set the timeflow point:\n%s" % (e))

            if dx_snap_params.type == "TimeflowPointSemantic":
                vfiles_params.timeflow_point_parameters = {
                    "type": dx_snap_params.type,
                    "container": dx_snap_params.container,
                    "location": dx_snap_params.location,
                }

            elif dx_snap_params.type == "TimeflowPointTimestamp":
                vfiles_params.timeflow_point_parameters = {
                    "type": dx_snap_params.type,
                    "timeflow": dx_snap_params.timeflow,
                    "timestamp": dx_snap_params.timestamp,
                }

        print_info("%s: Provisioning %s\n" % (engine["hostname"], vfiles_name))

        try:
            database.provision(dx_session_obj.server_session, vfiles_params)

        except (JobError, RequestError, HttpError) as e:
            raise DlpxException(
                "\nERROR: Could not provision the database:" "\n%s" % (e)
            )

        # Add the job into the jobs dictionary so we can track its progress
        jobs[engine["hostname"]] = dx_session_obj.server_session.last_job

        # return the job object to the calling statement so that we can tell if
        # a job was created or not (will return None, if no job)
        return dx_session_obj.server_session.last_job
    else:
        print_info(
            "\nERROR %s: %s already exists. \n" % (engine["hostname"], vfiles_name)
        )
        return vfiles_obj.reference


def create_oracle_si_vdb(
    engine,
    jobs,
    vdb_name,
    vdb_group_obj,
    environment_obj,
    container_obj,
    pre_refresh=None,
    post_refresh=None,
    pre_rollback=None,
    post_rollback=None,
    configure_clone=None,
):

    """
    Create an Oracle SI VDB
    """

    vdb_obj = None

    try:
        vdb_obj = find_obj_by_name(dx_session_obj.server_session, database, vdb_name)
    except DlpxException:
        pass

    if vdb_obj == None:
        vdb_params = OracleProvisionParameters()
        vdb_params.open_resetlogs = True

        if arguments["--noopen"]:
            vdb_params.open_resetlogs = False

        vdb_params.container = OracleDatabaseContainer()
        vdb_params.container.group = vdb_group_obj.reference
        vdb_params.container.name = vdb_name
        vdb_params.source = OracleVirtualSource()
        vdb_params.source.allow_auto_vdb_restart_on_host_reboot = False

        if arguments["--instname"]:
            inst_name = arguments["--instname"]
        elif arguments["--instname"] == None:
            inst_name = vdb_name

        if arguments["--uniqname"]:
            unique_name = arguments["--uniqname"]
        elif arguments["--uniqname"] == None:
            unique_name = vdb_name

        if arguments["--db"]:
            db = arguments["--db"]
        elif arguments["--db"] == None:
            db = vdb_name

        vdb_params.source.mount_base = arguments["--mntpoint"]

        if arguments["--mapfile"]:
            vdb_params.source.file_mapping_rules = arguments["--mapfile"]

        if arguments["--template"]:
            template_obj = find_obj_by_name(
                dx_session_obj.server_session,
                database.template,
                arguments["--template"],
            )

            vdb_params.source.config_template = template_obj.reference

        vdb_params.source_config = OracleSIConfig()
        vdb_params.source.operations = VirtualSourceOperations()

        if pre_refresh:
            vdb_params.source.operations.pre_refresh = [
                {"type": "RunCommandOnSourceOperation", "command": pre_refresh}
            ]

        if post_refresh:
            vdb_params.source.operations.post_refresh = [
                {"type": "RunCommandOnSourceOperation", "command": post_refresh}
            ]

        if pre_rollback:
            vdb_params.source.operations.pre_rollback = [
                {"type": "RunCommandOnSourceOperation", "command": pre_rollback}
            ]

        if post_rollback:
            vdb_params.source.operations.post_rollback = [
                {"type": "RunCommandOnSourceOperation", "command": post_rollback}
            ]

        if configure_clone:
            vdb_params.source.operations.configure_clone = [
                {"type": "RunCommandOnSourceOperation", "command": configure_clone}
            ]

        vdb_repo = find_dbrepo_by_environment_ref_and_install_path(
            engine,
            dx_session_obj.server_session,
            "OracleInstall",
            environment_obj.reference,
            arguments["--envinst"],
        )

        vdb_params.source_config.database_name = db
        vdb_params.source_config.unique_name = unique_name
        vdb_params.source_config.instance = OracleInstance()
        vdb_params.source_config.instance.instance_name = inst_name
        vdb_params.source_config.instance.instance_number = 1
        vdb_params.source_config.repository = vdb_repo.reference

        dx_timeflow_obj = DxTimeflow(dx_session_obj.server_session)
        vdb_params.timeflow_point_parameters = dx_timeflow_obj.set_timeflow_point(
            container_obj, arguments["--timestamp_type"], arguments["--timestamp"]
        )

        print(vdb_params, "\n\n\n")
        print_info(engine["hostname"] + ": Provisioning " + vdb_name)
        database.provision(dx_session_obj.server_session, vdb_params)
        # Add the job into the jobs dictionary so we can track its progress

        jobs[engine["hostname"]] = dx_session_obj.server_session.last_job
        # return the job object to the calling statement so that we can tell if
        # a job was created or not (will return None, if no job)

        return dx_session_obj.server_session.last_job

    else:
        raise DlpxException(
            "\nERROR: %s: %s alread exists\n" % (engine["hostname"], vdb_name)
        )


def find_all_databases_by_group_name(
    engine, server, group_name, exclude_js_container=False
):
    """
    Easy way to quickly find databases by group name
    """

    # First search groups for the name specified and return its reference
    group_obj = find_obj_by_name(dx_session_obj.server_session, group, group_name)
    if group_obj:
        databases = database.get_all(
            server,
            group=group_obj.reference,
            no_js_container_data_source=exclude_js_container,
        )
        return databases


def find_database_by_name_and_group_name(engine, server, group_name, database_name):

    databases = find_all_databases_by_group_name(engine, server, group_name)

    for each in databases:
        if each.name == database_name:
            print_debug(
                "%s: Found a match %s" % (engine["hostname"], str(each.reference))
            )
            return each

    print_info(
        "%s unable to find %s in %s" % (engine["hostname"], database_name, group_name)
    )


def find_dbrepo_by_environment_ref_and_install_path(
    engine, server, install_type, f_environment_ref, f_install_path
):
    """
    Function to find database repository objects by environment reference and
    install path, and return the object's reference as a string
    You might use this function to find Oracle and PostGreSQL database repos.
    """
    print_debug(
        "%s: Searching objects in the %s class for one with the "
        "environment reference of %s and an install path of %s"
        % (engine["hostname"], install_type, f_environment_ref, f_install_path),
        debug,
    )

    for obj in repository.get_all(server, environment=f_environment_ref):
        if install_type == "PgSQLInstall":
            if obj.type == install_type and obj.installation_path == f_install_path:
                print_debug(
                    "%s: Found a match %s" % (engine["hostname"], str(obj.reference)),
                    debug,
                )
                return obj

        elif install_type == "OracleInstall":
            if obj.type == install_type and obj.installation_home == f_install_path:

                print_debug(
                    "%s: Fount a match %s" % (engine["hostname"], str(obj.reference)),
                    debug,
                )
                return obj
        else:
            raise DlpxException(
                "%s: No Repo match found for type %s.\n"
                % (engine["hostname"], install_type)
            )


def find_repo_by_environment_ref(
    engine, repo_type, f_environment_ref, f_install_path=None
):
    """
    Function to find unstructured file repository objects by environment
    reference and name, and return the object's reference as a string
    You might use this function to find Unstructured File repos.
    """

    print_debug(
        "\n%s: Searching objects in the %s class for one with the"
        "environment reference of %s\n"
        % (engine["hostname"], repo_type, f_environment_ref),
        debug,
    )

    obj_ref = ""
    all_objs = repository.get_all(
        dx_session_obj.server_session, environment=f_environment_ref
    )

    for obj in all_objs:
        if obj.name == repo_type:
            print_debug(engine["hostname"] + ": Found a match " + str(obj.reference))
            return obj

        elif obj.type == repo_type:
            print_debug(
                "%s Found a match %s" % (engine["hostname"], str(obj.reference)), debug
            )
            return obj

    raise DlpxException(
        "%s: No Repo match found for type %s\n" % (engine["hostname"], repo_type)
    )


def find_dbrepo_by_environment_ref_and_name(
    engine, repo_type, f_environment_ref, f_name
):
    """
    Function to find database repository objects by environment reference and
    name, and return the object's reference as a string
    You might use this function to find MSSQL database repos.
    """

    print_debug(
        "%s: Searching objects in the %s class for one with the "
        "environment reference of %s and a name of %s."
        % (engine["hostname"], repo_type, f_environment_ref, f_name),
        debug,
    )

    obj_ref = ""
    all_objs = repository.get_all(server, environment=f_environment_ref)

    for obj in all_objs:
        if repo_type == "MSSqlInstance" or repo_type == "ASEInstance":
            if obj.type == repo_type and obj.name == f_name:
                print_debug(
                    "%s: Found a match %s" % (engine["hostname"], str(obj.reference)),
                    debug,
                )
                return obj

        elif repo_type == "Unstructured Files":
            if obj.value == install_type:
                print_debug(
                    "%s: Found a match %s" % (engine["hostname"], str(obj.reference)),
                    debug,
                )
                return obj

    raise DlpxException(
        "%s: No Repo match found for type %s\n" % (engine["hostname"], repo_type)
    )


def find_snapshot_by_database_and_name(engine, database_obj, snap_name):
    """
    Find snapshots by database and name. Return snapshot reference.

    engine: Dictionary of engines from config file.
    database_obj: Database object to find the snapshot against
    snap_name: Name of the snapshot
    """

    snapshots = snapshot.get_all(
        dx_session_obj.server_session, database=database_obj.reference
    )
    matches = []
    for snapshot_obj in snapshots:
        if str(snapshot_obj.name).startswith(arguments["--timestamp"]):
            matches.append(snapshot_obj)

    for each in matches:
        print_debug(each.name, debug)

    if len(matches) == 1:
        print_debug(
            "%s: Found one and only one match. This is good.\n %s"
            % (engine["hostname"], matches[0]),
            debug,
        )
        return matches[0]

    elif len(matches) > 1:
        raise DlpxException(
            "%s: The name specified was not specific enough."
            " More than one match found.\n" % (engine["hostname"],)
        )

    else:
        raise DlpxException(
            "%s: No matches found for the time specified.\n" % (engine["hostname"])
        )


def find_snapshot_by_database_and_time(engine, database_obj, snap_time):
    snapshots = snapshot.get_all(
        dx_session_obj.server_session, database=database_obj.reference
    )
    matches = []

    for snapshot_obj in snapshots:
        if str(snapshot_obj.latest_change_point.timestamp).startswith(
            arguments["--timestamp"]
        ):

            matches.append(snapshot_obj)

    if len(matches) == 1:
        print_debug(
            '%s": Found one and only one match. This is good.\n%s'
            % (engine["hostname"], matches[0]),
            debug,
        )

        return matches[0]

    elif len(matches) > 1:
        print_debug(matches, debug)

        raise DlpxException(
            "%s: The time specified was not specific enough."
            "More than one match found.\n" % (engine["hostname"])
        )
    else:
        raise DlpxException(
            "%s: No matches found for the time specified.\n" % (engine["hostname"])
        )


def find_source_by_database(engine, database_obj):
    # The source tells us if the database is enabled/disables, virtual,
    # vdb/dSource, or is a staging database.
    source_obj = source.get_all(server, database=database_obj.reference)

    # We'll just do a little sanity check here to ensure we only have a 1:1
    # result.
    if len(source_obj) == 0:
        raise DlpxException(
            "%s: Did not find a source for %s. Exiting.\n"
            % (engine["hostname"], database_obj.name)
        )

    elif len(source_obj) > 1:
        raise DlpxException(
            "%s: More than one source returned for %s. "
            "Exiting.\n" % (engine["hostname"], database_obj.name + ". Exiting")
        )
    return source_obj


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
        func_hl = Thread(target=func, args=args, kwargs=kwargs)
        func_hl.start()
        return func_hl

    return async_func


@run_async
def main_workflow(engine):
    """
    This function actually runs the jobs.
    Use the @run_async decorator to run this function asynchronously.
    This allows us to run against multiple Delphix Engine simultaneously

    engine: Dictionary containing engine information
    """

    # Establish these variables as empty for use later
    environment_obj = None
    source_objs = None
    jobs = {}

    try:
        # Setup the connection to the Delphix Engine
        dx_session_obj.serversess(
            engine["ip_address"], engine["username"], engine["password"]
        )

        group_obj = find_obj_by_name(
            dx_session_obj.server_session, group, arguments["--target_grp"]
        )

        # Get the reference of the target environment.
        print_debug("Getting environment for %s\n" % (host_name), debug)

        # Get the environment object by the hostname
        environment_obj = find_obj_by_name(
            dx_session_obj.server_session, environment, host_name
        )

    except DlpxException as e:
        print(
            "\nERROR: Engine %s encountered an error while provisioning "
            "%s:\n%s\n" % (engine["hostname"], arguments["--target"], e)
        )
        sys.exit(1)

    print_debug(
        "Getting database information for %s\n" % (arguments["--source"]), debug
    )
    try:
        # Get the database reference we are copying from the database name
        database_obj = find_obj_by_name(
            dx_session_obj.server_session, database, arguments["--source"]
        )
    except DlpxException:
        return

    thingstodo = ["thingtodo"]
    # reset the running job count before we begin
    i = 0

    try:
        with dx_session_obj.job_mode(single_thread):
            while len(jobs) > 0 or len(thingstodo) > 0:
                arg_type = arguments["--type"].lower()
                if len(thingstodo) > 0:

                    if arg_type == "oracle":
                        create_oracle_si_vdb(
                            engine,
                            jobs,
                            database_name,
                            group_obj,
                            environment_obj,
                            database_obj,
                            arguments["--prerefresh"],
                            arguments["--postrefresh"],
                            arguments["--prerollback"],
                            arguments["--postrollback"],
                            arguments["--configure-clone"],
                        )

                    elif arg_type == "ase":
                        create_ase_vdb(
                            engine,
                            server,
                            jobs,
                            group_obj,
                            database_name,
                            environment_obj,
                            database_obj,
                        )

                    elif arg_type == "mssql":
                        create_mssql_vdb(
                            engine,
                            jobs,
                            group_obj,
                            database_name,
                            environment_obj,
                            database_obj,
                        )

                    elif arg_type == "vfiles":
                        create_vfiles_vdb(
                            engine,
                            jobs,
                            group_obj,
                            database_name,
                            environment_obj,
                            database_obj,
                            arguments["--prerefresh"],
                            arguments["--postrefresh"],
                            arguments["--prerollback"],
                            arguments["--postrollback"],
                            arguments["--configure-clone"],
                        )

                    thingstodo.pop()

                # get all the jobs, then inspect them
                i = 0
                for j in jobs.keys():
                    job_obj = job.get(dx_session_obj.server_session, jobs[j])
                    print_debug(job_obj, debug)
                    print_info(
                        engine["hostname"] + ": VDB Provision: " + job_obj.job_state
                    )

                    if job_obj.job_state in ["CANCELED", "COMPLETED", "FAILED"]:
                        # If the job is in a non-running state, remove it from
                        # the running jobs list.
                        del jobs[j]
                    else:
                        # If the job is in a running state, increment the
                        # running job count.
                        i += 1

                print_info("%s: %s jobs running." % (engine["hostname"], str(i)))

                # If we have running jobs, pause before repeating the checks.
                if len(jobs) > 0:
                    sleep(float(arguments["--poll"]))

    except (DlpxException, JobError) as e:
        print("\nError while provisioning %s:\n%s" % (database_name, e.message))
        sys.exit(1)


def run_job():
    """
    This function runs the main_workflow aynchronously against all the servers
    specified

    No arguments required for run_job().
    """
    # Create an empty list to store threads we create.
    threads = []

    # If the --all argument was given, run against every engine in dxtools.conf
    if arguments["--all"]:
        print_info("Executing against all Delphix Engines in the dxtools.conf")

        try:
            # For each server in the dxtools.conf...
            for delphix_engine in dx_session_obj.dlpx_engines:
                engine = dx_session_obj[delphix_engine]
                # Create a new thread and add it to the list.
                threads.append(main_workflow(engine))

        except DlpxException as e:
            print("Error encountered in main_workflow:\n%s" % (e))
            sys.exit(1)

    elif arguments["--all"] is False:
        # Else if the --engine argument was given, test to see if the engine
        # exists in dxtools.conf
        if arguments["--engine"]:
            try:
                engine = dx_session_obj.dlpx_engines[arguments["--engine"]]
                print_info(
                    "Executing against Delphix Engine: %s\n" % (arguments["--engine"])
                )

            except (DlpxException, RequestError, KeyError) as e:
                raise DlpxException(
                    "\nERROR: Delphix Engine %s cannot be "
                    "found in %s. Please check your value "
                    "and try again. Exiting.\n"
                    % (arguments["--engine"], config_file_path)
                )

        else:
            # Else search for a default engine in the dxtools.conf
            for delphix_engine in dx_session_obj.dlpx_engines:
                if dx_session_obj.dlpx_engines[delphix_engine]["default"] == "true":

                    engine = dx_session_obj.dlpx_engines[delphix_engine]
                    print_info(
                        "Executing against the default Delphix Engine "
                        "in the dxtools.conf: %s"
                        % (dx_session_obj.dlpx_engines[delphix_engine]["hostname"])
                    )

                break

            if engine == None:
                raise DlpxException("\nERROR: No default engine found. Exiting")

        # run the job against the engine
        threads.append(main_workflow(engine))

    # For each thread in the list...
    for each in threads:
        # join them back together so that we wait for all threads to complete
        # before moving on
        each.join()


def serversess(f_engine_address, f_engine_username, f_engine_password):
    """
    Function to setup the session with the Delphix Engine
    """
    server_session = DelphixEngine(
        f_engine_address, f_engine_username, f_engine_password, "DOMAIN"
    )
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

    if arguments["--timestamp_type"].upper() == "SNAPSHOT":
        if arguments["--timestamp"].upper() == "LATEST":
            print_debug("%s: Using the latest Snapshot." % (engine["hostname"]), debug)

            timeflow_point_parameters = TimeflowPointSemantic()
            timeflow_point_parameters.container = container_obj.reference
            timeflow_point_parameters.location = "LATEST_SNAPSHOT"

        elif arguments["--timestamp"].startswith("@"):
            print_debug("%s: Using a named snapshot" % (engine["hostname"]), debug)

            snapshot_obj = find_snapshot_by_database_and_name(
                engine, server, container_obj, arguments["--timestamp"]
            )

            if snapshot_obj != None:
                timeflow_point_parameters = TimeflowPointLocation()
                timeflow_point_parameters.timeflow = snapshot_obj.timeflow
                timeflow_point_parameters.location = (
                    snapshot_obj.latest_change_point.location
                )

            else:
                raise DlpxException(
                    "%s: Was unable to use the specified "
                    "snapshot %s for database %s\n"
                    % (engine["hostname"], arguments["--timestamp"], container_obj.name)
                )

        else:
            print_debug(
                "%s: Using a time-designated snapshot" % (engine["hostname"]), debug
            )

            snapshot_obj = find_snapshot_by_database_and_time(
                engine, server, container_obj, arguments["--timestamp"]
            )
            if snapshot_obj != None:
                timeflow_point_parameters = TimeflowPointTimestamp()
                timeflow_point_parameters.timeflow = snapshot_obj.timeflow
                timeflow_point_parameters.timestamp = (
                    snapshot_obj.latest_change_point.timestamp
                )
            else:
                raise DlpxException(
                    "%s: Was unable to find a suitable time "
                    " for %s for database %s.\n"
                    % (engine["hostname"], arguments["--timestamp"], container_obj.name)
                )

    elif arguments["--timestamp_type"].upper() == "TIME":
        if arguments["--timestamp"].upper() == "LATEST":
            timeflow_point_parameters = TimeflowPointSemantic()
            timeflow_point_parameters.location = "LATEST_POINT"
        else:
            raise DlpxException(
                "%s: Only support a --timestamp value of "
                '"latest" when used with timestamp_type '
                "of time" % s(engine["hostname"])
            )

    else:
        raise DlpxException(
            "%s is not a valied timestamp_type. Exiting\n"
            % (arguments["--timestamp_type"])
        )

    timeflow_point_parameters.container = container_obj.reference
    return timeflow_point_parameters


def time_elapsed():
    """
    This function calculates the time elapsed since the beginning of the script.
    Call this anywhere you want to note the progress in terms of time
    """
    elapsed_minutes = round((time() - time_start) / 60, +1)
    return elapsed_minutes


def update_jobs_dictionary(engine, server, jobs):
    """
    This function checks each job in the dictionary and updates its status or
    removes it if the job is complete.
    Return the number of jobs still running.
    """
    # Establish the running jobs counter, as we are about to update the count
    # from the jobs report.
    i = 0
    # get all the jobs, then inspect them
    for j in jobs.keys():
        job_obj = job.get(server, jobs[j])
        print_debug("%s: %s" % (engine["hostname"], str(job_obj)), debug)
        print_info("%s: %s: %s" % (engine["hostname"], j.name, job_obj.job_state))

        if job_obj.job_state in ["CANCELED", "COMPLETED", "FAILED"]:
            # If the job is in a non-running state, remove it from the running
            # jobs list.
            del jobs[j]
        else:
            # If the job is in a running state, increment the running job count.
            i += 1
    return i


def main(argv):
    # We want to be able to call on these variables anywhere in the script.
    global single_thread
    global usebackup
    global time_start
    global config_file_path
    global database_name
    global host_name
    global dx_session_obj
    global debug

    try:
        dx_session_obj = GetSession()
        debug = arguments["--debug"]
        logging_est(arguments["--logdir"], debug)
        print_debug(arguments, debug)
        time_start = time()
        single_thread = False
        config_file_path = arguments["--config"]

        print_info("Welcome to %s version %s" % (basename(__file__), VERSION))

        # Parse the dxtools.conf and put it into a dictionary
        dx_session_obj.get_config(config_file_path)

        database_name = arguments["--target"]
        host_name = arguments["--environment"]

        # This is the function that will handle processing main_workflow for
        # all the servers.
        run_job()

        elapsed_minutes = time_elapsed()
        print_info("script took %s minutes to get this far. " % (str(elapsed_minutes)))

    # Here we handle what we do when the unexpected happens
    except SystemExit as e:
        """
        This is what we use to handle our sys.exit(#)
        """
        sys.exit(e)

    except DlpxException as e:
        """
        We use this exception handler when an error occurs in a function call.
        """

        print("\nERROR: Please check the ERROR message below:\n%s" % (e.message))
        sys.exit(2)

    except HttpError as e:
        """
        We use this exception handler when our connection to Delphix fails
        """
        print(
            "\nERROR: Connection failed to the Delphix Engine. Please "
            "check the ERROR message below:\n%s" % (e.message)
        )
        sys.exit(2)

    except JobError as e:
        """
        We use this exception handler when a job fails in Delphix so
        that we have actionable data
        """
        print("A job failed in the Delphix Engine:\n%s"(e.job))
        elapsed_minutes = time_elapsed()
        print_info(
            "%s took %s minutes to get this far"
            % (basename(__file__), str(elapsed_minutes))
        )
        sys.exit(3)

    except KeyboardInterrupt:
        """
        We use this exception handler to gracefully handle ctrl+c exits
        """
        print_debug("You sent a CTRL+C to interrupt the process")
        elapsed_minutes = time_elapsed()
        print_info(
            "%s took %s minutes to get this far"
            % (basename(__file__), str(elapsed_minutes))
        )

    except:
        """
        Everything else gets caught here
        """
        print(sys.exc_info()[0])
        print(traceback.format_exc())
        elapsed_minutes = time_elapsed()
        print_info(
            "%s took %s minutes to get this far"
            % (basename(__file__), str(elapsed_minutes))
        )
        sys.exit(1)


if __name__ == "__main__":
    # Grab our arguments from the doc at the top of the script
    arguments = docopt(__doc__, version=basename(__file__) + " " + VERSION)
    # Feed our arguments to the main function, and off we go!
    main(arguments)
