"""
Module that provides lookups of references and names of Delphix objects.
"""

from datetime import datetime

from dateutil import tz

from delphixpy.v1_10_2 import exceptions
from delphixpy.v1_10_2.web import database
from delphixpy.v1_10_2.web import group
from delphixpy.v1_10_2.web import job
from delphixpy.v1_10_2.web import repository
from delphixpy.v1_10_2.web import source
from delphixpy.v1_10_2.web import sourceconfig
from delphixpy.v1_10_2.web import vo
from delphixpy.v1_10_2.web.service import time
from lib import dlpx_exceptions

VERSION = "v.0.3.006"


def convert_timestamp(engine, timestamp):
    """
    Convert timezone from Zulu/UTC to the Engine's timezone
    :param engine: A Delphix engine session object
    :type engine: lib.get_session.GetSession object
    :param timestamp: the timstamp in Zulu/UTC to be converted
    :type timestamp: str
    :return: Timestamp converted localtime
    """

    default_tz = tz.gettz("UTC")
    engine_tz = time.time.get(engine)
    try:
        convert_tz = tz.gettz(engine_tz.system_time_zone)
        utc = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
        utc = utc.replace(tzinfo=default_tz)
        converted_tz = utc.astimezone(convert_tz)
        engine_local_tz = (
            f"{str(converted_tz.date())} "
            f"{str(converted_tz.time())} {str(converted_tz.tzname())}"
        )
        return engine_local_tz
    except TypeError:
        return None


def find_obj_specs(engine, obj_lst):
    """
    Function to find objects for replication
    engine: Delphix Virtualization session object
    obj_lst: List of names for replication
    :return: List of references for the given object names
    """
    rep_lst = []
    for obj in obj_lst:
        rep_lst.append(find_obj_by_name(engine, database, obj).reference)
    return rep_lst


def get_running_job(engine, object_ref):
    """
    Function to find a running job from the DB target reference.
    :param engine: A Delphix DDP session object
    :type engine: lib.GetSession.GetSession object
    :param object_ref: Reference to the object of the running job
    :type object_ref: str
    :return: reference of the running job(s)
    """
    try:
        return job.get_all(engine, target=object_ref, job_state="RUNNING")[0].reference
    except IndexError:
        return None


def find_obj_by_name(engine, f_class, obj_name):
    """
    Function to find objects by name and object class
    :param engine: A Delphix DDP session object
    :type engine: lib.GetSession.GetSession object
    :param f_class: The objects class. I.E. database or timeflow.
    :type f_class: Supported class type by Delphix
    :param obj_name: The name of the object
    :type obj_name: str
    :return: object of f_class type
    """
    obj_list = f_class.get_all(engine)
    for obj in obj_list:
        if obj.name == obj_name:
            return obj
    raise dlpx_exceptions.DlpxObjectNotFound(f"Object {obj_name} not found.")


def find_obj_by_reference(engine, f_class, reference):
    """
    Function to find objects by reference and object class
    :param engine: A Delphix DDP session object
    :type engine: lib.GetSession.GetSession object
    :param f_class: The objects class. I.E. database or timeflow.
    :type f_class: Supported class type by Delphix
    :param obj_name: The refere ce of the object
    :type reference: str
    :return: object of f_class type
    """
    obj_list = f_class.get_all(engine)
    for obj in obj_list:
        if obj.reference == reference:
            return obj
    raise dlpx_exceptions.DlpxObjectNotFound(
        f"Object with reference {reference} not found."
    )


def find_source_by_db_name(engine, obj_name):
    """
    Function to find sources by database name and object class, and return
    object's reference as a string
    :param engine: A Delphix DDP session object
    :type engine: lib.GetSession.GetSession object
    :param obj_name: The name of the database object in Delphix
    :type obj_name: str
    :return: The parent DB object
    """
    for obj in database.get_all(engine):
        if obj.name == obj_name:
            source_obj = source.get_all(engine, database=obj.reference)
            return source_obj[0]
    raise dlpx_exceptions.DlpxObjectNotFound(
        f"{obj_name} was not found on " f"engine {engine.address}.\n"
    )


def find_obj_name(engine, f_class, obj_reference):
    """
    Return the obj name from obj_reference

    :param engine: A Delphix DDP Session object
    :type engine: lib.GetSession.GetSession object
    :param f_class: The objects class. I.E. database or timeflow
    :type f_class: Supported class type by Delphix
    :param obj_reference: The object reference to retrieve the name
    :type obj_reference: str
    :return: str object name
    """
    try:
        obj_name = f_class.get(engine, obj_reference)
        return obj_name.name
    except (exceptions.RequestError, exceptions.JobError, exceptions.HttpError) as err:
        raise dlpx_exceptions.DlpxException(err)


def find_db_repo(engine, install_type, f_environment_ref, f_install_path):
    """
    Function to find database repository objects by environment reference and
    install path, and return the object's reference as a string
    You might use this function to find Oracle and PostGreSQL database repos.
    :param engine: A Delphix DDP session object
    :type engine: lib.GetSession.GetSession object
    :param install_type: Type of install - Oracle, or MSSQL
    :type install_type: str
    :param f_environment_ref: Reference of the environment for the repository
    :type f_install_path: str
    :param f_install_path: Path to the installation directory.
    :type f_install_path: str
    :return: delphixpy.web.vo.SourceRepository object
    """
    for obj in repository.get_all(engine, environment=f_environment_ref):
        if install_type == "OracleInstall":
            if install_type == obj.type and obj.installation_home == f_install_path:
                return obj.reference
        elif install_type == "MSSqlInstance":
            if obj.type == install_type and obj.instance_name == f_install_path:
                return obj.reference
        elif install_type == "AppDataRepository":
            if obj.type == install_type and obj.instance_name == f_install_path:
                return obj.reference
        else:
            raise dlpx_exceptions.DlpxException(
                f"Only OracleInstall, AppDataRepository or MSSqlInstance "
                f"types are supported.\n"
            )


def find_sourceconfig(engine, sourceconfig_name, f_environment_ref):
    """
    Function to find database sourceconfig objects by environment reference,
    sourceconfig name (db name) and return the object
    You might use this function to find Oracle and PostGreSQL database
    sourceconfigs.
    :param engine: A Delphix DDP session object
    :type engine: lib.get_session.GetSession object
    :param sourceconfig_name: Name of source config, usually name of db
    instance (i.e. orcl)
    :type sourceconfig_name: str
    :param f_environment_ref: Reference of the environment for the repository
    :return: Type is determined by sourceonfig. Found in delphixpy.web.objects
    """
    for obj in sourceconfig.get_all(engine, environment=f_environment_ref):
        if obj.name == sourceconfig_name:
            return obj
    raise dlpx_exceptions.DlpxObjectNotFound(
        f"No sourceconfig match found for type {sourceconfig_name}.\n"
    )


def find_all_databases_by_group(engine, group_name, exclude_js_container=False):
    """
    Easy way to quickly find databases by group name
    :param engine: A Delphix DDP session object
    :type engine: lib.get_session.GetSession object
    :param group_name: Name of the group for the database
    :type group_name: str
    :param exclude_js_container: If set to true, search self-service
    containers
    :type exclude_js_container: bool
    :return: list of :py:class:`delphixpy.web.vo.Container`
    """
    # First search groups for the name specified and return its reference
    group_ref = find_obj_by_name(engine, group, group_name).reference
    if group_ref:
        databases = database.get_all(
            engine, group=group_ref, no_js_container_data_source=exclude_js_container
        )
        return databases
    raise dlpx_exceptions.DlpxObjectNotFound(
        f"No databases found in " f"group {group_name}.\n"
    )


def find_source_by_database(engine, database_obj):
    """
    The source tells us if the database is enabled/disabled, virtual,
    vdb/dSource, or is a staging database.
    :param engine: Delphix DDP Session object
    :type engine: lib.get_session.GetSession object
    :param database_obj: Delphix database object
    :type database_obj: delphixpy.web.vo.Container
    """
    source_obj = source.get_all(engine, database=database_obj.reference)
    # We'll just do a little sanity check here to ensure we only have a
    # 1:1 result.
    if not source_obj:
        raise dlpx_exceptions.DlpxObjectNotFound(
            f'{engine["hostname"]}: Did not find a source for ' f"{database_obj.name}."
        )
    elif len(source_obj) > 1:
        raise dlpx_exceptions.DlpxException(
            f'{engine["hostname"]} More than one source returned for '
            f"{database_obj.name}"
        )
    return source_obj


def build_data_source_params(dlpx_obj, obj, data_source):
    """
    Builds the datasource parameters
    :param dlpx_obj: DDP session object
    :type dlpx_obj: lib.GetSession.GetSession object
    :param obj: object type to use when finding db
    :type obj: Type of object to build DS params
    :param data_source: Name of the database to use when building the
    parameters
    :type data_source: str
    """
    ds_params = vo.JSDataSourceCreateParameters()
    ds_params.source = vo.JSDataSource()
    ds_params.source.name = data_source
    try:
        db_obj = find_obj_by_name(dlpx_obj.server_session, obj, data_source)
        ds_params.container = db_obj.reference
        return ds_params
    except exceptions.RequestError as err:
        raise dlpx_exceptions.DlpxObjectNotFound(
            f"\nCould not find {data_source}\n{err}"
        )


def find_all_objects(engine, f_class):
    """
    Return all objects from a given class
    :param engine: A Delphix engine session object
    :type engine: lib.GetSession.GetSession object
    :param f_class: The objects class. I.E. database or timeflow.
    :return: list
    """
    try:
        return f_class.get_all(engine)
    except (exceptions.JobError, exceptions.HttpError) as err:
        raise dlpx_exceptions.DlpxException(
            f"{engine.address} Error encountered in {f_class}: {err}\n"
        )


def find_obj_list(obj_lst, obj_name):
    """
    Function to find an object in a list of objects
    :param obj_lst: List containing objects from the get_all() method
    :type obj_lst: list
    :param obj_name: Name of the object to match
    :type obj_name: str
    :return: The named object, otherwise, DlpxObjectNotFound
    """
    for obj in obj_lst:
        if obj_name == obj.name:
            return obj
    raise dlpx_exceptions.DlpxObjectNotFound(f"Did not find {obj_name}\n")
