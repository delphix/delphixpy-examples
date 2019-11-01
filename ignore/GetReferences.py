"""
Module that provides lookups of references and names of Delphix objects.
"""

from datetime import datetime
from dateutil import tz

from delphixpy.v1_10_2.web.service import time
from delphixpy.v1_10_2 import exceptions
from delphixpy.v1_10_2 import web

from lib import dlpx_exceptions
from .DxLogging import print_debug

VERSION = 'v.0.3.000'


def convert_timestamp(engine, timestamp):
    """
    Convert timezone from Zulu/UTC to the Engine's timezone
    :param engine: A Delphix engine session object
    :type engine: lib.GetSession.GetSession object
    :param timestamp: the timstamp in Zulu/UTC to be converted
    :type timestamp: str
    :return: Timestamp converted localtime
    """

    default_tz = tz.gettz('UTC')
    engine_tz = time.time.get(engine)
    try:
        convert_tz = tz.gettz(engine_tz.system_time_zone)
        utc = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
        utc = utc.replace(tzinfo=default_tz)
        converted_tz = utc.astimezone(convert_tz)
        engine_local_tz = f'{str(converted_tz.date())} ' \
            f'{str(converted_tz.time())} {str(converted_tz.tzname())}'
        return engine_local_tz
    except TypeError:
        return None


def find_all_objects(engine, f_class):
    """
    Return all objects for a given class
    :param engine: A Delphix DDP session object
    :type engine: lib.GetSession.GetSession object
    :param f_class: The objects class. I.E. database or timeflow.
    :type f_class: Any of the supported DDP classes
    :return: List of objects
    """
    try:
        return f_class.get_all(engine)
    except (exceptions.JobError, exceptions.HttpError) as err:
        raise dlpx_exceptions.DlpxException(f'{engine.address} Error '
                                            f'encountered in {f_class}: '
                                            f'{err}\n')


def find_obj_specs(engine, obj_lst):
    """
    Function to find objects for replication
    :param engine: Delphix DDP session object
    :type engine: lib.GetSession.GetSession object
    :param obj_lst: List of names for replication
    :type obj_lst: list
    :return: List of references for the given object names
    """
    print('change this to a generator\n\n')
    rep_lst = []
    for obj in obj_lst:
        rep_lst.append(find_obj_by_name(engine, web.database, obj).reference)
    return rep_lst


def get_running_job(engine, target_ref):
    """
    Function to find a running job from the DB target reference.
    :param engine: A Delphix DDP session object
    :type engine: lib.GetSession.GetSession object
    :param target_ref: Reference to the target of the running job
    :type target_ref: str
    :return: Running job(s) on the target system
    """
    return web.job.get_all(engine, target=target_ref,
                           job_state='RUNNING')[0].reference


def find_obj_list(obj_lst, obj_name):
    """
    Function to find an object in a list of objects
    :param obj_lst: List containing objects from the get_all() method
    :type obj_lst: list
    :param obj_name: Name of the object to match
    :type obj_name: str
    :return: The named object. None is returned if no match is found.`
    """
    for obj in obj_lst:
        if obj_name == obj.name:
            return obj
    raise dlpx_exceptions.DlpxException(f'No objects found for {obj_name}')


def find_obj_by_name(engine, f_class, obj_name, active_branch=False):
    """
    Function to find objects by name and object class, and return object's
    reference as a string
    :param engine: A Delphix DDP session object
    :type engine: lib.GetSession.GetSession object
    :param f_class: The objects class. I.E. database or timeflow.
    :type f_class: Supported class type by Delphix
    :param obj_name: The name of the object
    :type obj_name: str
    :param active_branch: If true, return list containing
                   the object's reference and active_branch. Otherwise,
                   return the reference.
    :type active_branch: bool
    """
    all_objs = f_class.get_all(engine)
    for obj in all_objs:
        if obj.name == obj_name:
            if active_branch is False:
                return obj
            elif active_branch is True:
                return obj


def find_source_by_dbname(engine, f_class, obj_name):
    """
    Function to find sources by database name and object class, and return
    object's reference as a string
    :param engine: A Delphix DDP session object
    :type engine: lib.GetSession.GetSession object
    :param f_class: The objects class. I.E. database or timeflow.
    :type f_class: Supported class type by Delphix
    :param obj_name: The name of the database object in Delphix
    :type obj_name: str
    :return: str Name of the parent DB
    """
    try:
        all_objs = f_class.get_all(engine)
    except AttributeError as err:
        raise dlpx_exceptions.DlpxObjectNotFound(f'Could not find reference '
                                                 f'for object class {err}')
    for obj in all_objs:
        if obj.name == obj_name:
            print_debug(f'object: {obj}\n\n')
            source_obj = web.source.get_all(engine, database=obj.reference)
            print_debug(f'source: {source_obj}\n\n')
            return source_obj[0]
    raise dlpx_exceptions.DlpxObjectNotFound(f'{obj_name} was not found on '
                                             f'engine {engine.address}.\n')


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
    except exceptions.RequestError as err:
        raise dlpx_exceptions.DlpxException(err)
    except (exceptions.JobError, exceptions.HttpError) as err:
        raise dlpx_exceptions.DlpxException(err)


def find_dbrepo(engine, install_type, f_environment_ref, f_install_path):
    """
    Function to find database repository objects by environment reference and
    install path, and return the object's reference as a string
    You might use this function to find Oracle and PostGreSQL database repos.
    :param engine: Virtualization Engine Session object
    :type engine: lib.GetSession.GetSession object
    :param install_type: Type of install - Oracle, ASE, SQL
    :type install_type: str
    :param f_environment_ref: Reference of the environment for the repository
    :type f_install_path: str
    :param f_install_path: Path to the installation directory.
    :type f_install_path: str
    :return: delphixpy.web.vo.SourceRepository object
    """

    print_debug(f'Searching objects in the {install_type} class for one '
                f'with the environment reference of {f_environment_ref} and '
                f'an install path of {f_install_path}')
    for obj in web.repository.get_all(engine, environment=f_environment_ref):
        if install_type == 'OracleInstall':
            if (install_type == obj.type and
                    obj.installation_home == f_install_path):
                print_debug(f'Found a match {obj.reference}')
                return obj
        elif install_type == 'MSSqlInstance':
            if (obj.type == install_type and
                    obj.instance_name == f_install_path):
                print_debug(f'Found a match {obj.reference}')
                return obj
        else:
            raise dlpx_exceptions.DlpxException(f'No Repo match found for '
                                                f'type {install_type}.\n')


def find_sourceconfig(engine, sourceconfig_name, f_environment_ref):
    """
    Function to find database sourceconfig objects by environment reference and
    sourceconfig name (db name), and return the object's reference as a string
    You might use this function to find Oracle and PostGreSQL database
    sourceconfigs.
    :param engine: Virtualization Engine Session object
    :type engine: lib.GetSession.GetSession object
    :param sourceconfig_name: Name of source config, usually name of db
    instance (i.e. orcl)
    :type sourceconfig_name: str
    :param f_environment_ref: Reference of the environment for the repository
    :return: delphixpy.web.vo.SourceConfig object
    """

    print_debug(f'Searching objects in the SourceConfig class for one with '
                f'the environment reference of {f_environment_ref} and a '
                f'name of {sourceconfig_name}')
    for obj in web.sourceconfig.get_all(engine,
                                        environment=f_environment_ref):
        if obj.name == sourceconfig_name:
            print_debug(f'Found a match {obj.reference}')
            return obj
    raise dlpx_exceptions.DlpxException(f'No sourceconfig match found for '
                                        f'type {sourceconfig_name}.\n')


def find_all_databases_by_group_name(engine, group_name,
                                     exclude_js_container=False):
    """
    Easy way to quickly find databases by group name
    :param engine: Virtualization Engine Session object
    :type engine: lib.GetSession.GetSession object
    :param group_name: Name of the group for the database
    :type group_name: str
    :param exclude_js_container: If set to true, search self-service
    containers
    :type exclude_js_container: bool
    :return: list of :py:class:`delphixpy.web.vo.Container`
    """
    # First search groups for the name specified and return its reference
    group_ref = find_obj_by_name(engine, web.group, group_name).reference
    if group_ref:
        databases = web.database.get_all(engine, group=group_ref,
                                         no_js_container_data_source=
                                         exclude_js_container)
        return databases
    elif not group_ref:
        raise dlpx_exceptions.DlpxException(f'No databases found in group '
                                            f'{group_name}.\n')


def find_database_by_name_and_group_name(engine, group_name, database_name):
    """
    Find database by the DB name and group
    :param engine: Virtualization Engine Session object
    :type engine: lib.GetSession.GetSession object
    :param group_name: Name of the group for the DB
    :type group_name: str
    :param database_name: Name of the database
    :type database_name: str
    :return: :py:class:`delphixpy.web.vo.Container` object
    """
    databases = find_all_databases_by_group_name(engine, group_name)
    for each in databases:
        if each.name == database_name:
            print_debug(f'{engine["hostname"]}: Found a match '
                        f'{str(each.reference)}')
            return each
    raise dlpx_exceptions.DlpxException(f'Unable to find {database_name} '
                                        f'in {group_name}')


def find_source_by_database(engine, database_obj):
    """
    The source tells us if the database is enabled/disables, virtual,
    vdb/dSource, or is a staging database.
    :param engine: Delphix DDP Session object
    :type engine: lib.GetSession.GetSession object
    :param database_obj: Delphix database object
    :type database_obj: :py:class:`delphixpy.web.vo.Container`
    """
    source_obj = web.source.get_all(engine, database=database_obj.reference)
    # We'll just do a little sanity check here to ensure we only have a
    # 1:1 result.
    if not source_obj:
        raise dlpx_exceptions.DlpxObjectNotFound(f'{engine["hostname"]}: '
                                                 f'Did not find a source for '
                                                 f'{database_obj.name}.')
    elif len(source_obj) > 1:
        raise dlpx_exceptions.DlpxException(f'{engine["hostname"]} More than '
                                            f'one source returned for '
                                            f'{database_obj.name}')
    return source_obj
