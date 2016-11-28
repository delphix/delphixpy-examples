"""
Module that provides lookups of references and names of Delphix objects.
"""

import re
from datetime import datetime
from dateutil import tz

from delphixpy.web.service import time
from delphixpy.exceptions import RequestError
from delphixpy.exceptions import HttpError
from delphixpy.exceptions import JobError
from delphixpy.web import timeflow
from delphixpy.web import database

from DlpxException import DlpxException


VERSION = 'v.0.2.000'

def convert_timestamp(engine, timestamp):
    """
    Convert timezone from Zulu/UTC to the Engine's timezone

    engine: A Delphix engine session object.
    timestamp: the timstamp in Zulu/UTC to be converted
    """

    default_tz = tz.gettz('UTC')
    engine_tz = time.time.get(engine)

    try:
        convert_tz = tz.gettz(engine_tz.system_time_zone)
        utc = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S')
        utc = utc.replace(tzinfo=default_tz)
        converted_tz = utc.astimezone(convert_tz)
        engine_local_tz = str(converted_tz.date()) + ' ' + \
                          str(converted_tz.time()) + ' ' + \
                          str(converted_tz.tzname())

        return engine_local_tz
    except TypeError:
        return None

def find_obj_by_name(engine, f_class, obj_name, active_branch=False):
    """
    Function to find objects by name and object class, and return object's 
    reference as a string

    engine: A Delphix engine session object
    f_class: The objects class. I.E. database or timeflow.
    obj_name: The name of the object
    active_branch: Default = False. If true, return list containing
                   the object's reference and active_branch. Otherwise, return 
                   the reference.
    """

    obj_ref = ''
    return_list = []

    all_objs = f_class.get_all(engine)
    for obj in all_objs:
        if obj.name == obj_name:

            if active_branch is False:
                return(obj)

            #This code is for JS objects only.
            elif active_branch is True:
                return_list.append(obj.reference)
                return_list.append(obj.active_branch)
                return(return_list)

            return obj

    #If the object isn't found, raise an exception.
    raise DlpxException('%s not found.\n' % (obj_name))


def get_obj_reference(engine, obj_type, obj_name, search_str=None,
                      container=False):
    """
    Return the reference for the provided object name

    engine: A Delphix engine object.
    results: List containing object name
    search_str (optional): string to search within results list
    container (optional): search for container instead of name
    """

    ret_lst = []

    results = obj_type.get_all(engine)

    for result in results:
        if container is False:
            if result.name == obj_name:
                ret_lst.append(result.reference)

                if search_str:
                    if re.search(search_str, result.reference, re.IGNORECASE):
                        ret_lst.append(True)
                    else:
                        ret_lst.append(False)

                return ret_lst
        else:
            if result.container == obj_name:
                ret_lst.append(result.reference)

                return ret_lst

    raise DlpxException('Reference not found for %s' % obj_name)


def get_db_name(engine, db_reference):
    """
    Return the database name from db_reference

    engine: A Delphix engine object.
    db_reference: The datbase reference to retrieve the db_name
    """

    try:
        db_name = database.get(engine, db_reference)
        return db_name.name

    except RequestError as e:
        raise DlpxException(e)

    except (JobError, HttpError) as e:
        raise DlpxException(e.message)
