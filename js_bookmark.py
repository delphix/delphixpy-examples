#!/usr/bin/env python 
# Program Name : jetstream.py
# Description  : Delphix implementation script
# Author       : Corey Brune
# Created: March 4 2016 (v1.0.0)
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

from delphixpy.v1_5_0.delphix_engine import DelphixEngine
from delphixpy.v1_5_0.web.jetstream import container
from delphixpy.v1_5_0.web.jetstream import bookmark
from delphixpy.v1_5_0.web.jetstream import datasource
from delphixpy.v1_5_0.web.jetstream import branch
from delphixpy.v1_5_0.web.jetstream import template
from delphixpy.v1_5_0 import job_context
from delphixpy.v1_5_0.web import database
from delphixpy.v1_5_0.web.service import time
from delphixpy.v1_5_0.web.vo import JSDataTemplateCreateParameters
from delphixpy.v1_5_0.web.vo import JSDataContainerCreateParameters
from delphixpy.v1_5_0.web.vo import JSBookmarkCreateParameters
from delphixpy.v1_5_0.web.vo import JSBranchCreateParameters
from delphixpy.v1_5_0.web.vo import JSBranch
from delphixpy.v1_5_0.web.vo import JSBookmark
from delphixpy.v1_5_0.exceptions import RequestError
from delphixpy.v1_5_0.exceptions import JobError
from delphixpy.v1_5_0.exceptions import HttpError

import sys, getopt, re
from datetime import datetime
from dateutil import tz


class dlpxExceptionHandler(Exception):


    def __init__(self, errors):
        self.errors = errors


def getDBReference(engine, db_name):
    """
    Return current_timeflow for the db_name
                
    :param engine: A Delphix engine object.
    :param db_name: The database name to retrieve current_timeflow
    """

    db_list = database.get_all(engine)

    for db in db_list:
        if db.name == db_name:
            return(db.reference)

    raise dlpxExceptionHandler('Reference not found for %s' % db_name)


def getReference(engine, object_type, object_name, active_branch=False):
    """
    Get a reference for the given object. 
                
    :param engine: A Delphix engine object.
    :param object_type: Type of object for the reference (bookmark, template...)
    :param object_name: Name of object to retrieve the reference.
    :param active_branch: Default = False. If true, return list containing
           the reference and active_branch. Otherwise, return the reference.
    """

    object_list = object_type.get_all(engine)
    return_list = []

    for js_object in object_list:
        if js_object.name == object_name:

            if active_branch == True:
                return_list.append(js_object.reference)
                return_list.append(js_object.active_branch)
                return(return_list)
            else:
                return(js_object.reference)

    raise dlpxExceptionHandler('Reference not found for %s' % object_name)


def convertTimestamp(engine, timestamp):
    """
    Convert timezone from Zulu/UTC to the Engine's timezone

    :param engine: A Delphix engine object.
    :param timestamp: the timstamp in Zulu/UTC to be converted
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

        return(engine_local_tz)
    except TypeError:
        return(None)


def build_ds_params(engine, db):
    try:
        db_reference = getDBReference(engine, db)
        return({'type': 'JSDataSourceCreateParameters', 'source': {'type':
                'JSDataSource', 'name': db}, 'container': db_reference})

    except RequestError, e:
        print('\nCould not find %s\n%s' % (db, e.message))
        sys.exit(1)


def updateJSObject(engine, obj_name, obj_type, vo_object, err_message):
        try:
            assert obj_name is not '', err_message

            obj_ref = getReference(engine, obj_type, obj_name)
            obj_type.update(engine, obj_ref, vo_object)
            print '%s was updated successfully.\n' % (obj_name)

        except AssertionError, e:
            print('\nAn error occurred updating %s:\n\n%s' % (obj_name, e))
            sys.exit(1)

        except RequestError, e:
            print('\nAn error occurred updating:\n%s' % (
                  e.message))
            sys.exit(1)

def usage():
    print('usage: \n--user, --engine, --database\n'
          '\nListing Jet Stream Objects:\n'
          '--list-container, --list-bookmark, --list-datasource, --list-branch, '
          '--list-template\n'
          '\nCreating Jet Stream Objects:\n'
          '[--create-container --container <name> --database <DB> --template '
          '<name>]\n'
          '[--create-bookmark --bookmark <name> --template <name>]\n'
          '[--create-template --template <name> --database <name>]\n'
          '[--create-branch --branch <name> --template <name> --container '
          '<name>]\n'
          '[--create-container --container <name> --database <DB> '
          '--template <name>]\n'
          '\nDeleting Jet Stream Objects:\n'
          '[--delete-template --template <name>]\n'
          '[--delete-bookmark --bookmark <name>]\n'
          '[--delete-branch --branch <name>]\n'
          '[--delete-container --container <name>]\n'
          '\nMisc. Jet Stream Operations:\n'
          '[--update-branch --branch <name>]\n'
          '[--activate-branch --branch <name>]\n'
          '[--update-bookmark --bookmark <name>]\n')

    sys.exit(1)


def main(argv):
    dlpx_domain = 'DOMAIN'
    list_container = ''
    list_jsbm = ''
    list_ds = ''
    rm_template = ''
    rm_branch = ''
    list_branch = ''
    update_branch = ''
    branch_name = ''
    list_template = ''
    create_branch = ''
    bookmark_name = ''
    template_name = ''
    rm_container = ''
    rm_bookmark = ''
    create_template = ''
    database_name = ''
    create_container = ''
    create_bookmark = ''
    container_name = ''
    activate_branch = ''
    update_bookmark = ''

    try:
        opts, args = getopt.getopt(argv, 'u:p:e:d:bsrtfx',
                     ['help', 'user=', 'password=', 'engine=', 'db=', 
                     'list-container', 'list-bookmark', 'list-datasource',
                     'list-branch', 'list-template', 'domain=', 'refresh',
                     'create-template', 'delete-template', 'template=',
                     'create-container', 'database=', 'bookmark=', 
                     'delete-container', 'container=', 'timestamp=',
                     'create-branch','delete-branch', 'branch=',
                     'create-bookmark','delete-bookmark', 'update-branch',
                     'activate-branch', 'update-bookmark'])

    except getopt.GetoptError, e:
        print e
        usage()

    for opt, arg in opts:
        if opt in ('--help'):
            usage()
        elif opt in ('--engine'):
            dlpx_engine = arg
        elif opt in ('--user'):
            dlpx_user = arg
        elif opt in ('--password'):
            dlpx_password = arg
        elif opt in ('--list-container'):
            list_container = True
        elif opt in ('--list-bookmark'):
            list_jsbm = True
        elif opt in ('--list-datasource'):
            list_ds = True
        elif opt in ( '--list-branch'):
            list_branch = True
        elif opt in ('--list-template'):
            list_template = True
        elif opt in ('--refresh'):
            refresh = True
        elif opt in ('--domain'):
            dlpx_domain = arg
        elif opt in ('--bookmark'):
            bookmark_name = arg
        elif opt in ('--delete-template'):
            rm_template = True
        elif opt in ('--template'):
            template_name = arg
        elif opt in ('--timestamp'):
            timestamp = arg
        elif opt in ('--create-template'):
            create_template = True
        elif opt in ('--create-container'):
            create_container = True
        elif opt in ('--container'):
            container_name = arg
        elif opt in ('--database'):
            database_name = arg
        elif opt in ('--delete-container'):
            rm_container = True
        elif opt in ('--delete-branch'):
            rm_branch = True
        elif opt in ('--create-branch'):
            create_branch = True
        elif opt in ('--branch'):
            branch_name = arg
        elif opt in ('--create-bookmark'):
            create_bookmark = True
        elif opt in ('--delete-bookmark'):
            rm_bookmark = True
        elif opt in ('--update-branch'):
            update_branch = True
        elif opt in ('--activate-branch'):
            activate_branch = True
        elif opt in ('--update-bookmark'):
            update_bookmark = True

    try:
        engine = DelphixEngine(dlpx_engine, dlpx_user, dlpx_password,
                               dlpx_domain)

    except dlpxExceptionHandler, e:
            print e.errors

    if list_container == True:
        header = '\nName\tActive Branch\tOwner\tReference\t\t' \
                 'Template\t\tLast Updated'
        js_containers = container.get_all(engine)

        print header
        for js_container in js_containers:
            last_updated = convertTimestamp(engine, 
                           js_container.last_updated[:-5])

            print js_container.name + '\t' + js_container.active_branch + \
                '\t' + str(js_container.owner) + '\t' + str(js_container.reference) + \
                '\t' + str(js_container.template) + '\t' + last_updated
        print '\n'

    elif activate_branch == True:
        try:
            assert branch_name is not '', '--branch option is required to ' \
                                 'activate a Jet Stream Branch.\n'

            branch_ref = getReference(engine, branch, branch_name)
            branch.activate(engine, branch_ref)
            print 'Branch %s was activated successfully.\n' % (branch_name)

        except AssertionError, e:
            print('\nAn error occurred updating the branch, %s:\n\n%s' % \
                  (branch, e))

        except RequestError, e:
            print('\nAn error occurred updating the branch:\n%s' % (e.message))
            sys.exit(1)

    elif update_bookmark == True:
        js_bookmark_obj = JSBookmark()

        msg = '--bookmark option is required to update a Jet Stream ' \
              'Bookmark.\n'

        try:
            updateJSObject(engine, bookmark_name, bookmark, js_bookmark_obj,
                           msg)
            print 'Bookmark %s was updated successfully.\n' % (bookmark_name)

        except dlpxExceptionHandler, e:
            print e.errors
            sys.exit(1)

    elif update_branch == True:
        js_branch_obj = JSBranch()

        msg = '--branch option is required to update a Jet Stream ' \
              'Branch.\n'

        try:
            updateJSObject(engine, branch_name, branch, js_branch_obj,
                           msg)
            print 'Branch %s was updated successfully.\n' % (branch_name)

        except dlpxExceptionHandler, e:
            print e.errors
            sys.exit(1)

    elif rm_bookmark == True:
        try:
            assert bookmark_name is not '', 'Bookmark option is required to ' \
                                 'delete a Jet Stream Bookmark.\n'

            bookmark_ref = getReference(engine, bookmark, bookmark_name)
            bookmark.delete(engine, bookmark_ref)
            print '\nBookmark %s is deleted.\n' % (bookmark_name)

        except dlpxExceptionHandler, e:
            print e.errors

        except RequestError, e:
            print('\nAn error occurred creating the bookmark:\n%s' % (e.message))
            sys.exit(1)

        except AssertionError, e:
            print('\nAn error occurred creating the bookmark, %s:\n\n%s' % \
                  (bookmark_name, e))


    elif create_bookmark == True:
        try:
            assert bookmark_name and template_name \
                is not '', '\nBookmark and Template options are required to ' \
                           'create a Jet Stream Container.\n'
      
            js_bookmark_params = JSBookmarkCreateParameters()

            (source_layout_ref,branch_ref) = \
                    getReference(engine, template, template_name, True)

            js_bookmark_params.type = 'JSBookmarkCreateParameters'
            js_bookmark_params.bookmark = {'name': bookmark_name,
                               'branch': branch_ref, 'type': 'JSBookmark'}

            js_bookmark_params.timeline_point_parameters = {
                                'sourceDataLayout': source_layout_ref,
                                'type': 'JSTimelinePointLatestTimeInput'}

            bookmark.create(engine, js_bookmark_params)
            print '\nBookmark %s created successfully.\n' % (bookmark_name)

        except dlpxExceptionHandler, e:
            print e.errors
            sys.exit(1)

        except RequestError, e:
            print('An error occurred creating the bookmark:\n%s' % (e.message))
            sys.exit(1)

        except AssertionError, e:
            print('\nAn error occurred creating the bookmark, %s:\n\n%s' % \
                  (bookmark_name, e))

    elif create_container == True:
        js_container_params = JSDataContainerCreateParameters()

        try:
            assert container_name and template_name and database_name \
                is not '', 'Container, Template and database names ' \
                'are required to create a Jet Stream Container.\n'

            db_reference = getDBReference(engine, database_name)
            js_template_ref = getReference(engine, template, template_name)
            js_container_params.template = js_template_ref

            js_container_params.timeline_point_parameters = {
                                'sourceDataLayout': js_template_ref,
                                'type': 'JSTimelinePointLatestTimeInput'}

            js_container_params.data_sources = [{'container': db_reference,
                               'source': {'name': container_name,
                               'type': 'JSDataSource'},
                               'type': 'JSDataSourceCreateParameters'}]

            js_container_params.name = container_name

            container_ret_val = container.create(engine, js_container_params)
            print 'Container %s created successfully.' % (container_name)

        except RequestError, e:
            print('\n', e.message)
            sys.exit(1)

        except AssertionError, e:
            print('\nAn error occurred creating the container, %s:\n\n%s' % \
                  (container_name, e))

    elif rm_container == True:
        try:
            container_ref = getReference(engine, container, container_name)
            container.delete(engine, container_ref)

            print '\nContainer %s is deleted.\n' % container_name

        except dlpxExceptionHandler, e:
            print('\nContainer %s was not deleted. The error was:\n%s\n' % \
                 (container_name, e.errors))
            sys.exit(1)

        except RequestError, e:
            print('\n', e.message)
            sys.exit(1)

    elif list_jsbm == True:
        header = 'Name\t\tBranch\t\tReference\tTemplate\tTimestamp'
        js_bookmarks = bookmark.get_all(engine)

        print header
        for js_bookmark in js_bookmarks:
            timestamp = convertTimestamp(engine, js_bookmark.timestamp[:-5])

            print js_bookmark.name + '\t' + js_bookmark.branch + \
                '\t' + js_bookmark.reference + '\t' + timestamp

    elif list_ds == True:
        header = 'Name\t\tReference\tContainer\t\tDatatbase Name'
        js_datasources = datasource.get_all(engine)

        print header
        for js_datasource in js_datasources:
            print js_datasource.name + '\t' + js_datasource.reference + \
                '\t' + js_datasource.container + '\t' + \
                js_datasource.runtime.database_name
                  
    elif rm_branch == True:
        try:
            branch_ref = getReference(engine, branch, branch_name)
            branch.delete(engine, branch_ref)
            print 'Branch %s is deleted.\n' % (branch_name)
 
        except RequestError, e:
            print('\n', e.message)
            sys.exit(1)

        except JobError, e:
            print 'Error deleting branch %s: \n%s\n' % (branch_name, e.message)

    elif create_branch == True:
        try:
            assert container_name and template_name and branch_name \
                is not '', 'Container name, Template name and branch name' \
                ' options are required to create a Jet Stream Branch.\n'
      
            js_branch_params = JSBranchCreateParameters()
            data_container_ref = getReference(engine, container, container_name)
            source_layout_ref = getReference(engine, template, template_name)

            js_branch_params.type = 'JSBranchCreateParameters'
            js_branch_params.name = branch_name
            js_branch_params.data_container = data_container_ref
            js_branch_params.timeline_point_parameters = {
                                'sourceDataLayout': source_layout_ref,
                                'type': 'JSTimelinePointLatestTimeInput'}

            branch.create(engine, js_branch_params)
            print 'Branch %s created successfully.\n' % (branch_name)

        except AssertionError, e:
            print('\nAn error occurred creating the container, %s:\n\n%s' % \
                  (container_name, e))

        except RequestError, e:
            print('\n', e.message)
            sys.exit(1)

    elif list_branch == True:
        header = 'Name\tReference\tJSBranch Name'
        js_branches = branch.get_all(engine)

        print header
        for js_branch in js_branches:
            print js_branch.name + '\t' + js_branch.reference + \
            '\t' + js_branch._name[0]

    elif list_template == True:
        js_list = ['name','reference','active_branch','last_updated']
        header = 'Name\t\t\tReference\t\tActive Branch\tLast Updated'
        js_templates = template.get_all(engine)
        print header

        for js_template in js_templates:
            last_updated = convertTimestamp(engine, 
                           js_template.last_updated[:-5])

            print js_template.name, js_template.reference, \
            js_template.active_branch, last_updated

    elif create_template == True:
        assert template_name and database_name is not '', 'The template name' \
                '  and database name are required for creating a JS Template.\n'

        js_template_params = JSDataTemplateCreateParameters()

        template_ds_lst = []
        for db in database_name.split(','):
            template_ds_lst.append(build_ds_params(engine, db))

        try:
            js_template_params.data_sources = template_ds_lst
            js_template_params.name = template_name
            js_template_params.type = 'JSDataTemplateCreateParameters'
            
            template_ret_val = template.create(engine, js_template_params)
            print('Template %s was created successfully with reference %s\n' % \
                 (template_name, template_ret_val))

        except dlpxExceptionHandler, e:
            print('\nThe template %s was not created. The error was:\n\n%s' % \
                 (template_name, e.errors))
            sys.exit(1)

        except AssertionError, e:
            print('\nAn error occurred creating the container, %s:\n\n%s' % \
                  (container_name, e))


    elif rm_template == True:
        try:
            assert template_name is not '', 'The template name is required ' \
                   'for deleting a JS Template.\n'

            template_ref = getReference(engine, template, template_name)
            template.delete(engine, template_ref)

            print 'Template %s is deleted.' % (template_name)

        except dlpxExceptionHandler, e:
            print('\nThe template %s was not deleted. The error was:\n\n%s' % \
                  (template_name, e.errors))
            sys.exit(1)

        except RequestError, e:
            print('\nThe template %s was not deleted. The error was:\n\n%s' % \
                  (template_name, e.message))

        except HttpError, e:
            print('\nThe template %s was not deleted. The error was:\n\n%s' % \
                  (template_name, e.message))

        except AssertionError, e:
            print('\nAn error occurred creating the container, %s:\n\n%s' % \
                  (container_name, e))

        except:
            print 'Caught an exception in main:', sys.exc_info()[1]
            sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
