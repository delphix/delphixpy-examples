#!/usr/bin/env python
# Program Name : database.py
# Description  : List all vDB and dSources from a given engine
# Author       : Corey Brune
# Created: May 10, 2016 (v1.0.0)
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
# v1.0.1

#Python imports
import sys
import getopt
from datetime import datetime
from dateutil import tz
import ConfigParser

#delphixpy imports
from delphixpy.v1_8_2.delphix_engine import DelphixEngine
from delphixpy.v1_8_2.web.database import database
from delphixpy.v1_8_2.exceptions import RequestError
from delphixpy.v1_8_2.exceptions import JobError
from delphixpy.v1_8_2.exceptions import HttpError


class dlpxExceptionHandler(Exception):
    def __init__(self, message):
        self.message = message


def usage():
    print('\nUSAGE: \n\nRequired for all operations:\n--config-file [-f] <file> '
          '[--list-database [-d]]\n')

    sys.exit(1)

def getAuthenticated(auth_dct):
    dlpx_user = auth_dct.get('user')
    dlpx_password = auth_dct.get('password')
    dlpx_engine = auth_dct.get('engine')
    dlpx_domain = 'DOMAIN'

    try:
        return(DelphixEngine(dlpx_engine.strip('\''), dlpx_user.strip('\''),
                          dlpx_password.strip('\''), dlpx_domain, True))

    except (RequestError, HttpError), e:
        raise dlpxExceptionHandler('An error occured while authenticating to \
              the engine: %s.\n' % (e.message))

def main(argv):
    config_file = ''

    try:
         opts, args = getopt.getopt(argv, 'hdf:',
                      ['help', 'config-file=', 'list-database'])

    except getopt.GetoptError, e:
         print e
         usage()

    for opt, arg in opts:
        if opt in ('-h'):
            usage()
        elif opt in ('--list-database'):
            list_database = True
        elif opt in ('--config-file', '-f'):
            config_file = arg

    try:
        assert config_file is not '', '-f <config file> is required.\n'

        cf = ConfigParser.ConfigParser()
        cf.read(config_file)

        properties_dct = {}
        for section in cf.sections():
            properties_dct[section] = {}
 
            for option in cf.options(section):
                properties_dct[section][option] = cf.get(section, option)

        engine = getAuthenticated(properties_dct.pop('engine'))

        for db in database.get_all(engine):
            if db.provision_container == None:
                db.provision_container = 'dSource'

            print 'name = ', str(db.name), '\n', 'current timeflow = ', \
                  str(db.current_timeflow), '\n', \
                  'provision container = ', str(db.provision_container), '\n', \
                  'description = ', str(db.description), '\n', \
                  'group = ', str(db.group), '\n', 'masked = ', \
                  str(db.masked), '\n', 'namespace = ', str(db.namespace), \
                  '\n', 'os = ', str(db.os), '\n', 'performance mode = ', \
                  str(db.performance_mode), '\n', 'processor = ', \
                  str(db.processor), '\n\n'
            
    except dlpxExceptionHandler, e:
        print e.message
        sys.exit(1)

    except (RequestError, HttpError), e:
        apiRetVal = e.message
        print('\nAn error occurred updating the branch:\n%s' %
             (apiRetVal.action))
        sys.exit(1)

    except AssertionError, e:
        print('\nAn error occurred listing databases:\n%s\n' % (e.message))
        usage()

    except AttributeError:
        pass

    except:
        print 'Caught an exception listing databases:', sys.exc_info()[1]
        sys.exit(1)



if __name__ == "__main__":
    main(sys.argv[1:])
