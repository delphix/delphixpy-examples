#!/usr/bin/env python

from lib.DxLogging import print_exception

import sys
import re
from os.path import basename
from time import sleep, time
from docopt import docopt
import ssl

from delphixpy.exceptions import HttpError
from delphixpy.exceptions import JobError
from delphixpy.exceptions import RequestError
from delphixpy.web import sourceconfig
from delphixpy.web import environment
from delphixpy.web.database import link
from delphixpy.web.vo import OracleSIConfig
from delphixpy.web.vo import OracleProvisionParameters
from delphixpy.web.vo import OracleService
from delphixpy.web.vo import OracleInstance
from delphixpy.web.vo import OracleVirtualSource
from delphixpy.web.vo import LinkParameters
from delphixpy.web.vo import OracleLinkData
from delphixpy.web.vo import OracleSourcingPolicy
from delphixpy.web.vo import LinkedSourceOperations
#from delphixpy.web import repository

from lib.GetReferences import find_obj_by_name
from lib.GetReferences import find_dbrepo
from lib.GetSession import GetSession

if hasattr(ssl, '_create_unverified_context'):
           ssl._create_default_https_context = ssl._create_unverified_context

dx_session_obj = GetSession()
dx_session_obj.serversess('172.16.169.146', 'delphix_admin', 'delphix', use_https=True)

##env_ref = find_obj_by_name(dx_session_obj.server_session, environment,
##                           'NikeSource').reference
##
##x = find_dbrepo(dx_session_obj.server_session, 'OracleInstall', env_ref,
##                '/u01/app/oracle/product/11.2.0.4/dbhome_1').reference
##
##dsource_params = OracleSIConfig()
##
##dsource_params.database_name = 'nikesrc1'
##dsource_params.unique_name = 'nikesrc1'
##dsource_params.repository = x
##dsource_params.instance = OracleInstance()
##dsource_params.instance.instance_name = 'nikesrc1'
##dsource_params.instance.instance_number = 2
##dsource_params.services = [{'type': 'OracleService','jdbcConnectionString': 'jdbc:oracle:thin:@192.168.166.11:1521:nikesrc1'}]
##p = sourceconfig.create(dx_session_obj.server_session, dsource_params)
##

x = find_obj_by_name(dx_session_obj.server_session, environment,
                     'NikeSource')
print x
sys.exit(1)
link_params = LinkParameters()
link_params.link_data = OracleLinkData()
link_params.link_data.sourcing_policy = OracleSourcingPolicy()
#link_params.link_data.operations = LinkedSourceOperations()
link_params.name = 'nikesrc1'
link_params.group = 'GROUP-34'
link_params.link_data.compressedLinkingEnabled = True
link_params.link_data.environment_user = 'HOST_USER-9'
link_params.link_data.db_user = 'delphixdb'
link_params.link_data.number_of_connections = 1
link_params.link_data.link_now = True
link_params.link_data.files_per_set = 5
link_params.link_data.rman_channels = 2
link_params.link_data.skip_space_check = False
link_params.link_data.db_credentials = {'type': 'PasswordCredential',
                                       'password':'delphix'}
link_params.link_data.sourcing_policy.logsync_enabled = True
link_params.link_data.sourcing_policy.logsync_mode = 'ARCHIVE_REDO_MODE'
#This is the sourceconfig reference
link_params.link_data.config = 'ORACLE_SINGLE_CONFIG-22'
#link_params.link_data.operations.double_sync = False

x = link(dx_session_obj.server_session, link_params)
print x

#{"type":"LinkParameters","name":"nikesrc1","group":"GROUP-34","linkData":{"type":"OracleLinkData","compressedLinkingEnabled":true,"checkLogical":false,"externalFilePath":"","environmentUser":"HOST_USER-9","bandwidthLimit":0,"dbUser":"delphixdb","numberOfConnections":1,"preProvisioningEnabled":false,"dbCredentials":{"type":"PasswordCredential","password":"delphix"},"linkNow":true,"backupLevelEnabled":false,"filesPerSet":5,"rmanChannels":2,"skipSpaceCheck":false,"sourcingPolicy":{"type":"OracleSourcingPolicy","logsyncEnabled":true,"logsyncMode":"ARCHIVE_REDO_MODE"},"operations":{"type":"LinkedSourceOperations","preSync":[],"postSync":[]},"config":"ORACLE_SINGLE_CONFIG-14","doubleSync":false},"description":""}

#{"name":"nikesrc1","databaseName":"nikesrc1","uniqueName":"nikesrc1","type":"OracleSIConfig","environmentUser":"HOST_USER-9","instance":{"type":"OracleInstance","instanceName":"nikesrc1","instanceNumber":1},"repository":"ORACLE_INSTALL-1","linkingEnabled":true}


#=== POST /resources/json/delphix/sourceconfig ===
#{
#    "type": "OracleSIConfig",
#    "repository": "ORACLE_INSTALL-1",
#    "databaseName": "nikesrc1",
#    "uniqueName": "nikesrc1",
#    "instance": {
#        "type": "OracleInstance",
#        "instanceName": "nikesrc1",
#        "instanceNumber": 1
#    }
#}
#dsource_params.services = OracleService()
#dsource_params.services = {'jdbc_connection_string':'jdbc:oracle:thin:@192.168.166.11:1521:nikesrc1'}

#dsource_params.container = OracleDatabaseContainer()
#dsource_params.container.group = vdb_group_obj.reference
#dsource_params.container.name = vdb_name
#dsource_params.source = OracleVirtualSource()


