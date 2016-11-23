#!/usr/bin/env python

from lib.DlpxException import DlpxException
from lib.GetSession import GetSession
from delphixpy.v1_6_0.web import timeflow


x = GetSession()
x.get_config()

#print x.delphix_engines['landshark']['ip_address']
#print x.delphix_engines['landshark']['username']
try:
    x.serversess(x.delphix_engines['landshark']['ip_address'],
                 x.delphix_engines['landshark']['username'],
                 x.delphix_engines['landshark']['password'])

except (HttpError, JobError, KeyError):
    print '\nWARNING: No password defined. Using SSH Keys instead.\n'
#    x.serversess(x.delphix_engines['landshark']['ip_address'], 
#                 x.delphix_engines['landshark']['username'])

print x.server_session

all_timeflows = timeflow.get_all(x.server_session)
print all_timeflows

