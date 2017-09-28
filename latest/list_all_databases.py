#!/usr/bin/env python
from delphixpy.delphix_engine import DelphixEngine
from delphixpy.web import database


server_session= DelphixEngine("landsharkengine", "delphix_admin", "landshark", "DOMAIN")

all_databases = database.get_all(server_session)

#print all_databases

print str(len(all_databases)) + " databases in the LandsharkEngine"

for each in all_databases:
	print each.name
