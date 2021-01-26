#!/usr/bin/env python
from delphixpy.v1_6_0 import job_context
from delphixpy.v1_6_0.delphix_engine import DelphixEngine
from delphixpy.v1_6_0.web import database
from delphixpy.v1_6_0.web import group

group_name = "Dev Copies"
# database_name = "Employee DB - Dev"

server_session = DelphixEngine(
    "landsharkengine", "delphix_admin", "landshark", "DOMAIN"
)

all_groups = group.get_all(server_session)

for each in all_groups:
    if group_name == each.name:
        group_reference = each.reference
        break

database_objs = database.get_all(server_session, group=group_reference)

with job_context.asyncly(server_session):
    for obj in database_objs:
        database.sync(server_session, obj.reference)
