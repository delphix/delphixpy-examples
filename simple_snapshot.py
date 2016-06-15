#!/usr/bin/env python
from delphixpy.v1_6_0.delphix_engine import DelphixEngine
from delphixpy.v1_6_0.web import group, database


server_session= DelphixEngine("landsharkengine", "delphix_admin", "landshark", "DOMAIN")

group.get_all(server_session)