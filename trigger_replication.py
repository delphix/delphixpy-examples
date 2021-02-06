#!/usr/bin/env python
# Adam Bowen Sept 2016
from __future__ import print_function

from delphixpy.v1_6_0.delphix_engine import DelphixEngine
from delphixpy.v1_6_0.web import replication
from delphixpy.v1_6_0.web.vo import ReplicationSpec

VERSION = "v.0.0.002"
# just a quick and dirty example of executing a replication profile


engine_address = "192.168.218.177"
engine_username = "delphix_admin"
engine_password = "landshark"

replication_profile_name = "Example Replication Profile"


def serversess(f_engine_address, f_engine_username, f_engine_password):
    """
    Function to setup the session with the Delphix Engine
    """
    server_session = DelphixEngine(
        f_engine_address, f_engine_username, f_engine_password, "DOMAIN"
    )
    return server_session


def find_obj_by_name(server, f_class, obj_name):
    """
    Function to find objects by name and object class, and return object's reference as a string
    You might use this function to find objects like groups.
    """
    print(
        "Searching objects in the "
        + f_class.__name__
        + ' class\n   for one named "'
        + obj_name
        + '"'
    )
    obj_ref = ""

    all_objs = f_class.get_all(server)
    for obj in all_objs:
        if obj.name == obj_name:
            print("Found a match " + str(obj.reference))
            return obj


server = serversess(engine_address, engine_username, engine_password)

replication_list = replication.spec.get_all(server)

print("##### REPLICATION LIST #######")
for obj in replication_list:
    print(obj.name)
print("##### END REPLICATION LIST #######")

replication_spec = find_obj_by_name(server, replication.spec, replication_profile_name)

print("##### REPLICATION PROFILE: " + replication_profile_name + " #######")
print(replication_spec.reference)

print("Executing " + replication_profile_name)

replication.spec.execute(server, replication_spec.reference)

print(replication_profile_name + " executed.")
