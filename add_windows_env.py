#!/usr/bin/env python
# Adam Bowen Sept 2016
VERSION = "v.0.0.001"
# just a quick and dirty example of adding a windows source

from delphixpy.delphix_engine import DelphixEngine
from delphixpy.web import environment
from delphixpy.web.vo import EnvironmentUser
from delphixpy.web.vo import HostEnvironmentCreateParameters
from delphixpy.web.vo import PasswordCredential
from delphixpy.web.vo import WindowsHost
from delphixpy.web.vo import WindowsHostCreateParameters
from delphixpy.web.vo import WindowsHostEnvironment

engine_address = "192.168.2.37"
engine_username = "delphix_admin"
engine_password = "landshark"


def serversess(f_engine_address, f_engine_username, f_engine_password):
    """
    Function to setup the session with the Delphix Engine
    """
    server_session = DelphixEngine(
        f_engine_address, f_engine_username, f_engine_password, "DOMAIN"
    )
    return server_session


server = serversess(engine_address, engine_username, engine_password)

envCreateParams = HostEnvironmentCreateParameters()


envCreateParams.primary_user = EnvironmentUser()
envCreateParams.primary_user.name = "delphix\delphix_admin"
envCreateParams.primary_user.credential = PasswordCredential()
envCreateParams.primary_user.credential.password = "delphix"
envCreateParams.host_environment = WindowsHostEnvironment()
envCreateParams.host_environment.name = "WINDOWSSOURCE"
envCreateParams.host_environment.proxy = "WINDOWS_HOST-6"  # This is the Host ID of the Windows Server that houses the connector
envCreateParams.host_parameters = WindowsHostCreateParameters()
envCreateParams.host_parameters.host = WindowsHost()
envCreateParams.host_parameters.host.address = "WINDOWSSOURCE"

environment.create(server, envCreateParams)
