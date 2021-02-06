#!/usr/bin/env python
"""
Adam Bowen - Jan 2016
This script configures the Delphix Engine networking.
"""
from __future__ import print_function

import errno
import getopt
import logging
import signal
import sys
import time
import traceback
from os.path import basename
from socket import error as socket_error

from delphixpy.v1_8_0.delphix_engine import DelphixEngine
from delphixpy.v1_8_0.exceptions import HttpError
from delphixpy.v1_8_0.exceptions import JobError
from delphixpy.v1_8_0.web import network
from delphixpy.v1_8_0.web import service
from delphixpy.v1_8_0.web import system
from delphixpy.v1_8_0.web import user
from delphixpy.v1_8_0.web.vo import DNSConfig
from delphixpy.v1_8_0.web.vo import InterfaceAddress
from delphixpy.v1_8_0.web.vo import NetworkInterface
from delphixpy.v1_8_0.web.vo import NetworkRoute
from delphixpy.v1_8_0.web.vo import PasswordCredential
from delphixpy.v1_8_0.web.vo import SystemInfo
from delphixpy.v1_8_0.web.vo import User

VERSION = "v.2.3.002"


def system_serversess(f_engine_address, f_engine_username, f_engine_password):
    """
    Function to grab the server session
    """
    server_session = DelphixEngine(
        f_engine_address, f_engine_username, f_engine_password, "SYSTEM"
    )
    return server_session


def help():
    print(
        basename(__file__) + " [-e <engine ip>] [-o <old sysadmin "
        "password] [-p <new sysadmin password]"
    )
    print(
        "Script requires five parameters, the IP of the Delphix Engine, "
        "the default\n gateway, a comma delimited string of DNS servers, the "
        "initial sysadmin \n password to connect with, and the new sysadmin "
        "password you want to use"
    )
    print("-h - Prints this message")
    print(
        "-e <Delphix Engine IP>  - The IP to use to connect to the Delphix "
        "Engine. \nEngine must be up, unconfigured, and console screen must be "
        "green"
    )
    print(
        "-p <new sysadmin password>  - will set the sysadmin user to this " "password"
    )
    print(
        "-n <new Delphix Engine CIDR>  - will set the Delphix Engine to this "
        "IP address \n(i.e. 10.0.1.10/24)"
    )
    print(
        "-g <default gateway> - will set the default gateway to point to "
        "this \nIP address"
    )
    print(
        "-d <dns servers> - comma delimited string of dns servers to use \n"
        '(i.e. "4.2.2.2,192.168.2.1"")'
    )
    print("-v - Print version information and exit")
    sys.exit(2)


def logging_est():
    """
    Establish Logging
    """
    global debug
    logging.basicConfig(
        filename="landshark_setup.log",
        format="%(levelname)s:\%(asctime)s:%(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    print_info("Welcome to " + basename(__file__) + ", version " + VERSION)
    global logger
    debug = True
    logger = logging.getLogger()
    logger.setLevel(10)
    print_info("Debug Logging is enabled.")


def on_exit(sig, func=None):
    print_info("Shutdown Command Received")
    print_info("Shutting down prime_setup.py")
    sys.exit(0)


def print_debug(print_obj):
    """
    DEBUG Log-level
    """
    if debug == True:
        print("DEBUG: " + str(print_obj))
        logging.debug(str(print_obj))


def print_error(print_obj):
    """
    ERROR Log-level
    """
    print("ERROR: " + str(print_obj))
    logging.error(str(print_obj))


def print_info(print_obj):
    """
    INFO Log-level
    """
    print("INFO: " + str(print_obj))
    logging.info(str(print_obj))


def print_warning(print_obj):
    """
    WARNING Log-level
    """
    print("WARNING: " + str(print_obj))
    logging.warning(str(print_obj))


def set_exit_handler(func):
    signal.signal(signal.SIGTERM, func)


def time_elapsed():
    elapsed_minutes = round((time.time() - time_start) / 60, +1)
    return elapsed_minutes


def version():
    print("Version: " + VERSION)
    logging_est()
    set_exit_handler(on_exit)
    sys.exit(1)


def main(argv):
    try:
        logging_est()
        global time_start
        time_start = time.time()
        engine_ip = ""
        engine_pass = ""
        dg = ""
        dns_servers = ""
        try:
            opts, args = getopt.getopt(argv, "e:n:g:d:p:hv")
        except getopt.GetoptError:
            help()
        for opt, arg in opts:
            if opt == "-h":
                help()
            elif opt == "-e":
                engine_ip = arg
            elif opt == "-p":
                engine_pass = arg
            elif opt == "-n":
                new_engine_cidr = arg
            elif opt == "-g":
                dg = arg
            elif opt == "-d":
                dns_servers = arg
            elif opt == "-v":
                version()

        if (
            engine_ip == ""
            or engine_pass == ""
            or new_engine_cidr == ""
            or dg == ""
            or dns_servers == ""
        ):
            help()

        sys_server = system_serversess(engine_ip, "sysadmin", engine_pass)

        # Configure Static IP
        primary_interface = network.interface.get_all(sys_server)[0].reference
        print_debug("Primary interface identified as " + primary_interface)
        ni_obj = NetworkInterface()
        if_obj = InterfaceAddress()
        if_obj.address = new_engine_cidr
        if_obj.address_type = "STATIC"
        # if_obj.addressType = "DHCP"
        ni_obj.addresses = [if_obj]
        # print_debug(str(ni_obj))
        try:
            print_debug(
                "Changing the IP address. This operation can take up to 60 seconds to complete"
            )
            network.interface.update(sys_server, primary_interface, ni_obj)
        except socket_error as e:
            if e.errno == errno.ETIMEDOUT:
                print_debug("IP address changed")
            else:
                raise e
        # if we made it this far, we need to operate on the new IP.
        engine_ip = new_engine_cidr.split("/")[0]
        print_debug("ENGINE IP: " + engine_ip)
        # Now re-establish the server session
        sys_server = system_serversess(engine_ip, "sysadmin", engine_pass)

        # configure DNS
        print_debug("Setting DNS")
        dns_obj = DNSConfig()
        dns_obj.servers = dns_servers.split(",")
        dns_obj.domain = []
        service.dns.set(sys_server, dns_obj)

        # configue default gateway
        print_debug("Setting default gateway")
        de_routes = network.route.get_all(sys_server)
        print_debug("Current routes: ")
        print_debug(str(de_routes))
        default_gateway = NetworkRoute()
        default_gateway.destination = "default"
        default_gateway.out_interface = primary_interface
        # Check to see if a DG already exists. If so, delete it.
        for de_route in de_routes:
            if de_route.destination == "default":
                print_debug("Found an existing DG. Deleting it")
                default_gateway.gateway = dg
                network.route.delete(sys_server, default_gateway)
        default_gateway.gateway = dg
        print_debug("Adding new route")
        network.route.add(sys_server, default_gateway)
        de_routes = network.route.get_all(sys_server)
        print_debug("New routes: ")
        print_debug(str(de_routes))

    except SystemExit as e:
        sys.exit(e)
    except HttpError as e:
        print_error("Connection failed to the Delphix Engine")
        print_error("Please check the ERROR message below")
        print_error(e.message)
        sys.exit(2)
    except JobError as e:
        print_error("A job failed in the Delphix Engine")
        print_error(e.job)
        elapsed_minutes = time_elapsed()
        print_info("Prime took " + str(elapsed_minutes) + " minutes to get this far.")
        sys.exit(2)
    except KeyboardInterrupt:
        print_debug("You sent a CTRL+C to interrupt the process")
        elapsed_minutes = time_elapsed()
        print_info("Prime took " + str(elapsed_minutes) + " minutes to get this far.")
        sys.exit(2)
    except socket_error as e:
        print_error("Connection failed to the Delphix Engine")
        print_error("Please check the ERROR message below")
        if e.errno == errno.ETIMEDOUT:
            print_debug("Connection timed out trying to connect to " + engine_ip)
        else:
            print_error(e.message)
        sys.exit(2)
    except:
        print_error(sys.exc_info()[0])
        print_error(traceback.format_exc())
        elapsed_minutes = time_elapsed()
        print_info("Prime took " + str(elapsed_minutes) + " minutes to get this far.")
        sys.exit(2)


if __name__ == "__main__":
    main(sys.argv[1:])
