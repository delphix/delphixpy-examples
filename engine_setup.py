#!/usr/bin/env python
"""
Adam Bowen - Jan 2016
This script configures the sysadmin user and configures domain0
Will come back and properly throw this with logging, etc
"""
from __future__ import print_function

import getopt
import logging
import signal
import sys
import time
import traceback
from os.path import basename

import untangle

from delphixpy.v1_6_0.delphix_engine import DelphixEngine
from delphixpy.v1_6_0.exceptions import HttpError
from delphixpy.v1_6_0.exceptions import JobError
from delphixpy.v1_6_0.web import domain
from delphixpy.v1_6_0.web import storage
from delphixpy.v1_6_0.web import user
from delphixpy.v1_6_0.web.vo import CredentialUpdateParameters
from delphixpy.v1_6_0.web.vo import DomainCreateParameters
from delphixpy.v1_6_0.web.vo import PasswordCredential
from delphixpy.v1_6_0.web.vo import User
from lib.GetSession import GetSession

VERSION = "v.2.3.005"
CONTENTDIR = "/u02/app/content"


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
        "\n"
        + basename(__file__)
        + " [-e <engine ip>] [-o <old sysadmin password] [-p <new sysadmin password]"
    )
    print(
        "\n\nScript requires three parameters, the IP of the Delphix Engine, the initial sysadmin password to connect with,  and the new sysadmin password you want to use"
    )
    print("-h - Prints this message")
    print(
        "-e <Delphix Engine IP>  - Engine must be up, unconfigured, and console screen must be green"
    )
    print(
        "-o <old sysadmin password>  - will use this password to initially access the system"
    )
    print("-p <new sysadmin password>  - will set the sysadmin user to this password")
    print("-v - Print version information and exit")
    sys.exit(2)


def logging_est():
    """
    Establish Logging
    """
    global debug
    logging.basicConfig(
        filename="landshark_setup.log",
        format="%(levelname)s:%(asctime)s:%(message)s",
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
        dx_session_obj = GetSession()
        engine_ip = ""
        engine_pass = ""
        old_engine_pass = ""
        try:
            opts, args = getopt.getopt(argv, "e:o:p:hv")
        except getopt.GetoptError:
            help()
        for opt, arg in opts:
            if opt == "-h":
                help()
            elif opt == "-e":
                engine_ip = arg
            elif opt == "-o":
                old_engine_pass = arg
            elif opt == "-p":
                engine_pass = arg
            elif opt == "-v":
                version()

        if engine_ip == "" or engine_pass == "" or old_engine_pass == "":
            help()

        dx_session_obj.serversess(engine_ip, "sysadmin", old_engine_pass, "SYSTEM")

        dx_session_obj.server_wait()

        sys_server = system_serversess(engine_ip, "sysadmin", old_engine_pass)

        if user.get(sys_server, "USER-1").email_address == None:
            print_info("Setting sysadmin's email address")
            sysadmin_user = User()
            sysadmin_user.email_address = "spam@delphix.com"
            user.update(sys_server, "USER-1", sysadmin_user)
            print_info("Setting sysadmin's password")
            sysadmin_credupdate = CredentialUpdateParameters()
            sysadmin_credupdate.new_credential = PasswordCredential()
            sysadmin_credupdate.new_credential.password = engine_pass
            user.update_credential(sys_server, "USER-1", sysadmin_credupdate)
        else:
            print_info("sysadmin user has already been configured")

        try:
            sys_server = system_serversess(engine_ip, "sysadmin", engine_pass)
            domain.get(sys_server)
            print_info("domain0 already exists. Skipping domain0 creation.")
            elapsed_minutes = time_elapsed()
            print_info(
                "Prime took " + str(elapsed_minutes) + " minutes to get this far."
            )
            sys.exit(7)
        except HttpError as e:
            device_list = storage.device.get_all(sys_server)
            system_init_params = DomainCreateParameters()
            system_init_params.devices = [
                device.reference for device in device_list if not device.configured
            ]
            print_info("Creating storage domain")
            domain.set(sys_server, system_init_params)
            while True:
                try:
                    sys_server = system_serversess(engine_ip, "sysadmin", engine_pass)
                    domain.get(sys_server)
                except:
                    break
                print_info("Waiting for Delphix Engine to go down")
                time.sleep(3)

        dx_session_obj.serversess(engine_ip, "sysadmin", engine_pass, "SYSTEM")

        dx_session_obj.server_wait()

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
    except:
        print_error(sys.exc_info()[0])
        print_error(traceback.format_exc())
        elapsed_minutes = time_elapsed()
        print_info("Prime took " + str(elapsed_minutes) + " minutes to get this far.")
        sys.exit(2)


if __name__ == "__main__":
    main(sys.argv[1:])
