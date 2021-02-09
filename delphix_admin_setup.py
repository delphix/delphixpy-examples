#!/usr/bin/env python
"""
Adam Bowen - Jan 2016
This script configures the delphix_admin user after domain0 is configured
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

from delphixpy.v1_6_0.delphix_engine import DelphixEngine
from delphixpy.v1_6_0.exceptions import HttpError
from delphixpy.v1_6_0.exceptions import JobError
from delphixpy.v1_6_0.web import user
from delphixpy.v1_6_0.web.vo import CredentialUpdateParameters
from delphixpy.v1_6_0.web.vo import PasswordCredential
from delphixpy.v1_6_0.web.vo import User

VERSION = "v.2.3.002"
CONTENTDIR = "/u02/app/content"


def serversess(f_engine_address, f_engine_username, f_engine_password):
    """
    Function to grab the server session
    """
    server_session = DelphixEngine(
        f_engine_address, f_engine_username, f_engine_password, "DOMAIN"
    )
    return server_session


def help():
    print(
        "\n"
        + basename(__file__)
        + " [-e <engine ip>] [-o <old delphix_admin password] [-p <new delphix_admin password]"
    )
    print(
        "\n\nScript requires three parameters, the IP of the Delphix Engine, the initial delphix_admin password to connect with,  and the new delphix_admin password you want to use"
    )
    print("-h - Prints this message")
    print(
        "-e <Delphix Engine IP>  - Engine must be up, unconfigured, and console screen must be green"
    )
    print(
        "-o <old delphix_admin password>  - will use this password to initially access the system"
    )
    print(
        "-p <new delphix_admin password>  - will set the delphix_admin user to this password"
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

        server = serversess(engine_ip, "delphix_admin", old_engine_pass)

        if user.get(server, "USER-2").email_address == None:
            print_debug("Setting delphix_admin's email address")
            delphix_admin_user = User()
            delphix_admin_user.email_address = "spam@delphix.com"
            user.update(server, "USER-2", delphix_admin_user)

            print_debug("Setting delphix_admin's password")
            delphix_admin_credupdate = CredentialUpdateParameters()
            delphix_admin_credupdate.new_credential = PasswordCredential()
            delphix_admin_credupdate.new_credential.password = engine_pass
            user.update_credential(server, "USER-2", delphix_admin_credupdate)
        else:
            print_info("The delphix_admin user has already been setup")

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
    except KeyboardInterrupt:
        print_debug("You sent a CTRL+C to interrupt the process")
        elapsed_minutes = time_elapsed()
        print_info("Prime took " + str(elapsed_minutes) + " minutes to get this far.")
    except:
        print_error(sys.exc_info()[0])
        print_error(traceback.format_exc())
        elapsed_minutes = time_elapsed()
        print_info("Prime took " + str(elapsed_minutes) + " minutes to get this far.")


if __name__ == "__main__":
    main(sys.argv[1:])
