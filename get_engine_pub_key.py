#!/usr/bin/env python
"""
Adam Bowen - May 2017
This script grabs
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
from delphixpy.v1_6_0.web import system
from lib.GetSession import GetSession

VERSION = "v.2.3.003"


def system_serversess(f_engine_address, f_engine_username, f_engine_password):
    """
    Function to grab the server session
    """
    server_session = DelphixEngine(
        f_engine_address, f_engine_username, f_engine_password, "SYSTEM"
    )
    return server_session


def help():
    print("\n" + basename(__file__) + " [-e <engine ip>] [-p <sysadmin password]")
    print(
        "\n\nScript requires two parameters, the IP of the Delphix Engine and the sysadmin password you want to use"
    )
    print("-h - Prints this message")
    print(
        "-e <Delphix Engine IP>  - Engine must be up and console screen must be green"
    )
    print("-p <sysadmin password>  - sysadmin password")
    print("-d <directory> - directory where key will be saved")
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
            opts, args = getopt.getopt(argv, "e:d:p:hv")
        except getopt.GetoptError:
            help()
        for opt, arg in opts:
            if opt == "-h":
                help()
            elif opt == "-e":
                engine_ip = arg
            elif opt == "-p":
                engine_pass = arg
            elif opt == "-d":
                key_path = arg + "/engine_key.pub"
            elif opt == "-v":
                version()

        if engine_ip == "" or engine_pass == "":
            help()

        dx_session_obj.serversess(engine_ip, "sysadmin", engine_pass, "SYSTEM")
        dx_session_obj.server_wait()

        sys_server = system_serversess(engine_ip, "sysadmin", engine_pass)
        system_info = system.get(sys_server)
        print_info(system_info.ssh_public_key)
        print_info("Writing to " + key_path)
        target = open(key_path, "w")
        target.write(system_info.ssh_public_key)
        target.close
        print_info("File saved")
        elapsed_minutes = time_elapsed()
        print_info("Script took " + str(elapsed_minutes) + " minutes to get this far.")

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
