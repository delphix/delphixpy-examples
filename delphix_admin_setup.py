#!/usr/bin/env python3

import getopt
import logging
import signal
import sys
import time
import traceback
from os.path import basename

from delphixpy.v1_11_16.delphix_engine import DelphixEngine
from delphixpy.v1_11_16.exceptions import HttpError
from delphixpy.v1_11_16.exceptions import JobError
from delphixpy.v1_11_16.web import user
from delphixpy.v1_11_16.web import system
from delphixpy.v1_11_16.web import domain
from delphixpy.v1_11_16.web import vo
#from delphixpy.v1_16_11.web.vo import PasswordCredential
#from delphixpy.v1_16_11.web.vo import User

from lib import dlpx_exceptions
from lib import dx_logging as log
from lib import get_references
from lib import get_session
from lib import run_job
from lib.run_async import run_async

"""
This script configures the delphix_admin user after domain0 is configured
Will come back and properly throw this with logging, etc
"""
VERSION = "v.3.0.002"


def serversess(f_engine_address, f_engine_username, f_engine_password, system_init=True):
    """
    Function to grab the server session
    """
    if system_init is True:
        server_session = DelphixEngine(
            f_engine_address, f_engine_username, f_engine_password, "SYSTEM"
        )
    else:
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
    sys.exit(2)


def on_exit(sig, func=None):
    log.print_info("Shutdown Command Received")
    log.print_info("Shutting down prime_setup.py")
    sys.exit(0)


def set_exit_handler(func):
    signal.signal(signal.SIGTERM, func)


def time_elapsed(time_start):
    elapsed_minutes = round((time.time() - time_start) / 60, +1)
    return elapsed_minutes


def main(argv):
    try:
        # ADD OPTION FOR LOG
        log.logging_est("delphix_admin.log")
        time_start = time.time()
        engine_ip = ""
        engine_pass = ""
        old_engine_pass = ""
        try:
            opts, args = getopt.getopt(argv, "t:e:o:p:hv")
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
            elif opt == "-t":
                engine_type = arg.upper()
#        if not engine_ip or not engine_pass or not old_engine_pass:
#            help()

        server = serversess(engine_ip, "sysadmin", old_engine_pass)
        init_engine = vo.SystemInitializationParameters()
        engine_system = vo.SystemInfo()
        if engine_type == "MASKING":
            engine_system.engine_type =  "MASKING"
            system.start_masking(server)
        elif engine_type == "VIRTUALIZATION":
            system.stop_masking(server)
            engine_system.engine_type =  "VIRTUALIZATION"
        system.set(server, system_info=engine_system)
        init_engine.default_user = "admin"
        init_engine.default_password = engine_pass
        init_engine.default_email = "spam@delphix.com"
        init_engine.devices = ["STORAGE_DEVICE-xvdb"]
        domain.initialize_system(server, init_engine)
        domain_server = serversess(engine_ip, "admin", "delphix", False)
        #if user.get(server, "USER-2").email_address is None:
        time.sleep(120)
        if user.get(domain_server, "USER-2").email_address is None:
            log.print_debug("Setting admin's email address")
            admin_user = vo.User()
            admin_user.email_address = "spam@delphix.com"
            user.update(domain_server, "USER-2", admin_user)
            log.print_debug("Setting admin's password")
            admin_credupdate = vo.CredentialUpdateParameters()
            admin_credupdate.new_credential = vo.PasswordCredential()
            admin_credupdate.new_credential.password = engine_pass
            user.update_credential(domain_server, "USER-2", delphix_admin_credupdate)
        else:
            log.print_info("The delphix_admin user has already been setup")
        

    except SystemExit as e:
        sys.exit(e)
    except HttpError as err:
        log.print_exception("Connection failed to the Delphix Engine")
        log.print_exception("Please check the ERROR message below")
        log.print_exception(err)
        sys.exit(2)
    except JobError as e:
        log.print_exception("A job failed in the Delphix Engine")
        log.print_exception(e.job)
        elapsed_minutes = time_elapsed()
        log.print_info("took " + str(elapsed_minutes) + " minutes to get this far.")
    except KeyboardInterrupt:
        log.print_debug("You sent a CTRL+C to interrupt the process")
        elapsed_minutes = time_elapsed()
        log.print_info("took " + str(elapsed_minutes) + " minutes to get this far.")
    except:
        log.print_exception(sys.exc_info()[0])
        log.print_exception(traceback.format_exc())
        elapsed_minutes = time_elapsed(time_start)
        log.print_info("took " + str(elapsed_minutes) + " minutes to get this far.")


if __name__ == "__main__":
    main(sys.argv[1:])
