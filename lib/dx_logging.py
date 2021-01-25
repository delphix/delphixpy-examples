"""
Package DxLogging
"""

import logging

VERSION = "v.0.3.000"


def logging_est(logfile_path, debug=False):
    """
    Establish Logging

    :param logfile_path: path to the logfile. Default: current directory.
    :type logfile_path: str
    :param debug: Set debug mode on (True) or off (False).
    :type debug: bool
    """
    logging.basicConfig(
        filename=logfile_path,
        format="%(levelname)s:%(asctime)s:%(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger()
    if debug is True:
        logger.setLevel(10)
        print_info("Debug Logging is enabled.")


def print_debug(print_obj):
    """
    Call this function with a log message to prefix the message with DEBUG
    :param print_obj: Object to print to logfile and stdout
    :type print_obj: type depends on objecting being passed. Typically str
    """
    print(f"DEBUG: {str(print_obj)}")
    logging.debug(str(print_obj))


def print_info(print_obj):
    """
    Call this function with a log message to prefix the message with INFO
    :param print_obj: Object to print to logfile and stdout
    :type print_obj: type depends on objecting being passed. Typically str
    """
    print(f"INFO: {print_obj}")
    logging.info(str(print_obj))


def print_warning(print_obj):
    """
    Call this function with a log message to prefix the message with INFO
    :param print_obj: Object to print to logfile and stdout
    :type print_obj: type depends on objecting being passed. Typically str
    """
    print(f"WARN: {print_obj}")
    logging.warning(str(print_obj))


def print_exception(print_obj):
    """
    Call this function with a log message to prefix the message with EXCEPTION
    :param print_obj: Object to print to logfile and stdout
    :type print_obj: type depends on objecting being passed. Typically str

    """
    print(str(print_obj))
    logging.error("EXCEPTION: %s" % (str(print_obj)))
