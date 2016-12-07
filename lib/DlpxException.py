"""
Custom exception class for delphixpy scripts
"""

from DxLogging import print_exception

class DlpxException(Exception):
    """
    Delphix Exception class. Exit signals are handled by calling method.
    """


    def __init__(self, message):
        print_exception(message)
        Exception.__init__(self, message)
