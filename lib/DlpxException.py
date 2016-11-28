"""
Custom exception class for delphixpy scripts
"""

from DxLogging import print_warning

class DlpxException(Exception):
    """
    Delphix Exception class. Exit signals are handled by calling method.
    """


    def __init__(self, message):
        print_warning(message)
        Exception.__init__(self, message)
