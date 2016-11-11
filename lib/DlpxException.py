class DlpxException(Exception):
    """
    Delphix Exception class. Exit signals are handled by calling method.
    """


    def __init__(self, message):
        Exception.__init__(self, message)
