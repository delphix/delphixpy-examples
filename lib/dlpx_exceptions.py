"""
Custom exception class for delphixpy scripts
"""


class DlpxException(BaseException):
    """
    Delphix Exception class. Exit signals are handled by calling method.
    """

    def __init__(self, error):
        super(DlpxException, self).__init__(error)
        self._error = error

    @property
    def error(self):
        """
        Return an DlpxException object describing this error.
        """
        return self.error


class DlpxObjectNotFound(BaseException):
    """
    Delphix Exception class. Exit signals are handled by calling method.
    Raised when a Delphix Object is not found
    """

    def __init__(self, message):
        super(DlpxObjectNotFound, self).__init__(message)
        self._message = message

    @property
    def message(self):
        """
        Return an ErrorResult object describing this request message.
        """
        return self._message


class DlpxObjectExists(BaseException):
    """
    Delphix Exception class. Exit signals are handled by calling method.
    Raised when a Delphix Object is found
    """

    def __init__(self, message):
        super(DlpxObjectExists, self).__init__(message)
        self._message = message

    @property
    def message(self):
        """
        Return an ErrorResult object describing this request message.
        """
        return self._message
