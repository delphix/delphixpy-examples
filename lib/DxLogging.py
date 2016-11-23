VERSION = 'v.0.0.001'

def print_debug(print_obj):
    """
    Call this function with a log message to prefix the message with DEBUG
    """
    try:
        if debug == True:
            print "DEBUG: " + str(print_obj)
            logging.debug(str(print_obj))
    except:
        pass


def print_info(print_obj):
    """
    Call this function with a log message to prefix the message with INFO
    """
    print "INFO: " + str(print_obj)
    logging.info(str(print_obj))
