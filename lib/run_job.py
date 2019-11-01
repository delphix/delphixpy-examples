"""
This does stuff
"""
from delphixpy.v1_10_2 import exceptions

from lib import dx_logging
from lib import dlpx_exceptions


def run_job(main_func, dx_obj, config_file='./dxtools.conf', engine='all'):
    """
    This method runs the main_func asynchronously against all the
    servers specified
    :param main_func: Name of the function to execute the job against
    :type main_func: function
    :param dx_obj: Delphix session object containing all engines from config
    :type dx_obj: lib.get_session.GetSession object
    :param config_file: Name of the configuration file, commonly dxtools.conf
    :type config_file: str
    :param engine: name of an engine, all or None
    :type engine: str
    :return: Generator of dlpx_engines
    """
    threads = []
    # If "all" argument was given, run against every engine in config_file
    if engine == 'all':
        dx_logging.print_info(f'Executing against all Delphix DDP in '
                              f'the {config_file}')
        try:
            for delphix_engine in dx_obj.dlpx_engines:
                dx_engine = dx_obj[delphix_engine]
                # Create a new thread and add it to the list.
                threads.append(main_func(dx_engine))
        except dlpx_exceptions.DlpxException as err:
            dx_logging.print_exception(f'Error encountered in run_job():'
                                       f'\n{err}')
    else:
        # Test to see if the engine exists in config_file
        if engine:
            try:
                dx_engine = dx_obj.dlpx_engines[engine]
                dx_logging.print_info(
                    f'Executing against Delphix DDP: {dx_engine}\n')
            except (exceptions.RequestError, KeyError) as err:
                raise dlpx_exceptions.DlpxException(
                    f'\nERROR: Delphix DDP {engine} cannot be found in '
                    f'{config_file}. Please check your value and try again.')
        else:
            # Else search for a default engine in config_file
            for delphix_engine in dx_obj.dlpx_engines:
                if dx_obj.dlpx_engines[delphix_engine]['default'] == 'true':
                    dx_engine = dx_obj.dlpx_engines[delphix_engine]
                    dx_logging.print_info(f'Executing against the default '
                                          f'Delphix DDP in {config_file}.')
                break
        if engine is None:
            raise dlpx_exceptions.DlpxException(f'ERROR: No default Delphix '
                                                f'DDP found in {config_file}.')
    # run the job against the engine
    threads.append(main_func(dx_engine))
    for each in threads:
        # join them back together so that we wait for all threads to complete
        # before moving on
        each.join()
