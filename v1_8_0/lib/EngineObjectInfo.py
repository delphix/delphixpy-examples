def find_source_obj(dlpx_obj, datasource_ref):
    """
    Find the source object for a Jet Stream datasource.

    :param dlpx_obj: Virtualization Engine session object
    :type dlpx_obj: lib.GetSession.GetSession
    :param datasource_ref:
    :return: source object
    :type delphixpy.web.vo.OracleLinkedSource
    """

    for src_obj in source.get_all(dlpx_obj.server_session):
        if src_obj.container == datasource_ref:
            return src_obj
    raise DlpxException('Could not find {} in engine {}.'.format(
        datasource_ref, dlpx_obj.dlpx_engines.keys()[0]))


def find_latest_vdb_refresh(dlpx_obj, obj_ref):
    """
    Find and return the latest refresh of the VDB

    :param dlpx_obj: Virtualization Engine session object
    :type dlpx_obj: lib.GetSession.GetSession
    :param obj_ref: Reference of the VDB
    :type obj_ref: string
    :return The date/time of the last refresh
    :type: string
    """

    try:
        timeflow_obj = timeflow.get(dlpx_obj.server_session,
                                    database.get(dlpx_obj.server_session,
                                                 obj_ref).current_timeflow)
        return timeflow_obj.name.split('@')[1]
    except (DlpxException, RequestError, HttpError) as e:
        print_exception('ERROR: The timeflow {} could not be found. The '
                        'error was:\n{}'.format(obj_ref, e))


def find_latest_dsource_snap(dlpx_obj, obj_ref):
    """
    Find and return the latest snapshot

    :param dlpx_obj: Virtualization Engine session object
    :type dlpx_obj: lib.GetSession.GetSession
    :param obj_ref: Reference of the dSource
    :type template_name: basestring
    :return latest: The latest snapshot object for the template DB.
    :type: str
    """

    latest = None
    for snap in snapshot.get_all(dlpx_obj.server_session, database=obj_ref):
        if latest < snap.latest_change_point.timestamp:
            latest = snap.latest_change_point.timestamp
    return latest
