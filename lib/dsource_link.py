"""
Create an object to link MS SQL or ASE dSources
"""

from delphixpy.v1_10_2.web import group
from delphixpy.v1_10_2.web import sourceconfig
from delphixpy.v1_10_2.web import vo
from lib import dlpx_exceptions
from lib import get_references

VERSION = "v.0.3.000"


class DsourceLink:
    """
    Base class for linking dSources
    """

    def __init__(
        self, dlpx_obj, dsource_name, db_passwd, db_user, dx_group, logsync, db_type
    ):
        """
        Attributes required for linking MS SQL or ASE dSources
        :param dlpx_obj: A Delphix DDP session object
        :type dlpx_obj: lib.get_session.GetSession
        :param dsource_name: Name of the dsource
        :type dsource_name: str
        :param dx_group: Group name of where the dSource will reside
        :type dx_group: str
        :param db_passwd: Password of the db_user
        :type db_passwd: str
        :param db_user: Username of the dSource
        :type db_user: str
        :param logsync: Enable logsync
        :type logsync: bool
        :param db_type: dSource type. mssql, sybase or oracle
        :type db_type: str
        """
        self.dlpx_obj = dlpx_obj
        self.group = dx_group
        self.db_passwd = db_passwd
        self.db_user = db_user
        self.dsource = dsource_name
        self.logsync = logsync
        self.db_type = db_type
        self.engine_name = self.dlpx_obj.dlpx_ddps["engine_name"]

    def dsource_prepare_link(self):
        """
        Prepare the dsource object for linking
        """
        link_params = vo.LinkParameters()
        link_params.name = self.dsource
        if self.db_type.lower() == "oracle":
            link_params.link_data = vo.OracleLinkData()
        elif self.db_type.lower() == "sybase":
            link_params.link_data = vo.MSSqlLinkData()
        elif self.db_type.lower() == "mssql":
            link_params.link_data = vo.ASELinkData()
        try:
            link_params.link_data.config = get_references.find_obj_by_name(
                self.dlpx_obj.server_session, sourceconfig, self.dsource
            ).reference
            link_params.group = get_references.find_obj_by_name(
                self.dlpx_obj.server_session, group, self.group
            ).reference
        except dlpx_exceptions.DlpxException as err:
            raise dlpx_exceptions.DlpxException(
                f"Could not link {self.dsource}:\n{err}"
            )
        link_params.link_data.db_credentials = vo.PasswordCredential()
        link_params.link_data.db_credentials.password = self.db_passwd
        link_params.link_data.db_user = self.db_user
        link_params.link_data.sourcing_policy = vo.SourcingPolicy()
        if self.logsync:
            link_params.link_data.sourcing_policy.logsync_enabled = True
        elif not self.logsync:
            link_params.link_data.sourcing_policy.logsync_enabled = False
        return link_params
