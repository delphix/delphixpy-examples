"""
Create an object to link MS SQL or ASE dSources
"""

from delphixpy.v1_10_2.web import sourceconfig
from delphixpy.v1_10_2.web import group
from delphixpy.v1_10_2.web import vo

from lib import dlpx_exceptions
from lib import get_references

VERSION = 'v.0.3.002'


class DsourceLink:
    """
    Base class for linking dSources
    """
    def __init__(self, dlpx_obj, dsource_name, db_passwd, db_user, dx_group,
                 db_type):
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
        :param db_type: dSource type. mssql, sybase or oracle
        :type db_type: str
        """
        self.dlpx_obj = dlpx_obj
        self.dx_group = dx_group
        self.db_passwd = db_passwd
        self.db_user = db_user
        self.dsource_name = dsource_name
        self.db_type = db_type
        self.engine_name = list(dlpx_obj.dlpx_ddps)[0]
        self.link_params = vo.LinkParameters()
        self.srccfg_obj = None

    def dsource_prepare_link(self):
        """
        Prepare the dsource object for linking
        """
        self.link_params.name = self.dsource_name
        if self.db_type.lower() == 'oracle':
            self.link_params.link_data = vo.OracleLinkData()
        elif self.db_type.lower() == 'sybase':
            self.link_params.link_data = vo.ASELinkData()
        elif self.db_type.lower() == 'mssql':
            self.link_params.link_data = vo.MSSqlLinkData()
        self.link_params.group = get_references.find_obj_by_name(
            self.dlpx_obj.server_session, group, self.dx_group).reference
        self.link_params.link_data.db_credentials = vo.PasswordCredential()
        self.link_params.link_data.db_credentials.password = self.db_passwd
        self.link_params.link_data.db_user = self.db_user
        self.link_params.link_data.sourcing_policy = vo.SourcingPolicy()
        # Enforce logsync. Set this to False if logsync is not needed
        self.link_params.link_data.sourcing_policy.logsync_enabled = True
        self.link_params.link_data.config = self.get_sourceconfig()
        return self.link_params

    def get_sourceconfig(self):
        """
        Get current sourceconfig
        """
        try:
            return get_references.find_obj_by_name(
                self.dlpx_obj.server_session, sourceconfig,
                self.dsource_name).reference
        except dlpx_exceptions.DlpxObjectNotFound:
            raise dlpx_exceptions.DlpxException(
                f'Object {self.dsource_name} not found on {self.engine_name}')


