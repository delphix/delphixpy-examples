#!/usr/bin/env python3
"""
Link a MSSQL dSource
"""
from delphixpy.v1_10_2 import exceptions
from delphixpy.v1_10_2.web import vo
from delphixpy.v1_10_2.web import environment
from delphixpy.v1_10_2.web import database

from lib.dsource_link import DsourceLink
from lib import dlpx_exceptions
from lib import get_references

VERSION = "v.0.3.001"


class DsourceLinkMssql(DsourceLink):
    """
    Derived class implementing linking of a MSSQL dSource
    """
    def __init__(self, dlpx_obj, dsource_name, db_passwd, db_user, dx_group,
                 db_type):
        """
        Constructor method
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
        super().__init__(dlpx_obj, dsource_name, db_passwd, db_user, dx_group,
                         db_type)
        self.dlpx_obj = dlpx_obj
        self.dsource_name = dsource_name
        self.db_passwd = db_passwd
        self.db_user = db_user
        self.dx_group = dx_group
        self.db_type = db_type

    def link_mssql_dsource(self, stage_env, stage_instance, backup_path,
                           backup_loc_passwd, backup_loc_user):
        """
        Link an MSSQL dSource
        :param stage_env: Name of the staging environment
        :type stage_env: str
        :param stage_instance: Name if the staging database instance
        :type stage_instance: str
        :param backup_path: Directory of where the backup is located
        :type backup_path: str
        :param backup_loc_passwd: Password of the shared backup path
        :type backup_loc_passwd: str
        :param backup_loc_user: Username for the shared backup path
        :type backup_loc_user: str
        """
        link_params = super().dsource_prepare_link()
        try:
            env_obj_ref = get_references.find_obj_by_name(
                self.dlpx_obj.server_session, environment,
                stage_env).reference
            link_params.link_data.ppt_repository = \
                get_references.find_db_repo(
                    self.dlpx_obj.server_session, 'MSSqlInstance',
                    env_obj_ref, stage_instance).reference
        except dlpx_exceptions.DlpxException as err:
            raise dlpx_exceptions.DlpxException(
                f'Could not link {self.dsource_name}:\n{err}')
        if backup_path != "auto":
            link_params.link_data.shared_backup_location = backup_path
        if backup_loc_passwd:
            link_params.link_data.backup_location_credentials = \
                vo.PasswordCredential()
            link_params.link_data.backup_location_credentials.password = \
                backup_loc_passwd
            link_params.link_data.backup_location_user = backup_loc_user
            link_params.link_data.sourcing_policy = vo.SourcingPolicy()
            try:
                database.link(self.dlpx_obj.server_session, link_params)
            except (exceptions.HttpError, exceptions.RequestError,
                    exceptions.JobError) as err:
                dlpx_exceptions.DlpxException(
                    f'Database link failed for {self.dsource_name}:\n{err}')
            self.dlpx_obj.jobs[self.engine_name] = \
                self.dlpx_obj.server_session.last_job

    def get_or_create_mssql_sourcecfg(self, env_name, db_install_path):
        """
        Create the sourceconfig used for provisioning an MSSQL dSource
        :param env_name: Name of the environment in Delphix
        :type env_name: str
        :param db_install_path: Path to where the Oracle binaries are installed
        :type db_install_path: str
        """
        env_obj = get_references.find_obj_by_name(
            self.dlpx_obj.server_session, environment, env_name)
        repo_ref = get_references.find_db_repo(
            self.dlpx_obj.server_session, 'MSSqlInstance', env_obj.reference,
            db_install_path)
        sourcecfg_params = vo.MSSqlSIConfig()
        sourcecfg_params.user = self.db_user
        sourcecfg_params.credentials = vo.PasswordCredential()
        sourcecfg_params.credentials.password = self.db_passwd
        sourcecfg_params.database_name = self.dsource_name
        sourcecfg_params.unique_name = self.dsource_name
        sourcecfg_params.repository = repo_ref
        sourcecfg_params.environment_user = env_obj.primary_user
        sourceconfig_ref = self.get_or_create_sourceconfig(sourcecfg_params)