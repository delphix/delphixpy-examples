"""
Create an object to link Oracle dSources
"""
from delphixpy.v1_10_2 import exceptions
from delphixpy.v1_10_2.web import sourceconfig
from delphixpy.v1_10_2.web import database
from delphixpy.v1_10_2.web import vo
from delphixpy.v1_10_2.web import environment

from lib import dlpx_exceptions
from lib import get_references
from lib.dsource_link import DsourceLink

VERSION = "v.0.3.000"


class DsourceLinkOracle(DsourceLink):
    """
    Base class for linking dSources
    """
    def __init__(self, dlpx_obj, dsource_name, db_passwd, db_user, dx_group,
                 logsync, db_type):
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
        super().__init__(dlpx_obj, dsource_name, db_passwd, db_user, dx_group,
                         db_type)
        self.dlpx_obj = dlpx_obj
        self.group = dx_group
        self.db_passwd = db_passwd
        self.db_user = db_user
        self.dsource_name = dsource_name
        self.logsync = logsync
        self.db_type = db_type
        self.engine_name = self.dlpx_obj.dlpx_ddps['engine_name']

    def get_or_create_ora_sourcecfg(self, env_name, db_install_path, ip_addr,
                                     port_num=1521):
        """
        Create the sourceconfig used for provisioning an Oracle dSource
        :param env_name: Name of the environment in Delphix
        :type env_name: str
        :param db_install_path: Path to where the Oracle binaries are installed
        :type db_install_path: str
        :param ip_addr: IP Address of the Delphix environment. Used for the
        Oracle connection string
        :type ip_addr: str
        :param port_num: Port number of the Oracle Listener (1521 default)
        :type port_num: int
        """
        port_num = str(port_num)
        env_obj = get_references.find_obj_by_name(
            self.dlpx_obj.server_session, environment, env_name)
        repo_ref = get_references.find_db_repo(
            self.dlpx_obj.server_session, 'OracleInstall', env_obj.reference,
            db_install_path)
        sourcecfg_params = vo.OracleSIConfig()
        connect_str = f'jdbc:oracle:thin:@{ip_addr}:{port_num}:' \
                      f'{self.dsource_name}'
        sourcecfg_params.user = self.db_user
        sourcecfg_params.environment_user = env_obj.primary_user
        sourcecfg_params.credentials = vo.PasswordCredential()
        sourcecfg_params.credentials.password = self.db_passwd
        sourcecfg_params.database_name = self.dsource_name
        sourcecfg_params.unique_name = self.dsource_name
        sourcecfg_params.instance = vo.OracleInstance()
        sourcecfg_params.instance.instance_name = self.dsource_name
        sourcecfg_params.instance.instance_number = 1
        sourcecfg_params.services = vo.OracleService()
        sourcecfg_params.repository = repo_ref
        sourcecfg_params.jdbcConnectionString = connect_str
        sourceconfig_ref = self.get_or_create_sourceconfig(sourcecfg_params)
        self.link_ora_dsource(sourceconfig_ref, env_obj.primary_user)

    def link_ora_dsource(self, srccconfig_ref, primary_user_ref,
                         num_connections=5, files_per_set=5, rman_channels=2):
        """
        Link an Oracle dSource
        :param srccconfig_ref: Reference to the sourceconfig object
        :type srccconfig_ref: str
        :param primary_user_ref: Reference to the environment user
        :type primary_user_ref: str
        :param num_connections: Number of connections for Oracle RMAN
        :type num_connections: int
        :param files_per_set: Configures how many files per set for Oracle RMAN
        :type files_per_set: int
        :param rman_channels: Configures the number of Oracle RMAN Channels
        :type rman_channels: int
        :return: Reference of the linked dSource
        """
        link_params = super().dsource_prepare_link()
        link_params.link_data.sourcing_policy = vo.OracleSourcingPolicy()
        link_params.link_data.compressedLinkingEnabled = True
        link_params.link_data.environment_user = primary_user_ref
        link_params.link_data.number_of_connections = int(num_connections)
        link_params.link_data.link_now = True
        link_params.link_data.files_per_set = int(files_per_set)
        link_params.link_data.rman_channels = int(rman_channels)
        try:
            database.link(self.dlpx_obj.server_session, link_params)
            self.dlpx_obj.jobs[self.engine_name] = \
                self.dlpx_obj.server_session.last_job
            self.dlpx_obj.jobs[self.engine_name + 'snap'] = \
                get_references.get_running_job(
                    self.dlpx_obj.server_session,
                    get_references.find_obj_by_name(
                        self.dlpx_obj.server_session, database,
                        self.dsource_name).reference
                )
        except (exceptions.HttpError, exceptions.RequestError,
                exceptions.JobError) as err:
            dlpx_exceptions.DlpxException(
                f'Database link failed for {self.dsource_name}:\n{err}')
