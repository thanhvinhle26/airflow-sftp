import os
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from airflow.contrib.hooks.ssh_hook import SSHHook
from sftphandler.logger import loggerFactory
from sftphandler.utils import file_utils

logger = loggerFactory(__name__)

class SFTPPutMultipleFilesOperator(BaseOperator):

    template_fields = ('local_directory', 'remote_directory', 'remote_host')

    def __init__(
        self,
        *,
        ssh_hook=None,
        ssh_conn_id=None,
        remote_host=None,
        local_directory=None,
        remote_directory=None,
        confirm=True,
        create_intermediate_dirs=False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.ssh_hook = ssh_hook
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.local_directory = local_directory
        self.remote_directory = remote_directory
        self.confirm = confirm
        self.create_intermediate_dirs = create_intermediate_dirs

    def execute(self, context: Any) -> str:
        file_msg = None
        try:
            if self.ssh_conn_id:
                if self.ssh_hook and isinstance(self.ssh_hook, SSHHook):
                    logger.info("ssh_conn_id is ignored when ssh_hook is provided.")
                else:
                    logger.info(
                        "ssh_hook is not provided or invalid. Trying ssh_conn_id to create SSHHook."
                    )
                    self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)

            if not self.ssh_hook:
                raise AirflowException("Cannot operate without ssh_hook or ssh_conn_id.")

            if self.remote_host is not None:
                logger.info(
                    "remote_host is provided explicitly. "
                    "It will replace the remote_host which was defined "
                    "in ssh_hook or predefined in connection of ssh_conn_id."
                )
                self.ssh_hook.remote_host = self.remote_host

            with self.ssh_hook.get_conn() as ssh_client:
                sftp_client = ssh_client.open_sftp()
                all_files = file_utils.get_only_files_in_folder(self.local_directory)
                logger.info(f"list file in local {all_files}")
                for f in all_files:
                    split_path_file = f.split('_')
                    path_file = '/'.join(split_path_file[:-1])
                    file_name = split_path_file[-1]
                    logger.info(f"{path_file} ")
                    logger.info(f"{file_name} ")
                    path_exist = check_path_exist(sftp_client,f'/{self.remote_directory}/{path_file}')
                    logger.info(f"{path_exist} ")
                    if path_file:
                        if not path_exist:
                            logger.info(f"Starting to create directory /{self.remote_directory}/{path_file} ")
                            sftp_client.mkdir(f'/{self.remote_directory}/{path_file}')
                        logger.info(f"Starting to transfer from /{self.local_directory}/{f} to /{self.remote_directory}/{path_file}/{file_name} ")
                        sftp_client.put(f'/{self.local_directory}/{f}', f'/{self.remote_directory}/{path_file}/{file_name}')
                    else:
                        logger.info(f"Starting to transfer from /{self.local_directory}/{f} to /{self.remote_directory}/{file_name} ")
                        sftp_client.put(f'/{self.local_directory}/{f}', f'/{self.remote_directory}/{file_name}')

                    file_utils.remove_file(f'/{self.local_directory}/{f}')
        except Exception as e:
            raise AirflowException(f"Error while transferring {file_msg}, error: {str(e)}")

        return self.local_directory
def check_path_exist(sftp_client, path):
    is_directory = False

    try:
        sftp_client.listdir(path)
        is_directory = True
    except IOError:
        pass

    return is_directory
