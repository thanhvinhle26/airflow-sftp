import os
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from airflow.contrib.hooks.ssh_hook import SSHHook
from sftphandler.logger import loggerFactory

from stat import S_ISDIR, S_ISREG

logger = loggerFactory(__name__)

class SFTPGetMultipleFilesOperator(BaseOperator):

    template_fields = ('local_directory', 'remote_directory', 'remote_host')

    def __init__(
        self,
        *,
        ssh_hook=None,
        ssh_conn_id=None,
        remote_host=None,
        local_directory=None,
        metatdata_directory=None,
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
        self.metatdata_directory = metatdata_directory
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
                result_file= _get_full_file_path(sftp_client, self.remote_directory )
                logger.info(f'Found {len(result_file)} files on server')
                local_folder = os.path.dirname(self.local_directory)
                if self.create_intermediate_dirs:
                    Path(local_folder).mkdir(parents=True, exist_ok=True)

                new_data = []
                
                for i in range(len(result_file)):
                    new_data.append(result_file[i])

                change_data = cdc_change(self.metatdata_directory, new_data)

                for i in range(len(change_data)):
                    new_format_name_file = '_'.join(change_data[i].split('/')[1:])
                    logger.info(f"Starting to transfer from /{change_data[i]} to {self.local_directory}/{new_format_name_file}")
                    sftp_client.get(f'/{change_data[i]}', f'/{self.local_directory}/{new_format_name_file}')
                    
        except Exception as e:
            raise AirflowException(f"Error while transferring {file_msg}, error: {str(e)}")

        return self.local_directory


def _get_full_file_path(sftp_client, remote_directory):
    result_file = []
   
    sftp_get_recursive(remote_directory,sftp_client,result_file)
    logger.info(f'Found {result_file} files on server')
    return result_file


def sftp_get_recursive(path,sftp_client,result_file):
    item_list = sftp_client.listdir_attr(path)
    logger.info(f'Found {item_list} files on server')
    for item in item_list:
        logger.info(f'Found {item.filename} files on server')
        mode = item.st_mode        
        if S_ISDIR(mode):
            sftp_get_recursive(path + "/" + item.filename, sftp_client,result_file)
        else:
            result_file.append(path + "/" + item.filename)
            logger.info(f'Result file: {result_file} s on server')
            
    return

def metadata_file_to_list(path):
    metadata_list = list(line.strip() for line in open(f'/{path}'))
    return metadata_list

def metadata_write_file(data, path):
    with open(f'/{path}', 'w+') as f:
        for line in data:
            f.write(f"{line}\n")

def compare_change(metadata, new_data):
    return list(set(new_data) - set(metadata))

def cdc_change(path, new_data):
    logger.info(f'check_file_exist {check_file_exist(path)} on server')
    if check_file_exist(path):
        meta_data = metadata_file_to_list(path)
        change_data = compare_change(meta_data, new_data)
        new_metadata = meta_data + change_data
        metadata_write_file(new_metadata,path)
        return change_data
    else:
        metadata_write_file(new_data,path)
        return new_data
def check_file_exist(path):
    file_exist = False

    try:
        file = open(f'/{path}')
        file_exist = True
    except IOError:
        pass

    return file_exist