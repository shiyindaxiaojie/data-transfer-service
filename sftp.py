import logging
import os
import paramiko

from datasource import DataSourceConfig

class SFTPUploader:
    def __init__(self, ds_config):
        self.ds_config = ds_config
        self._load_sftp_config()
        self.logger = logging.getLogger(f"SFTP.{ds_config.name}")

    def _load_sftp_config(self):
        """从环境变量加载SFTP配置"""
        self.sftp_config = {
            'host': os.getenv(f'OUTPUT_SFTP_HOST', '127.0.0.1'),
            'port': int(os.getenv(f'OUTPUT_SFTP_PORT', '22')),
            'username': os.getenv(f'OUTPUT_SFTP_USER', ''),
            'password': os.getenv(f'OUTPUT_SFTP_PASSWORD', ''),
            'remote_base': os.getenv(f'OUTPUT_SFTP_REMOTE_DIR', '/data')
        }

    def _mkdir_p(self, sftp, remote_path):
        """递归创建远程目录"""
        try:
            sftp.chdir(remote_path)
            return
        except IOError:
            dirname, basename = os.path.split(remote_path.rstrip('/'))
            self._mkdir_p(sftp, dirname)
            sftp.mkdir(basename)
            sftp.chdir(basename)

    def upload_directory(self, local_dir):
        """上传整个目录到SFTP"""
        try:
            transport = paramiko.Transport((self.sftp_config['host'], self.sftp_config['port']))
            transport.connect(username=self.sftp_config['username'], password=self.sftp_config['password'])
            sftp = paramiko.SFTPClient.from_transport(transport)

            # 构建远程路径
            remote_dir = os.path.join(
                self.sftp_config['remote_base'],
                os.path.basename(local_dir)
            )

            # 创建远程目录
            self._mkdir_p(sftp, remote_dir)

            # 遍历本地目录上传
            for root, dirs, files in os.walk(local_dir):
                for filename in files:
                    local_path = os.path.join(root, filename)
                    relative_path = os.path.relpath(local_path, local_dir)
                    remote_path = os.path.join(remote_dir, relative_path)

                    # 确保远程目录存在
                    remote_dirname = os.path.dirname(remote_path)
                    self._mkdir_p(sftp, remote_dirname)

                    sftp.put(local_path, remote_path)
                    self.logger.info(f"上传成功: {local_path} -> {remote_path}")

            sftp.close()
            transport.close()
        except Exception as e:
            self.logger.error(f"SFTP上传失败: {str(e)}", exc_info=True)