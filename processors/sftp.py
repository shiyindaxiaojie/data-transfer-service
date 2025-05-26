import logging
import os
import paramiko
from processors import DataProcessor

class SftpProcessor(DataProcessor):
    """SFTP 处理器，负责文件上传和下载"""
    
    def __init__(self):
        super().__init__()
        # 创建临时目录
        self.tmp_dir = os.path.join(os.getenv('TARGET_DIR', './target'), 'tmp')
        os.makedirs(self.tmp_dir, exist_ok=True)

    
        # 初始化基础logger
        self.logger = logging.getLogger("Processor:SFTP")
        self.logger_initialized = False
        
        if not self.logger.handlers:
            self.logger.propagate = True
            
            try:
                log_dir = os.path.join(os.getenv('TARGET_DIR', './target'), 'logs')
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                formatter = logging.Formatter(
                    '%(asctime)s.%(msecs)03d [%(levelname)s] [%(name)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                )
                file_handler = logging.FileHandler(os.path.join(log_dir, 'sftp.log'), encoding='utf-8')
                file_handler.setFormatter(formatter)
                file_handler.setLevel(logging.INFO)  # 文件处理器只记录INFO及以上级别
                self.logger.addHandler(file_handler)
            except Exception as e:
                print(f"无法创建 SFTP 日志文件: {str(e)}")
                
            self.logger.setLevel(logging.INFO)  # 设置logger本身的级别为INFO
    
    def process(self, data_source, data_dir):
        """处理数据源生成的数据，上传到 SFTP 服务器"""
        # 第一次处理数据时，使用带数据源信息的logger
        if not self.logger_initialized:
            self.logger = logging.getLogger(f"Processor:SFTP:{data_source.name}")
            self.logger.propagate = True
            self.logger.setLevel(logging.INFO)
            self.logger_initialized = True
            
        # 检查数据目录是否存在
        if not os.path.exists(data_dir):
            self.logger.info(f"数据目录不存在: {data_dir}，跳过SFTP上传")
            return
            
        # 检查数据目录是否为空
        if not os.listdir(data_dir):
            self.logger.info(f"数据目录为空: {data_dir}，跳过SFTP上传")
            return
            
        # 检查是否启用了SFTP上传
        env_var = f'{data_source.name}_ENABLE_SFTP_UPLOAD'
        env_value = os.getenv(env_var, 'true')
        enable_sftp = env_value.lower() == 'true'
        
        if not enable_sftp:
            self.logger.info(f"数据源 {data_source.name} 未启用 SFTP 上传")
            return
        
        # 加载SFTP配置
        try:
            self.logger.info(f"开始加载数据源 {data_source.name} 的 SFTP 配置")
            sftp_config = self._load_sftp_config(data_source.name)
            
            # 检查必要的SFTP配置
            required_configs = ['host', 'port', 'username', 'password', 'remote_base']
            missing_configs = [config for config in required_configs if not sftp_config.get(config)]
            
            if missing_configs:
                self.logger.error(f"SFTP配置缺失: {', '.join(missing_configs)}")
                return
                
            self.logger.info(f"SFTP配置加载成功: 主机={sftp_config['host']}, 端口={sftp_config['port']}, 用户={sftp_config['username']}, 远程目录={sftp_config['remote_base']}")
        except Exception as e:
            self.logger.error(f"加载SFTP配置失败: {str(e)}")
            return
        
        # 上传数据
        try:
            self.logger.info(f"开始上传数据源 {data_source.name} 的数据到SFTP服务器")
            self.logger.info(f"本地数据目录: {data_dir}")
            self.logger.info(f"目录内容: {os.listdir(data_dir)}")
            
            self._upload_directory(data_dir, sftp_config)
            self.logger.info(f"数据源 {data_source.name} 的数据已上传到SFTP服务器")
        except Exception as e:
            self.logger.error(f"上传数据源 {data_source.name} 的数据到SFTP服务器失败: {str(e)}")
            # 不抛出异常，避免影响其他处理器
    
    def _load_sftp_config(self, ds_name):
        """从环境变量加载SFTP配置，使用数据源名称作为前缀"""
        return {
            'host': os.getenv(f'{ds_name}_SFTP_HOST', os.getenv('OUTPUT_SFTP_HOST', '127.0.0.1')),
            'port': int(os.getenv(f'{ds_name}_SFTP_PORT', os.getenv('OUTPUT_SFTP_PORT', '22'))),
            'username': os.getenv(f'{ds_name}_SFTP_USER', os.getenv('OUTPUT_SFTP_USER', '')),
            'password': os.getenv(f'{ds_name}_SFTP_PASSWORD', os.getenv('OUTPUT_SFTP_PASSWORD', '')),
            'remote_base': os.getenv(f'{ds_name}_SFTP_REMOTE_DIR', os.getenv('OUTPUT_SFTP_REMOTE_DIR', '/upload'))
        }
    
    def _mkdir_p(self, sftp, remote_path):
        """递归创建远程目录，确保使用POSIX风格的路径分隔符"""
        # 标准化路径分隔符
        remote_path = remote_path.replace('\\', '/')
        try:
            sftp.chdir(remote_path)
            return
        except IOError:
            # 使用POSIX风格分割路径
            parts = remote_path.rstrip('/').split('/')
            if len(parts) <= 1:
                return
            dirname = '/'.join(parts[:-1])
            basename = parts[-1]
            if dirname:
                self._mkdir_p(sftp, dirname)
            try:
                sftp.mkdir(basename)
            except IOError as e:
                # 忽略目录已存在的错误
                if 'Failure' not in str(e):
                    raise
            sftp.chdir(basename)
    
    def _upload_directory(self, local_dir, sftp_config):
        """上传整个目录到SFTP"""
        transport = None
        sftp = None
        try:
            # 检查本地目录是否存在
            if not os.path.exists(local_dir):
                self.logger.warning(f"本地目录不存在: {local_dir}，跳过上传")
                return
                
            # 检查本地目录是否为空
            if not os.listdir(local_dir):
                self.logger.info(f"本地目录为空: {local_dir}，跳过上传")
                return
            
            # 打印SFTP配置信息（隐藏密码）
            safe_config = sftp_config.copy()
            if 'password' in safe_config:
                safe_config['password'] = '******'
            self.logger.info(f"SFTP配置: {safe_config}")
                
            self.logger.info(f"开始连接SFTP服务器: {sftp_config['host']}:{sftp_config['port']}")
            
            # 创建Transport对象
            try:
                transport = paramiko.Transport((sftp_config['host'], sftp_config['port']))
                self.logger.info("Transport对象创建成功")
            except Exception as e:
                self.logger.error(f"创建Transport对象失败: {str(e)}")
                raise
            
            # 连接认证
            try:
                transport.connect(username=sftp_config['username'], password=sftp_config['password'])
                self.logger.info("SFTP认证成功")
            except paramiko.AuthenticationException:
                self.logger.error(f"SFTP认证失败: 用户名或密码错误 (用户名: {sftp_config['username']})")
                raise
            except Exception as e:
                self.logger.error(f"SFTP连接失败: {str(e)}")
                raise
            
            # 创建SFTP客户端
            try:
                sftp = paramiko.SFTPClient.from_transport(transport)
                self.logger.info("SFTP客户端创建成功")
            except Exception as e:
                self.logger.error(f"创建SFTP客户端失败: {str(e)}")
                raise

            # 构建远程路径，使用POSIX风格的路径分隔符
            remote_dir = f"{sftp_config['remote_base'].rstrip('/')}/{os.path.basename(local_dir)}"
            remote_dir = remote_dir.replace('\\', '/')
            self.logger.info(f"远程目录: {remote_dir}")

            # 创建远程目录
            try:
                self._mkdir_p(sftp, remote_dir)
                self.logger.info(f"已创建远程目录: {remote_dir}")
            except Exception as e:
                self.logger.error(f"创建远程目录失败: {remote_dir}, 错误: {str(e)}")
                raise

            # 遍历本地目录上传
            files_count = 0
            upload_errors = 0
            for root, dirs, files in os.walk(local_dir):
                for filename in files:
                    local_path = os.path.join(root, filename)
                    relative_path = os.path.relpath(local_path, local_dir)
                    remote_path = f"{remote_dir}/{relative_path}"
                    remote_path = remote_path.replace('\\', '/')

                    # 确保远程目录存在
                    try:
                        remote_dirname = os.path.dirname(remote_path)
                        self._mkdir_p(sftp, remote_dirname)
                    except Exception as e:
                        self.logger.error(f"创建远程子目录失败: {remote_dirname}, 错误: {str(e)}")
                        upload_errors += 1
                        continue

                    # 上传文件
                    try:
                        file_size = os.path.getsize(local_path) / 1024  # KB
                        self.logger.info(f"开始上传文件: {local_path} -> {remote_path} (大小: {file_size:.2f} KB)")
                        
                        # 尝试使用UTF-8编码读取文件内容
                        try:
                            with open(local_path, 'r', encoding='utf-8') as local_file:
                                content = local_file.read()
                                # 使用二进制模式写入，确保编码正确
                                with sftp.file(remote_path, 'wb') as remote_file:
                                    remote_file.write(content.encode('utf-8'))
                                self.logger.info(f"文件上传成功: {local_path}")
                                files_count += 1
                        except UnicodeDecodeError:
                            # 如果UTF-8解码失败，使用二进制模式上传
                            self.logger.warning(f"文件 {local_path} 不是UTF-8编码，使用二进制模式上传")
                            sftp.put(local_path, remote_path)
                            self.logger.info(f"文件上传成功（二进制模式）: {local_path}")
                            files_count += 1
                        
                        self.logger.info(f"上传成功: {local_path} -> {remote_path}")
                    except Exception as e:
                        self.logger.error(f"上传文件失败: {local_path} -> {remote_path}, 错误: {str(e)}")
                        upload_errors += 1
                    except Exception as e:
                        self.logger.error(f"上传文件失败: {local_path} -> {remote_path}, 错误: {str(e)}")
                        upload_errors += 1

            if upload_errors > 0:
                self.logger.warning(f"SFTP上传完成，但有 {upload_errors} 个文件上传失败，成功上传 {files_count} 个文件")
            else:
                self.logger.info(f"SFTP上传完成，共上传 {files_count} 个文件")
                
        except Exception as e:
            self.logger.error(f"SFTP上传失败: {str(e)}", exc_info=True)
        finally:
            # 确保连接正确关闭
            try:
                if sftp:
                    sftp.close()
                    self.logger.info("SFTP客户端已关闭")
                if transport:
                    transport.close()
                    self.logger.info("Transport连接已关闭")
            except Exception as e:
                self.logger.warning(f"关闭SFTP连接时发生错误: {str(e)}")
    
    def download_files(self, data_source):
        """从SFTP服务器下载文件到临时目录"""
        # 加载SFTP配置
        try:
            self.logger.info(f"开始加载数据源 {data_source.name} 的 SFTP 配置")
            sftp_config = self._load_sftp_config(data_source.name)
            
            # 检查必要的SFTP配置
            required_configs = ['host', 'port', 'username', 'password', 'remote_base']
            missing_configs = [config for config in required_configs if not sftp_config.get(config)]
            
            if missing_configs:
                self.logger.error(f"SFTP配置缺失: {', '.join(missing_configs)}")
                return
            
            # 创建Transport对象并连接
            transport = None
            sftp = None
            try:
                transport = paramiko.Transport((sftp_config['host'], sftp_config['port']))
                transport.connect(username=sftp_config['username'], password=sftp_config['password'])
                sftp = paramiko.SFTPClient.from_transport(transport)
                
                # 获取远程目录列表
                remote_dir = sftp_config['remote_base'].rstrip('/')
                try:
                    remote_files = sftp.listdir_attr(remote_dir)
                    self.logger.info(f"找到 {len(remote_files)} 个远程文件")
                    
                    # 创建数据源专用的临时目录
                    ds_tmp_dir = os.path.join(self.tmp_dir, data_source.name)
                    if not os.path.exists(ds_tmp_dir):
                        os.makedirs(ds_tmp_dir)
                    
                    # 下载文件
                    for remote_file in remote_files:
                        if not remote_file.filename.startswith('.'):
                            remote_path = f"{remote_dir}/{remote_file.filename}"
                            local_path = os.path.join(ds_tmp_dir, remote_file.filename)
                            
                            try:
                                self.logger.info(f"开始下载文件: {remote_path} -> {local_path}")
                                sftp.get(remote_path, local_path)
                                self.logger.info(f"文件下载成功: {remote_path}")
                            except Exception as e:
                                self.logger.error(f"下载文件失败: {remote_path}, 错误: {str(e)}")
                    
                except IOError as e:
                    self.logger.error(f"无法访问远程目录: {remote_dir}, 错误: {str(e)}")
                    
            finally:
                if sftp:
                    sftp.close()
                if transport:
                    transport.close()
                    
        except Exception as e:
            self.logger.error(f"SFTP下载失败: {str(e)}", exc_info=True)