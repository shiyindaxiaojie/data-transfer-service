import gzip
import logging
import os
import sys
import subprocess
import time

import pymysql
import csv
import threading
from queue import Queue
from datetime import datetime
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import RotateEvent
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
    TableMapEvent
)
from dotenv import load_dotenv, dotenv_values
import re
import paramiko
from stat import S_ISDIR

# 加载环境变量
load_dotenv()

def load_data_sources():
    """加载所有数据源配置"""
    env_vars = dotenv_values(".env")
    sources = []
    pattern = re.compile(r'^(DS[A-Z0-9_]+)_MYSQL_HOST$')

    for key in env_vars:
        match = pattern.match(key)
        if match:
            ds_name = match.group(1)
            try:
                mysql_settings = {
                    'host': env_vars.get(f'{ds_name}_MYSQL_HOST'),
                    'port': int(env_vars.get(f'{ds_name}_MYSQL_PORT', '3306')),
                    'user': env_vars.get(f'{ds_name}_MYSQL_USER'),
                    'password': env_vars.get(f'{ds_name}_MYSQL_PASSWORD'),
                    'charset': 'utf8mb4'
                }
                tables_list = [
                    tbl.strip()
                    for tbl in env_vars.get(f'{ds_name}_TABLES_LIST', '').split(',')
                    if tbl.strip()
                ]
                sources.append(DataSourceConfig(
                    name=ds_name,
                    mysql_settings=mysql_settings,
                    tables_list=tables_list
                ))
            except Exception as e:
                logging.error(f"加载数据源 {ds_name} 失败: {str(e)}")
    return sources

class DataSourceConfig:
    def __init__(self, name, mysql_settings, tables_list):
        self.name = name
        self.mysql_settings = mysql_settings
        self.tables_to_watch = [tuple(tbl.strip().split('.', 1)) for tbl in tables_list if '.' in tbl]

        # 生成数据源专用目录结构
        self.base_dir = os.path.abspath(os.getenv('TARGET_DIR', './target'))
        self.output_dir = os.path.join(self.base_dir, 'data', self.name)
        self.checkpoint_dir = os.path.join(self.base_dir, 'checkpoint', self.name)
        self.log_dir = os.path.join(self.base_dir, 'logs', self.name)

        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        os.makedirs(self.log_dir, exist_ok=True)


class BinlogSync:
    def __init__(self, ds_config):
        self.ds_config = ds_config
        self._configure_logging()

        self.server_id = int(os.getenv(f'{ds_config.name}_MYSQL_SERVER_ID', '888888'))
        self.position_file = os.path.join(ds_config.checkpoint_dir, 'binlog_pos.txt')

        self.file_handles = {}
        self.table_map = {}
        self.table_schemas = {}

    def _configure_logging(self):
        """配置日志系统"""
        log_file = os.path.join(self.ds_config.log_dir, 'sync.log')

        self.logger = logging.getLogger(self.ds_config.name)
        self.logger.setLevel(logging.DEBUG)

        if not self.logger.handlers:
            formatter = logging.Formatter(
                f'%(asctime)s.%(msecs)03d [%(levelname)s] [DS:{self.ds_config.name}] [%(funcName)s] %(message)s',
                 datefmt='%Y-%m-%d %H:%M:%S'
            )

            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

            try:
                file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='a')
                file_handler.setFormatter(formatter)
                self.logger.addHandler(file_handler)
            except Exception as e:
                sys.stderr.write(f"无法创建日志文件: {str(e)}\n")

    def _create_mysql_config(self):
        """创建临时配置文件"""
        config_content = f"""
        [client]
        host={self.ds_config.mysql_settings['host']}
        port={self.ds_config.mysql_settings['port']}
        user={self.ds_config.mysql_settings['user']}
        password={self.ds_config.mysql_settings['password']}
        """
        config_path = os.path.join(self.ds_config.base_dir, f'tmp_{self.ds_config.name}.cnf')
        with open(config_path, 'w', encoding='utf-8') as f:
            f.write(config_content.strip())
        return config_path

    def full_export(self, enable_full_export):
        """全量导出"""
        if not enable_full_export:
            self.logger.info("全量导出功能已禁用")
            return

        config_path = None
        try:
            config_path = self._create_mysql_config()
            dump_dir = os.path.join(self.ds_config.output_dir, 'full_export')
            os.makedirs(dump_dir, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            dump_file = os.path.join(dump_dir, f"full_{timestamp}")

            # 按数据库分组导出
            db_tables = {}
            for db, table in self.ds_config.tables_to_watch:
                if db not in db_tables:
                    db_tables[db] = []
                db_tables[db].append(table)

            for db in db_tables:
                output_file = f"{dump_file}_{db}.sql.gz"
                cmd = [
                    'mysqldump',
                    f"--defaults-extra-file={config_path}",
                    "--single-transaction",
                    "--quick",
                    "--extended-insert",
                    "--column-statistics=0",
                    "--databases", db,
                    "--tables", *db_tables[db]
                ]

                with gzip.open(output_file, 'wb') as f:
                    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    for line in process.stdout:
                        f.write(line)
                    stderr = process.stderr.read().decode()
                    if stderr:
                        raise RuntimeError(f"mysqldump 导出错误: {stderr}")

                self.logger.info(f"数据库 {db} 导出完成 -> {os.path.getsize(output_file) / 1024 / 1024:.2f} MB")
        except Exception as e:
            self.logger.error(f"全量导出失败: {str(e)}", exc_info=True)
            raise
        finally:
            if config_path and os.path.exists(config_path):
                try:
                    os.remove(config_path)
                except Exception as e:
                    self.logger.warning(f"临时文件删除失败: {str(e)}")

    def _load_checkpoint(self):
        """加载检查点"""
        try:
            with open(self.position_file, 'r') as f:
                log_file, log_pos = f.read().strip().split(':')
                return log_file, int(log_pos)
        except (FileNotFoundError, ValueError):
            return None, None

    def _save_checkpoint(self, log_file, log_pos):
        """保存检查点"""
        with open(self.position_file, 'w') as f:
            f.write(f"{log_file}:{log_pos}")

    def _get_table_schema(self, db, table):
        """获取表结构"""
        cache_key = (db, table)
        if cache_key in self.table_schemas:
            return self.table_schemas[cache_key]

        try:
            conn = pymysql.connect(**self.ds_config.mysql_settings)
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(f"USE `{db}`")
            cursor.execute(f"SHOW FULL COLUMNS FROM `{table}`")
            columns = [row["Field"] for row in cursor.fetchall()]
            self.table_schemas[cache_key] = columns + ['dml', 'log_pos']
            return self.table_schemas[cache_key]
        except Exception as e:
            self.logger.error(f"获取表结构失败 {db}.{table}: {str(e)}", exc_info=True)
            return None
        finally:
            cursor.close()
            conn.close()

    def _get_csv_writer(self, db, table):
        """获取或创建CSV写入器"""
        key = (db, table)
        if key in self.file_handles:
            return self.file_handles[key]['writer']

        current_date = datetime.now().strftime('%Y-%m-%d')
        date_dir = os.path.join(self.ds_config.output_dir, current_date)
        os.makedirs(date_dir, exist_ok=True)

        filename = os.path.join(date_dir, f"{db}_{table}.csv")
        columns = self._get_table_schema(db, table)
        if not columns:
            return None

        file_obj = open(filename, 'a', newline='', encoding='utf-8')
        writer = csv.DictWriter(file_obj, fieldnames=columns)

        if os.path.getsize(filename) == 0:
            writer.writeheader()
            file_obj.flush()

        self.file_handles[key] = {
            'file': file_obj,
            'writer': writer,
            'date': current_date
        }

        return writer

    def _close_file(self, key):
        if key in self.file_handles:
            try:
                self.file_handles[key]['file'].close()
                del self.file_handles[key]
            except Exception as e:
                self.logger.warning(f"关闭文件失败: {key} -> {str(e)}")

    def _close_all_files(self):
        for key in list(self.file_handles.keys()):
            self._close_file(key)

    def _process_event(self, event):
        if isinstance(event, TableMapEvent):
            self.table_map[event.table_id] = event
            self.logger.debug(f"表映射更新: table_id={event.table_id} {event.schema}.{event.table}")
            return

        if not isinstance(event, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)):
            return

        table_map_event = self.table_map.get(event.table_id)
        if not table_map_event:
            self.logger.error(f"未找到表结构映射 table_id={event.table_id}")
            return

        db = table_map_event.schema
        table = table_map_event.table
        if (db, table) not in self.ds_config.tables_to_watch:
            return

        current_date = datetime.now().strftime('%Y-%m-%d')
        key = (db, table)

        if key in self.file_handles and self.file_handles[key]['date'] != current_date:
            self.logger.info(f"检测到日期变更 {self.file_handles[key]['date']} -> {current_date}, 切换文件")
            self._close_file(key)
            writer = None
        else:
            writer = self._get_csv_writer(db, table)

        if not writer:
            writer = self._get_csv_writer(db, table)
            if not writer:
                return

        operation_type = event.__class__.__name__.replace('RowsEvent', '').lower()

        for row in event.rows:
            if isinstance(event, WriteRowsEvent):
                row_data = row["values"]
            elif isinstance(event, UpdateRowsEvent):
                row_data = row["after_values"]
            elif isinstance(event, DeleteRowsEvent):
                row_data = row["values"] if "values" in row else row

            data = {}
            for idx, column in enumerate(table_map_event.columns):
                if column.name in writer.fieldnames:
                    try:
                        data[column.name] = row_data[column.name] if column.name in row_data else None
                    except (KeyError, TypeError):
                        data[column.name] = None

            data['dml'] = operation_type
            data['log_pos'] = event.packet.log_pos

            try:
                writer.writerow(data)
                self.file_handles[key]['file'].flush()
            except Exception as e:
                self.logger.error(f"CSV写入失败: {db}.{table} -> {str(e)}", exc_info=True)
                continue

            self.logger.info(
                f"变更捕获: db={db} table={table} type={operation_type} "
                f"pos={event.packet.log_pos} data={ {k: v for k, v in data.items() if k not in ['log_pos']} }"
            )

    def start_stream(self, enable_incremental_sync):
        if not enable_incremental_sync:
            self.logger.info("增量同步功能已禁用")
            return

        log_file, log_pos = self._load_checkpoint()
        self.logger.info(f"启动增量同步。起始位置: {log_file or 'latest'}:{log_pos or 'latest'}")

        stream_settings = {
            'connection_settings': self.ds_config.mysql_settings,
            'server_id': self.server_id,
            'resume_stream': True,
            'log_file': log_file,
            'log_pos': log_pos,
            'only_events': [
                DeleteRowsEvent,
                UpdateRowsEvent,
                WriteRowsEvent,
                RotateEvent,
                TableMapEvent
            ],
            'blocking': True,
            'only_schemas': list({db for db, _ in self.ds_config.tables_to_watch}),
            'only_tables': list({tbl for _, tbl in self.ds_config.tables_to_watch}),
            'freeze_schema': True
        }

        stream = None
        try:
            stream = BinLogStreamReader(**stream_settings)
            self.logger.info(f"Binlog流已连接 当前文件: {stream.log_file}")

            for event in stream:
                if isinstance(event, RotateEvent):
                    self._save_checkpoint(event.next_binlog, event.position)
                    self.logger.info(f"检测到binlog切换: {event.next_binlog}")
                else:
                    self._process_event(event)
                    self._save_checkpoint(stream.log_file, event.packet.log_pos)
        except KeyboardInterrupt:
            self.logger.info("用户手动终止同步")
        except Exception as e:
            self.logger.error("增量同步异常", exc_info=True)
            raise
        finally:
            if stream:
                stream.close()
            self._close_all_files()
            self.logger.info(f"增量同步停止 最终位置: {self._load_checkpoint()[0]}:{self._load_checkpoint()[1]}")

class ThreadManager:
    def __init__(self, data_sources):
        self.data_sources = data_sources
        self.threads = []
        self.exception_queue = Queue()

    def _sync_worker(self, ds):
        """单个数据源的同步任务"""
        try:
            syncer = BinlogSync(ds)
            enable_full = os.getenv(f'{ds.name}_ENABLE_FULL_EXPORT', 'false').lower() == 'true'
            enable_incr = os.getenv(f'{ds.name}_ENABLE_INCREMENTAL_SYNC', 'true').lower() == 'true'
            syncer.full_export(enable_full)
            syncer.start_stream(enable_incr)
        except Exception as e:
            self.exception_queue.put(e)
            logging.error(f"数据源 {ds.name} 同步异常: {str(e)}", exc_info=True)

    def start(self):
        """启动所有数据源的并行同步"""
        for ds in self.data_sources:
            thread = threading.Thread(
                target=self._sync_worker,
                args=(ds,),
                name=f"SyncThread-{ds.name}",
                daemon=True
            )
            self.threads.append(thread)
            thread.start()
            logging.info(f"已启动数据源 {ds.name} 的同步线程")

        try:
            while any(t.is_alive() for t in self.threads):
                time.sleep(0.5)
                if not self.exception_queue.empty():
                    raise self.exception_queue.get()
        except KeyboardInterrupt:
            logging.info("用户终止所有同步")
        finally:
            # 等待所有线程优雅退出
            for t in self.threads:
                t.join(timeout=3)

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

if __name__ == '__main__':
    try:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s.%(msecs)03d [%(levelname)s] [main] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('./main.log')
            ]
        )

        data_sources = load_data_sources()

        if not data_sources:
            raise ValueError("未找到有效的数据源配置，请检查配置是否正确。")

        manager = ThreadManager(data_sources)
        manager.start()

    except Exception as e:
        logging.critical(f"全局异常: {str(e)}", exc_info=True)
        sys.exit(1)