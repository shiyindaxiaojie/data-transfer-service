import gzip
import logging
import os
import sys
import subprocess
import time

import pymysql
import csv
from datetime import datetime
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import RotateEvent
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
    TableMapEvent
)

from datasource import DataSourceConfig
from processors import ProcessorManager

class BinlogSync:
    def __init__(self, ds_config):
        self.ds_config = ds_config
        self._configure_logging()

        self.server_id = int(os.getenv(f'{ds_config.name}_MYSQL_SERVER_ID', '888888'))
        self.position_file = os.path.join(ds_config.checkpoint_dir, 'binlog_pos.txt')

        self.file_handles = {}
        self.table_map = {}
        self.table_schemas = {}
        
        # 初始化处理器管理器
        self.processor_manager = ProcessorManager()
        self.processor_manager.load_processors()
        
        # 确保数据目录存在
        current_date = datetime.now().strftime('%Y-%m-%d')
        date_dir = os.path.join(self.ds_config.output_dir, current_date)
        os.makedirs(date_dir, exist_ok=True)
        self.logger.info(f"已创建数据目录: {date_dir}")
        
        # 验证binlog配置和权限
        self._verify_binlog_settings()

    def _verify_binlog_settings(self):
        """验证binlog配置和权限"""
        try:
            conn = pymysql.connect(**self.ds_config.mysql_settings)
            cursor = conn.cursor()
            
            # 检查binlog是否开启
            cursor.execute("SHOW VARIABLES LIKE 'log_bin'")
            log_bin = cursor.fetchone()
            if not log_bin or log_bin[1].lower() != 'on':
                raise RuntimeError("MySQL binlog未开启，请检查MySQL配置")
            
            # 检查binlog格式
            cursor.execute("SHOW VARIABLES LIKE 'binlog_format'")
            binlog_format = cursor.fetchone()
            if not binlog_format or binlog_format[1].upper() != 'ROW':
                raise RuntimeError(f"Binlog格式必须为ROW，当前为: {binlog_format[1] if binlog_format else 'UNKNOWN'}")
            
            # 检查用户权限
            cursor.execute("SHOW GRANTS FOR CURRENT_USER")
            grants = [grant[0].upper() for grant in cursor.fetchall()]
            replication_found = any('REPLICATION' in grant for grant in grants)
            if not replication_found:
                raise RuntimeError("当前用户缺少REPLICATION权限，请授予必要权限")
            
            # 获取当前binlog文件和位置
            cursor.execute("SHOW MASTER STATUS")
            master_status = cursor.fetchone()
            if not master_status:
                raise RuntimeError("无法获取binlog状态，请检查MySQL配置")
                
            self.logger.info(f"Binlog配置验证成功: 格式={binlog_format[1]}, 当前文件={master_status[0]}")
            
        except Exception as e:
            self.logger.error(f"Binlog配置验证失败: {str(e)}")
            raise
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
    
    def _configure_logging(self):
        """配置日志系统"""
        # 使用带数据源前缀的logger名称
        logger_name = f"DS:{self.ds_config.name}"
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)
        
        # 确保日志传播到根日志记录器
        self.logger.propagate = True
        
        # 添加数据源特定的文件处理器
        try:
            log_file = os.path.join(self.ds_config.log_dir, 'sync.log')
            os.makedirs(self.ds_config.log_dir, exist_ok=True)
            
            # 清除已有的处理器，避免重复
            if self.logger.handlers:
                for handler in self.logger.handlers[:]:
                    self.logger.removeHandler(handler)
            
            formatter = logging.Formatter(
                '%(asctime)s.%(msecs)03d [%(levelname)s] [%(name)s] [%(funcName)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setFormatter(formatter)
            file_handler.setLevel(logging.DEBUG)
            self.logger.addHandler(file_handler)
            
            self.logger.info(f'日志系统初始化完成，日志文件: {log_file}')
        except Exception as e:
            sys.stderr.write(f"无法创建日志文件 {log_file}: {str(e)}\n")
            raise

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

    def full_sync(self, enable_full_export):
        """全量同步"""
        if not enable_full_export:
            self.logger.info(f"{self.ds_config.name} 全量同步功能已禁用")
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
            
            self.processor_manager.process_data(self.ds_config, dump_dir)
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
        """加载检查点，如果检查点无效则获取当前binlog位置"""
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(self.position_file), exist_ok=True)
            
            # 尝试从检查点文件加载
            if os.path.exists(self.position_file):
                with open(self.position_file, 'r') as f:
                    content = f.read().strip()
                    if content and ':' in content:
                        log_file, log_pos = content.split(':')
                        self.logger.info(f"成功加载检查点: {log_file}:{log_pos}")
                        return log_file, int(log_pos)
                    else:
                        self.logger.warning(f"检查点文件格式不正确: {content}")
            else:
                self.logger.warning(f"检查点文件不存在: {self.position_file}")
            
            # 如果检查点无效，获取当前binlog位置
            try:
                conn = pymysql.connect(**self.ds_config.mysql_settings)
                cursor = conn.cursor()
                cursor.execute("SHOW MASTER STATUS")
                result = cursor.fetchone()
                if result:
                    current_log_file, current_log_pos = result[0], int(result[1])
                    self.logger.info(f"使用当前binlog位置: {current_log_file}:{current_log_pos}")
                    return current_log_file, current_log_pos
                else:
                    self.logger.error("无法获取当前binlog位置")
            except Exception as e:
                self.logger.error(f"获取当前binlog位置失败: {str(e)}")
            finally:
                if 'cursor' in locals():
                    cursor.close()
                if 'conn' in locals():
                    conn.close()
            
            return None, None
        except (FileNotFoundError, ValueError, IOError) as e:
            self.logger.error(f"加载检查点失败: {str(e)}")
            return None, None

    def _save_checkpoint(self, log_file, log_pos):
        """保存检查点"""
        try:
            os.makedirs(os.path.dirname(self.position_file), exist_ok=True)
            
            with open(self.position_file, 'w') as f:
                f.write(f"{log_file}:{log_pos}")
                f.flush()
                os.fsync(f.fileno())  # 确保数据写入磁盘
            self.logger.debug(f"保存检查点成功: {log_file}:{log_pos}")
        except (IOError, OSError) as e:
            self.logger.error(f"保存检查点失败: {str(e)}")
            # 不抛出异常，避免中断同步进程

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

    def incr_sync(self, enable_incremental_sync):
        if not enable_incremental_sync:
            self.logger.info(f"{self.ds_config.name} 增量同步功能已禁用")
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
            'freeze_schema': True,
            'slave_heartbeat': 0.5  # 设置从服务器心跳间隔为0.5秒
        }

        stream = None
        try:
            stream = BinLogStreamReader(**stream_settings)
            self.logger.info(f"连接 binlog，当前文件: {stream.log_file}")

            for event in stream:
                if isinstance(event, RotateEvent):
                    self._save_checkpoint(event.next_binlog, event.position)
                    self.logger.info(f"检测到 binlog 切换: {event.next_binlog}")
                else:
                    self._process_event(event)
                    self._save_checkpoint(stream.log_file, event.packet.log_pos)
                    
                    # 每次有数据变更时立即处理
                    current_date = datetime.now().strftime('%Y-%m-%d')
                    date_dir = os.path.join(self.ds_config.output_dir, current_date)
                    # 先关闭所有文件，确保数据已写入
                    self._close_all_files()
                    if os.path.exists(date_dir) and os.listdir(date_dir):
                        self.processor_manager.process_data(self.ds_config, date_dir)
                        self.logger.info(f"已调用处理器处理数据: {date_dir}")
                    else:
                        self.logger.info(f"数据目录不存在或为空，跳过处理: {date_dir}")
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