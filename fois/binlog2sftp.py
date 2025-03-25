import configparser
import gzip
import logging
import os
import sys
import subprocess
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

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)-8s] [%(funcName)-20s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('sync.log', encoding='utf-8')
    ]
)


class BinlogSync:
    def __init__(self, config_path):
        self.config = configparser.ConfigParser(interpolation=None)
        if not self.config.read(config_path, encoding='utf-8'):
            raise ValueError(f"配置文件 {config_path} 未找到")

        # 初始化路径
        self.output_dir = os.path.abspath(
            self.config.get('main', 'OutputDir', fallback='./data')
        )
        self.checkpoint_dir = os.path.join(os.path.dirname(__file__), 'checkpoint')
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)

        # MySQL连接配置
        self.mysql_settings = {
            'host': self.config.get('mysql', 'Host'),
            'port': self.config.getint('mysql', 'Port'),
            'user': self.config.get('mysql', 'User'),
            'password': self.config.get('mysql', 'Password'),
            'charset': 'utf8mb4'
        }
        self.server_id = self.config.getint('mysql', 'ServerID', fallback=888888)
        self.tables_to_watch = self._parse_tables()
        self.position_file = os.path.join(self.checkpoint_dir, 'binlog_pos.txt')
        self.enable_full_export = self.config.getboolean('main', 'EnableFullExport', fallback=False)
        self.batch_size = self.config.getint('main', 'BatchSize', fallback=500)
        self.current_batch = {}
        self.table_map = {}  # 表结构缓存字典
        self.table_schemas = {}

    def _parse_tables(self):
        """解析表配置"""
        tables_str = self.config.get('Tables', 'tables_list', fallback='')
        return [tuple(tbl.strip().split('.', 1)) for tbl in tables_str.split(',') if '.' in tbl]

    def _create_mysql_config(self):
        """创建临时配置文件"""
        config_content = f"""
[client]
host={self.mysql_settings['host']}
port={self.mysql_settings['port']}
user={self.mysql_settings['user']}
password={self.mysql_settings['password']}
"""
        config_path = os.path.join(os.path.dirname(__file__), 'tmp_mysql.cnf')
        with open(config_path, 'w', encoding='utf-8') as f:
            f.write(config_content.strip())
        return config_path

    def full_export(self):
        """全量导出"""
        if not self.enable_full_export:
            logging.info("全量导出功能已禁用")
            return

        config_path = None
        try:
            config_path = self._create_mysql_config()
            dump_dir = os.path.join(self.output_dir, 'full_export')
            os.makedirs(dump_dir, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            dump_file = os.path.join(dump_dir, f"full_{timestamp}")

            # 按数据库分组导出
            db_tables = {}
            for db, table in self.tables_to_watch:
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
                    # 检查错误
                    stderr = process.stderr.read().decode()
                    if stderr:
                        raise RuntimeError(f"mysqldump错误: {stderr}")

                logging.info(f"数据库 {db} 导出完成 -> {os.path.getsize(output_file) / 1024 / 1024:.2f} MB")
        except Exception as e:
            logging.error(f"全量导出失败: {str(e)}", exc_info=True)
            raise
        finally:
            if config_path and os.path.exists(config_path):
                try:
                    os.remove(config_path)
                except Exception as e:
                    logging.warning(f"临时文件删除失败: {str(e)}")

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
            with pymysql.connect(**self.mysql_settings) as conn:
                with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                    cursor.execute(f"USE `{db}`")
                    cursor.execute(f"SHOW FULL COLUMNS FROM `{table}`")
                    columns = [row["Field"] for row in cursor.fetchall()]
                    self.table_schemas[cache_key] = columns + ['dml', 'log_pos']
                    return self.table_schemas[cache_key]
        except Exception as e:
            logging.error(f"获取表结构失败 {db}.{table}: {str(e)}")
            return None

    def _process_event(self, event):
        """处理事件（最终修复版）"""
        # 1. 处理表结构映射事件
        if isinstance(event, TableMapEvent):
            self.table_map[event.table_id] = event
            return

        # 2. 处理行变更事件
        if not isinstance(event, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)):
            return

        # 3. 获取表结构映射
        table_map_event = self.table_map.get(event.table_id)
        if not table_map_event:
            logging.error(f"未找到表结构映射 table_id={event.table_id}")
            return

        db = table_map_event.schema
        table = table_map_event.table
        if (db, table) not in self.tables_to_watch:
            return

        # 4. 获取字段名列表（兼容新旧版本）
        column_names = table_map_event.columns  # 类型为列表，例如 ['id', 'name']
        # 或通过解析 column_schemas 获取更详细的列信息（如数据类型）
        column_schemas = table_map_event.column_schemas  # 包含类型、长度等元数据

        columns = self._get_table_schema(db, table)
        if not columns:
            return

        # 5. 处理数据行
        rows = []
        for row in event.rows:
            data = {}
            if isinstance(event, WriteRowsEvent):
                row_data = row["values"]
            elif isinstance(event, UpdateRowsEvent):
                row_data = row["after_values"]
            elif isinstance(event, DeleteRowsEvent):
                row_data = row["values"]
            else:
                continue

            # 按字段顺序映射
            for idx, col_name in enumerate(column_names):
                if col_name in columns:
                    data[col_name] = row_data.get(idx, None)

            data['dml'] = event.__class__.__name__.replace('RowsEvent', '').lower()
            data['log_pos'] = event.packet.log_pos
            rows.append(data)

        key = (db, table)
        if key not in self.current_batch:
            self.current_batch[key] = []
        self.current_batch[key].extend(rows)

        if len(self.current_batch[key]) >= self.batch_size:
            self._write_batch(key)

    def _write_batch(self, key):
        """批量写入CSV"""
        db, table = key
        filename = os.path.join(
            self.output_dir,
            datetime.now().strftime('%Y-%m-%d'),
            f"{db}_{table}.csv"
        )
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        columns = self.table_schemas.get(key, [])

        with open(filename, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            if os.path.getsize(filename) == 0:
                writer.writeheader()
            writer.writerows(self.current_batch[key])

        logging.info(f"写入 {len(self.current_batch[key])} 行到 {os.path.basename(filename)}")
        del self.current_batch[key]

    def start_stream(self):
        """增量同步（最终稳定版）"""
        log_file, log_pos = self._load_checkpoint()
        logging.info(f">> 启动增量同步 起始位置: {log_file or '最新'}:{log_pos or '最新'}")

        stream_settings = {
            'connection_settings': self.mysql_settings,
            'server_id': self.server_id,
            'resume_stream': True,
            'log_file': log_file,
            'log_pos': log_pos,
            'only_events': [
                DeleteRowsEvent,
                UpdateRowsEvent,
                WriteRowsEvent,
                RotateEvent,
                TableMapEvent  # 必须包含此事件类型
            ],
            'blocking': True,
            'only_schemas': list({db for db, _ in self.tables_to_watch}),
            'only_tables': list({tbl for _, tbl in self.tables_to_watch})
        }

        stream = None
        try:
            stream = BinLogStreamReader(**stream_settings)
            self.current_log_file = stream.log_file
            logging.info(f"Binlog流已连接 当前文件: {self.current_log_file}")

            for event in stream:
                if isinstance(event, RotateEvent):
                    self._save_checkpoint(event.next_binlog, event.packet.log_pos)
                    logging.info(f"检测到binlog切换: {event.next_binlog}")
                else:
                    self._process_event(event)
                    self._save_checkpoint(stream.log_file, event.packet.log_pos)
        except KeyboardInterrupt:
            logging.info("用户手动终止同步")
        except Exception as e:
            logging.error("增量同步异常", exc_info=True)
            raise
        finally:
            if stream:
                stream.close()
            for key in list(self.current_batch.keys()):
                self._write_batch(key)
            logging.info(f"<< 增量同步停止 最终位置: {self._load_checkpoint()[0]}:{self._load_checkpoint()[1]}")


if __name__ == '__main__':
    try:
        logging.info("========== 同步任务启动 ==========")
        syncer = BinlogSync('config.ini')
        syncer.full_export()
        syncer.start_stream()
        logging.info("========== 同步正常结束 ==========")
    except Exception as e:
        logging.critical("!!!!!!!! 同步异常终止 !!!!!!!!", exc_info=True)
        sys.exit(1)