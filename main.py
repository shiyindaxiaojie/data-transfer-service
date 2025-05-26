import logging
import os
import sys
import threading
import time
from queue import Queue
from dotenv import load_dotenv

from datasource import load_data_sources
from binlog import BinlogSync

# 加载环境变量
load_dotenv()

class ThreadManager:
    def __init__(self, data_sources):
        self.data_sources = data_sources
        self.threads = []
        self.exception_queue = Queue()

    def _sync_worker(self, ds):
        """单个数据源的同步任务"""
        try:
            syncer = BinlogSync(ds)
            enable_full = os.getenv(f'{ds.name}_ENABLE_FULL_SYNC', 'false').lower() == 'true'
            enable_incr = os.getenv(f'{ds.name}_ENABLE_INCREMENTAL_SYNC', 'true').lower() == 'true'
            syncer.full_sync(enable_full)
            syncer.incr_sync(enable_incr)
        except Exception as e:
            self.exception_queue.put(e)
            logger = logging.getLogger('main')
            logger.error(f"数据源 {ds.name} 同步异常: {str(e)}", exc_info=True)

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
            logger = logging.getLogger('main')
            logger.info(f"已启动数据源 {ds.name} 的同步线程")

        try:
            while any(t.is_alive() for t in self.threads):
                time.sleep(0.5)
                if not self.exception_queue.empty():
                    raise self.exception_queue.get()
        except KeyboardInterrupt:
            logger = logging.getLogger('main')
            logger.info("用户终止所有同步")
        finally:
            for t in self.threads:
                t.join(timeout=3)

if __name__ == '__main__':
    try:
        # 配置根日志记录器
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)  # 设置为INFO级别
        
        # 清除现有的处理器
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
            
        # 创建格式化器
        formatter = logging.Formatter(
            '%(asctime)s.%(msecs)03d [%(levelname)s] [%(name)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # 添加控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)  # 控制台显示INFO及以上级别的日志
        root_logger.addHandler(console_handler)
        
        # 创建日志目录
        log_dir = os.path.join(os.getcwd(), 'target', 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        # 添加文件处理器
        file_handler = logging.FileHandler(os.path.join(log_dir, 'main.log'), encoding='utf-8')
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)  # 文件记录INFO及以上级别的日志
        root_logger.addHandler(file_handler)
        
        # 配置main日志记录器
        main_logger = logging.getLogger('main')
        main_logger.setLevel(logging.INFO)
        main_logger.info('日志系统初始化完成')
        main_logger.info(f'日志目录: {log_dir}')

        data_sources = load_data_sources()

        if not data_sources:
            raise ValueError("未找到有效的数据源配置，请检查配置是否正确。")

        manager = ThreadManager(data_sources)
        manager.start()

    except Exception as e:
        logger = logging.getLogger('main')
        logger.critical(f"全局异常: {str(e)}", exc_info=True)
        sys.exit(1)