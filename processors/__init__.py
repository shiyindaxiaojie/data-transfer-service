import importlib
import logging
import os
from abc import ABC, abstractmethod

class DataProcessor(ABC):
    """数据处理器接口，所有处理器必须实现这个接口"""
    
    @abstractmethod
    def process(self, data_source, data_dir):
        """处理数据源生成的数据
        
        Args:
            data_source: 数据源配置对象
            data_dir: 数据目录路径
        """
        pass

class ProcessorManager:
    """处理器管理器，负责加载和执行所有启用的处理器"""
    
    def __init__(self):
        self.processors = []
        self.logger = logging.getLogger("ProcessorManager")
        
        if self.logger.handlers:
            for handler in self.logger.handlers[:]:
                self.logger.removeHandler(handler)
        
        self.logger.propagate = True
        
        self.logger.setLevel(logging.DEBUG)
    
    def load_processors(self):
        """从配置加载所有启用的处理器"""
        enabled_processors = os.getenv('ENABLED_PROCESSORS', '').split(',')
        enabled_processors = [p.strip() for p in enabled_processors if p.strip()]
        
        if not enabled_processors:
            self.logger.info("未配置任何数据处理器")
            return
        
        for processor_name in enabled_processors:
            try:
                module_path = f"processors.{processor_name}"
                module = importlib.import_module(module_path)
                
                processor_class = getattr(module, f"{processor_name.capitalize()}Processor")
                processor = processor_class()
                
                self.processors.append(processor)
            except (ImportError, AttributeError) as e:
                self.logger.error(f"加载处理器 {processor_name} 失败: {str(e)}")
    
    def process_data(self, data_source, data_dir):
        """使用所有处理器处理数据"""
        ds_tag = f"[DS:{data_source.name}]"
        self.logger.debug(f"{ds_tag} 尝试处理数据: {data_dir}")
        
        if not os.path.exists(data_dir):
            self.logger.warning(f"{ds_tag} 数据目录不存在: {data_dir}，跳过处理")
            return
            
        if not os.listdir(data_dir):
            self.logger.info(f"{ds_tag} 数据目录为空: {data_dir}，跳过处理")
            return
            
        self.logger.info(f"{ds_tag} 开始处理数据: {data_dir}")
        for processor in self.processors:
            try:
                processor.process(data_source, data_dir)
            except Exception as e:
                self.logger.error(f"{ds_tag} 处理器 {processor.__class__.__name__} 处理失败: {str(e)}", exc_info=True)