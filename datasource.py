import logging
import os
import re
from dotenv import dotenv_values

class DataSourceConfig:
    def __init__(self, name, mysql_settings, tables_list):
        self.name = name
        self.mysql_settings = mysql_settings
        self.tables_to_watch = [tuple(tbl.strip().split('.', 1)) for tbl in tables_list if '.' in tbl]

        self.base_dir = os.path.abspath(os.getenv('TARGET_DIR', './target'))
        self.output_dir = os.path.join(self.base_dir, 'data', self.name)
        self.checkpoint_dir = os.path.join(self.base_dir, 'checkpoint', self.name)
        self.log_dir = os.path.join(self.base_dir, 'logs', self.name)

        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        os.makedirs(self.log_dir, exist_ok=True)


def load_data_sources():
    """加载所有数据源配置"""
    logger = logging.getLogger('datasource')
    logger.info('开始加载数据源配置')
    
    env_vars = dotenv_values(".env")
    if not env_vars:
        logger.error('无法加载.env文件或文件为空')
        return []
        
    sources = []
    pattern = re.compile(r'^(DS[A-Z0-9_]+)_MYSQL_HOST$')

    for key in env_vars:
        match = pattern.match(key)
        if match:
            ds_name = match.group(1)
            logger.info(f'发现数据源配置: {ds_name}')
            try:
                mysql_settings = {
                    'host': env_vars.get(f'{ds_name}_MYSQL_HOST'),
                    'port': int(env_vars.get(f'{ds_name}_MYSQL_PORT', '3306')),
                    'user': env_vars.get(f'{ds_name}_MYSQL_USER'),
                    'password': env_vars.get(f'{ds_name}_MYSQL_PASSWORD'),
                    'charset': 'utf8mb4'
                }
                
                # 验证必要的MySQL配置
                missing_configs = [k for k, v in mysql_settings.items() if not v and k != 'charset']
                if missing_configs:
                    logger.error(f'数据源 {ds_name} 缺少必要的MySQL配置: {", ".join(missing_configs)}')
                    continue
                    
                tables_list = [
                    tbl.strip()
                    for tbl in env_vars.get(f'{ds_name}_TABLES_LIST', '').split(',')
                    if tbl.strip()
                ]
                
                if not tables_list:
                    logger.warning(f'数据源 {ds_name} 未配置监控表列表')
                    continue
                    
                logger.info(f'数据源 {ds_name} 配置加载成功，监控表数量: {len(tables_list)}')
                sources.append(DataSourceConfig(
                    name=ds_name,
                    mysql_settings=mysql_settings,
                    tables_list=tables_list
                ))
            except Exception as e:
                logger.error(f'加载数据源 {ds_name} 失败: {str(e)}', exc_info=True)
                
    logger.info(f'数据源配置加载完成，共加载 {len(sources)} 个数据源')
    return sources