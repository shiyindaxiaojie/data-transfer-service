# 数据传输服务

这是一个基于 Python 的数据传输服务，用于监控 MySQL binlog 并将数据变更通过 SFTP 上传到指定服务器。

## 功能特点

- 支持多数据源配置
- 支持增量同步（通过 binlog）
- 支持全量同步（可选）
- 支持 SFTP 文件上传
- 所有配置通过环境变量管理

## Docker 部署

### 使用 docker-compose（推荐）

1. 修改 `docker-compose.yml` 中的环境变量配置
2. 运行服务：
   ```bash
   docker-compose up -d
   ```
3. 查看日志：
   ```bash
   docker-compose logs -f
   ```

### 使用 Docker 命令

1. 构建镜像：
   ```bash
   docker build -t data-transfer-service .
   ```

2. 运行容器：
   ```bash
   docker run -d \
     --name data-transfer \
     -v $(pwd)/target:/app/target \
     -e DS1_MYSQL_HOST=your_host \
     -e DS1_MYSQL_PORT=3306 \
     -e DS1_MYSQL_USER=your_user \
     -e DS1_MYSQL_PASSWORD=your_password \
     -e DS1_MYSQL_SERVER_ID=your_server_id \
     -e DS1_TABLES_LIST=db.table1,db.table2 \
     -e OUTPUT_SFTP_HOST=sftp_host \
     -e OUTPUT_SFTP_PORT=22 \
     -e OUTPUT_SFTP_USER=sftp_user \
     -e OUTPUT_SFTP_PASSWORD=sftp_password \
     -e OUTPUT_SFTP_REMOTE_DIR=/upload \
     data-transfer-service
   ```

## 环境变量配置

### 全局配置
- `TARGET_DIR`: 数据和日志目录（默认：/app/target）
- `BINLOG_INCR_SYNC_INTERVAL_SECONDS`: binlog 同步间隔（默认：5）
- `ENABLED_PROCESSORS`: 启用的处理器列表（默认：sftp）

### 数据源配置（以 DS1 为例）
- `DS1_MYSQL_HOST`: MySQL 主机地址
- `DS1_MYSQL_PORT`: MySQL 端口
- `DS1_MYSQL_USER`: MySQL 用户名
- `DS1_MYSQL_PASSWORD`: MySQL 密码
- `DS1_MYSQL_SERVER_ID`: MySQL server ID
- `DS1_TABLES_LIST`: 监控的表列表，格式：db.table1,db.table2
- `DS1_ENABLE_FULL_SYNC`: 是否启用全量同步
- `DS1_ENABLE_INCREMENTAL_SYNC`: 是否启用增量同步
- `DS1_ENABLE_SFTP_UPLOAD`: 是否启用 SFTP 上传

### SFTP 配置
- `OUTPUT_SFTP_HOST`: SFTP 服务器地址
- `OUTPUT_SFTP_PORT`: SFTP 端口
- `OUTPUT_SFTP_USER`: SFTP 用户名
- `OUTPUT_SFTP_PASSWORD`: SFTP 密码
- `OUTPUT_SFTP_REMOTE_DIR`: SFTP 远程目录

## 数据目录结构

```
target/
├── data/           # 数据文件
│   ├── DS1/        # 各数据源的数据
│   └── DS2/
├── logs/           # 日志文件
└── checkpoint/     # binlog 检查点
```