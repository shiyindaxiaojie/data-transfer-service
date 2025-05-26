FROM python:3.11-slim

WORKDIR /app

# 设置默认环境变量
ENV TARGET_DIR=/app/target \
    BINLOG_INCR_SYNC_INTERVAL_SECONDS=5 \
    DS1_MYSQL_PORT=3306 \
    DS1_ENABLE_FULL_SYNC=false \
    DS1_ENABLE_INCREMENTAL_SYNC=true \
    DS1_ENABLE_SFTP_UPLOAD=true \
    DS2_MYSQL_PORT=3306 \
    DS2_ENABLE_FULL_SYNC=false \
    DS2_ENABLE_INCREMENTAL_SYNC=true \
    DS2_ENABLE_SFTP_UPLOAD=true \
    ENABLED_PROCESSORS=sftp \
    OUTPUT_SFTP_PORT=22 \
    OUTPUT_SFTP_REMOTE_DIR=/upload

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 创建目标目录
RUN mkdir -p ${TARGET_DIR}

CMD ["python", "main.py"]