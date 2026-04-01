# 使用 Python 3.10 官方镜像作为基础
FROM python:3.10-slim

# 设置工作目录
WORKDIR /app

# 设置环境变量，确保 Python 输出实时显示
ENV PYTHONUNBUFFERED=1

# 安装系统依赖（pandas 需要）
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 复制 requirements.txt 并安装 Python 依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目文件到容器
COPY . .

# 创建日志目录（如果不存在）
RUN mkdir -p /app/logs

# 设置默认命令，使用环境变量启动
CMD ["python", "hero_winrate_scheduler.py"]