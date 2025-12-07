# 使用 Astronomer Runtime 镜像 (这是 astro dev start 必须的)
# Runtime 10.6.0 对应 Airflow 2.8 版本
FROM quay.io/astronomer/astro-runtime:10.6.0

# 复制并安装依赖
# (Astro 其实会自动处理 requirements.txt，但保留这段手动命令更保险)
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# NLTK 数据下载 (目前保持注释状态，确保先能跑通)
# RUN python -c "import nltk; nltk.download('vader_lexicon', quiet=True)"