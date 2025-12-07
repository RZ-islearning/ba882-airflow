# 你的云端环境要求 Runtime 3.1-4 或更高 (对应 Airflow 3.1)
# 注意：Runtime 3.x 使用了新的版本号命名规则
FROM quay.io/astronomer/astro-runtime:3.1-4

# 复制并安装依赖
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# NLTK 数据下载 (目前保持注释状态，确保先能跑通)
 RUN python -c "import nltk; nltk.download('vader_lexicon', quiet=True)"