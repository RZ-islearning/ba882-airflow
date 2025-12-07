# ⚠️ 关键修改：Airflow 3 (Runtime 3.x) 的镜像托管在新的 Azure 仓库中
FROM astrocrpublic.azurecr.io/runtime:3.1-4

# 复制并安装依赖
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# NLTK 数据下载 (目前保持注释状态，确保先能跑通)
 RUN python -c "import nltk; nltk.download('vader_lexicon', quiet=True)"