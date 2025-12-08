# -*- coding: utf-8 -*-
"""
DAG 1.5: reddit_separate_by_date_dag
-------------------------------------
每天 8:10 运行（在 collect 和 sentiment 之间）：
- 从 GCS commons 目录读取当天收集的 reddit_comments_YYYYMMDD.json
- 将其复制到以执行日期命名的文件（支持 backfill）
- 结果写入 GCS：gs://<bucket>/commons/reddit_comments_<EXECUTION_DATE>.json

这个 DAG 的作用是支持 backfill，使得 sentiment DAG 可以正确读取对应日期的数据。

环境变量：
- GCS_REDDIT_BUCKET   默认 "reddit_sandbox"
- GCS_COMMONS_PREFIX  默认 "commons"
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

from airflow import DAG

# PythonOperator (Airflow 2.x / 3.x)
try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator

from google.cloud import storage


def separate_by_date(**kwargs):
    """
    任务函数：
    - 读取当天收集的 JSON 文件
    - 将其写入以执行日期命名的文件（用于 backfill）
    """
    # 使用 DAG 的执行日期（支持 backfill）
    execution_date = kwargs.get("logical_date") or kwargs.get("execution_date")
    execution_date_str = execution_date.strftime("%Y%m%d")

    # 源文件使用当天日期（collection DAG 写入的文件）
    current_date_str = datetime.utcnow().strftime("%Y%m%d")

    bucket_name = os.environ.get("GCS_REDDIT_BUCKET", "reddit_sandbox")
    commons_prefix = os.environ.get("GCS_COMMONS_PREFIX", "commons").lstrip("/")

    source_object = f"{commons_prefix}/reddit_comments_{current_date_str}.json"
    target_object = f"{commons_prefix}/reddit_comments_{execution_date_str}.json"

    logging.info(
        "Copying from gs://%s/%s to gs://%s/%s",
        bucket_name,
        source_object,
        bucket_name,
        target_object,
    )

    # 读取源文件
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    source_blob = bucket.blob(source_object)

    if not source_blob.exists():
        logging.warning(
            "Source file gs://%s/%s does not exist. Creating empty file for execution date.",
            bucket_name,
            source_object,
        )
        # 创建空文件
        data = []
    else:
        data_str = source_blob.download_as_text()
        data = json.loads(data_str)
        logging.info("Loaded %d records from source file", len(data))

    # 写入目标文件（以执行日期命名）
    target_blob = bucket.blob(target_object)
    target_blob.upload_from_string(
        json.dumps(data), content_type="application/json"
    )

    logging.info(
        "Successfully copied %d records to gs://%s/%s",
        len(data),
        bucket_name,
        target_object,
    )


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="reddit_separate_by_date_dag",
    default_args=default_args,
    start_date=datetime(2024, 10, 1),
    # 运行时间在 collect (8:00) 和 sentiment (8:15) 之间
    schedule="10 8 * * *",
    catchup=False,
    tags=["reddit", "data-processing", "etl"],
) as dag:

    separate_task = PythonOperator(
        task_id="separate_by_date",
        python_callable=separate_by_date,
    )

    separate_task
