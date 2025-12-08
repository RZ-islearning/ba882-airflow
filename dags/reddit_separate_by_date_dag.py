# -*- coding: utf-8 -*-
"""
DAG 1.5: reddit_separate_by_date_dag
-------------------------------------
每天 8:10 运行（在 collect 和 sentiment 之间）：
- 从 GCS commons 目录读取收集的 reddit_comments_YYYYMMDD.json
- 根据 comment_created_utc 字段过滤，只保留在执行日期创建的评论
- 结果写入 GCS：gs://<bucket>/processed/reddit_comments_<EXECUTION_DATE>.json

这个 DAG 的作用是支持 backfill，通过 created_utc 时间戳将数据按日期分离。

环境变量：
- GCS_REDDIT_BUCKET     默认 "reddit_sandbox"
- GCS_COMMONS_PREFIX    默认 "commons"
- GCS_PROCESSED_PREFIX  默认 "processed"
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
    - 读取收集的 JSON 文件
    - 根据 comment_created_utc 过滤出执行日期的评论
    - 将过滤后的数据写入 processed 目录
    """
    # 使用 DAG 的执行日期（支持 backfill）
    execution_date = kwargs.get("logical_date") or kwargs.get("execution_date")
    execution_date_str = execution_date.strftime("%Y%m%d")

    # 计算执行日期的 Unix 时间戳范围（UTC）
    # execution_date 是 datetime，表示一天的开始（00:00:00）
    start_timestamp = int(execution_date.timestamp())
    end_timestamp = int((execution_date + timedelta(days=1)).timestamp())

    logging.info(
        "Filtering comments created on execution_date=%s (UTC timestamp range: %d to %d)",
        execution_date_str,
        start_timestamp,
        end_timestamp,
    )

    # 源文件使用当天日期（collection DAG 写入的文件）
    current_date_str = datetime.utcnow().strftime("%Y%m%d")

    bucket_name = os.environ.get("GCS_REDDIT_BUCKET", "reddit_sandbox")
    commons_prefix = os.environ.get("GCS_COMMONS_PREFIX", "commons").lstrip("/")
    processed_prefix = os.environ.get("GCS_PROCESSED_PREFIX", "processed").lstrip("/")

    source_object = f"{commons_prefix}/reddit_comments_{current_date_str}.json"
    target_object = f"{processed_prefix}/reddit_comments_{execution_date_str}.json"

    logging.info(
        "Reading from gs://%s/%s and filtering by created_utc",
        bucket_name,
        source_object,
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
        filtered_data = []
    else:
        data_str = source_blob.download_as_text()
        all_data = json.loads(data_str)
        logging.info("Loaded %d total records from source file", len(all_data))

        # 过滤：只保留 comment_created_utc 在执行日期范围内的记录
        filtered_data = []
        for record in all_data:
            comment_utc = record.get("comment_created_utc")
            if comment_utc is not None and start_timestamp <= comment_utc < end_timestamp:
                filtered_data.append(record)

        logging.info(
            "Filtered to %d records created on %s (from %d total records)",
            len(filtered_data),
            execution_date_str,
            len(all_data),
        )

    # 写入目标文件（以执行日期命名）
    target_blob = bucket.blob(target_object)
    target_blob.upload_from_string(
        json.dumps(filtered_data), content_type="application/json"
    )

    logging.info(
        "Successfully wrote %d records to gs://%s/%s",
        len(filtered_data),
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
