# -*- coding: utf-8 -*-
"""
DAG 2: reddit_sentiment_from_commons_dag
----------------------------------------
每天 8:15 运行：
- 从 GCS processed 目录读取 separation DAG 存的 reddit_comments_YYYYMMDD.json
- 对 comment_body 做情感分析
- 结果写入 GCS：gs://<bucket>/database/reddit_sentiment_YYYYMMDD.csv

环境变量：
- GCS_REDDIT_BUCKET     默认 "reddit_sandbox"
- GCS_PROCESSED_PREFIX  默认 "processed"
- GCS_DATABASE_PREFIX   默认 "database"
"""

import os
import csv
import io
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

from airflow import DAG

# PythonOperator (Airflow 2.x)
from airflow.operators.python import PythonOperator

from google.cloud import storage


def _download_json_from_gcs(bucket_name: str, object_name: str) -> List[Dict[str, Any]]:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    if not blob.exists():
        raise FileNotFoundError(f"Object gs://{bucket_name}/{object_name} not found")
    data = blob.download_as_text()
    return json.loads(data)


def _upload_csv_to_gcs(
    rows: List[Dict[str, Any]], bucket_name: str, object_name: str
) -> None:
    if not rows:
        logging.warning("No rows to upload; skip CSV upload.")
        return

    fieldnames = sorted(rows[0].keys())
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    csv_data = buf.getvalue()
    buf.close()

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_string(csv_data, content_type="text/csv")
    logging.info("Uploaded %d rows to gs://%s/%s", len(rows), bucket_name, object_name)


def sentiment_from_commons(**kwargs):
    # Import NLTK here to avoid parse-time overhead
    import nltk
    from nltk.sentiment import SentimentIntensityAnalyzer

    # 使用 DAG 的执行日期（支持 backfill）
    execution_date = kwargs.get("logical_date") or kwargs.get("execution_date")
    today_str = execution_date.strftime("%Y%m%d")

    bucket_name = os.environ.get("GCS_REDDIT_BUCKET", "reddit_sandbox")
    processed_prefix = os.environ.get("GCS_PROCESSED_PREFIX", "processed").lstrip("/")
    db_prefix = os.environ.get("GCS_DATABASE_PREFIX", "database").lstrip("/")

    processed_object = f"{processed_prefix}/reddit_comments_{today_str}.json"
    target_object = f"{db_prefix}/reddit_sentiment_{today_str}.csv"

    logging.info("Reading processed data from gs://%s/%s", bucket_name, processed_object)

    rows = _download_json_from_gcs(bucket_name, processed_object)
    logging.info("Loaded %d comments from commons", len(rows))

    # 准备 VADER (data is already downloaded in Docker image)
    try:
        nltk.data.find("sentiment/vader_lexicon.zip")
    except LookupError:
        nltk.download("vader_lexicon")
    analyzer = SentimentIntensityAnalyzer()

    enriched_rows: List[Dict[str, Any]] = []
    for r in rows:
        body = r.get("comment_body") or ""
        scores = analyzer.polarity_scores(body) if body else {
            "compound": None, "pos": None, "neg": None, "neu": None
        }
        new_r = dict(r)
        new_r.update(
            {
                "sentiment_compound": scores.get("compound"),
                "sentiment_pos": scores.get("pos"),
                "sentiment_neg": scores.get("neg"),
                "sentiment_neu": scores.get("neu"),
            }
        )
        enriched_rows.append(new_r)

    logging.info("Finished sentiment analysis for %d comments", len(enriched_rows))

    logging.info(
        "Writing sentiment CSV to gs://%s/%s", bucket_name, target_object
    )
    _upload_csv_to_gcs(enriched_rows, bucket_name, target_object)


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="reddit_sentiment_from_commons_dag",
    default_args=default_args,
    start_date=datetime(2024, 10, 1),
    schedule="15 8 * * *",  # 每天 8:15 跑，确保前一个 DAG 已完成
    catchup=False,
    tags=["reddit", "sentiment-analysis", "etl"],
) as dag:

    sentiment_task = PythonOperator(
        task_id="sentiment_from_commons",
        python_callable=sentiment_from_commons,
    )

    sentiment_task
