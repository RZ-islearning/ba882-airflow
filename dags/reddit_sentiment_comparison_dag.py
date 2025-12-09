# -*- coding: utf-8 -*-
"""
DAG 4: reddit_sentiment_comparison_dag
----------------------------------------
每天 9:00 运行（在 NLP 和 LLM sentiment DAGs 之后）：
- 从 GCS database 目录读取 NLP 和 LLM 的情感分析结果
- 合并两个结果并进行对比分析
- 生成对比报告：gs://<bucket>/database/reddit_sentiment_comparison_YYYYMMDD.csv

环境变量：
- GCS_REDDIT_BUCKET     默认 "reddit_sandbox"
- GCS_DATABASE_PREFIX   默认 "database"
"""

import os
import csv
import io
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator

from google.cloud import storage


def _download_csv_from_gcs(bucket_name: str, object_name: str) -> List[Dict[str, Any]]:
    """从 GCS 下载 CSV 并转换为字典列表"""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    if not blob.exists():
        raise FileNotFoundError(f"Object gs://{bucket_name}/{object_name} not found")

    data = blob.download_as_text()
    reader = csv.DictReader(io.StringIO(data))
    return list(reader)


def _upload_csv_to_gcs(
    rows: List[Dict[str, Any]], bucket_name: str, object_name: str
) -> None:
    """上传字典列表为 CSV 到 GCS"""
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


def _convert_nlp_to_label(compound_score: float) -> str:
    """
    将 NLP VADER compound score 转换为 positive/negative/neutral 标签
    VADER 标准：compound >= 0.05 为 positive, <= -0.05 为 negative，否则为 neutral
    """
    if compound_score >= 0.05:
        return "positive"
    elif compound_score <= -0.05:
        return "negative"
    else:
        return "neutral"


def compare_sentiments(**kwargs):
    """
    比较 NLP 和 LLM 的情感分析结果
    """
    # 使用 DAG 的执行日期（支持 backfill）
    execution_date = kwargs.get("logical_date") or kwargs.get("execution_date")
    today_str = execution_date.strftime("%Y%m%d")

    bucket_name = os.environ.get("GCS_REDDIT_BUCKET", "reddit_sandbox")
    db_prefix = os.environ.get("GCS_DATABASE_PREFIX", "database").lstrip("/")

    nlp_object = f"{db_prefix}/reddit_sentiment_{today_str}.csv"
    llm_object = f"{db_prefix}/reddit_sentiment_llm_{today_str}.csv"
    comparison_object = f"{db_prefix}/reddit_sentiment_comparison_{today_str}.csv"

    logging.info("Reading NLP results from gs://%s/%s", bucket_name, nlp_object)
    nlp_rows = _download_csv_from_gcs(bucket_name, nlp_object)
    logging.info("Loaded %d NLP sentiment rows", len(nlp_rows))

    logging.info("Reading LLM results from gs://%s/%s", bucket_name, llm_object)
    llm_rows = _download_csv_from_gcs(bucket_name, llm_object)
    logging.info("Loaded %d LLM sentiment rows", len(llm_rows))

    if len(nlp_rows) != len(llm_rows):
        logging.warning(
            f"Row count mismatch: NLP={len(nlp_rows)}, LLM={len(llm_rows)}"
        )

    # 合并结果并进行对比
    comparison_rows: List[Dict[str, Any]] = []
    agreement_count = 0
    total_count = min(len(nlp_rows), len(llm_rows))

    for i in range(total_count):
        nlp_row = nlp_rows[i]
        llm_row = llm_rows[i]

        # 提取 NLP 结果
        nlp_compound = nlp_row.get("sentiment_compound")
        nlp_label = None
        if nlp_compound and nlp_compound != "":
            try:
                nlp_compound_float = float(nlp_compound)
                nlp_label = _convert_nlp_to_label(nlp_compound_float)
            except (ValueError, TypeError):
                nlp_label = "unknown"
        else:
            nlp_label = "unknown"

        # 提取 LLM 结果
        llm_label = llm_row.get("llm_sentiment", "unknown")
        llm_score = llm_row.get("llm_score", "")
        llm_reasoning = llm_row.get("llm_reasoning", "")

        # 判断是否一致
        agreement = (nlp_label == llm_label) if (nlp_label != "unknown" and llm_label not in ["unknown", "error"]) else None
        if agreement is True:
            agreement_count += 1

        # 构建对比行
        comparison_row = {
            # 原始数据字段（从 nlp_row 获取，假设两者的基础字段相同）
            "comment_body": nlp_row.get("comment_body", ""),
            "comment_id": nlp_row.get("comment_id", ""),
            "post_id": nlp_row.get("post_id", ""),
            "subreddit": nlp_row.get("subreddit", ""),

            # NLP 结果
            "nlp_compound": nlp_compound,
            "nlp_pos": nlp_row.get("sentiment_pos", ""),
            "nlp_neg": nlp_row.get("sentiment_neg", ""),
            "nlp_neu": nlp_row.get("sentiment_neu", ""),
            "nlp_label": nlp_label,

            # LLM 结果
            "llm_sentiment": llm_label,
            "llm_score": llm_score,
            "llm_reasoning": llm_reasoning,

            # 对比结果
            "agreement": "Yes" if agreement is True else ("No" if agreement is False else "N/A"),
        }

        comparison_rows.append(comparison_row)

    # 计算总体统计
    agreement_rate = (agreement_count / total_count * 100) if total_count > 0 else 0
    logging.info(
        f"Comparison complete: {agreement_count}/{total_count} agreements ({agreement_rate:.2f}%)"
    )

    # 添加统计摘要到日志
    logging.info(f"Total comments analyzed: {total_count}")
    logging.info(f"Agreements: {agreement_count}")
    logging.info(f"Agreement rate: {agreement_rate:.2f}%")

    logging.info(
        "Writing comparison CSV to gs://%s/%s", bucket_name, comparison_object
    )
    _upload_csv_to_gcs(comparison_rows, bucket_name, comparison_object)

    # 将统计信息存储到 XCom 供后续使用
    kwargs["ti"].xcom_push(key="total_count", value=total_count)
    kwargs["ti"].xcom_push(key="agreement_count", value=agreement_count)
    kwargs["ti"].xcom_push(key="agreement_rate", value=agreement_rate)


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="reddit_sentiment_comparison_dag",
    default_args=default_args,
    start_date=datetime(2024, 10, 1),
    schedule="0 9 * * *",  # 每天 9:00 跑，确保前两个 DAG 都已完成
    catchup=False,
    tags=["reddit", "sentiment-analysis", "comparison"],
) as dag:

    comparison_task = PythonOperator(
        task_id="compare_sentiments",
        python_callable=compare_sentiments,
    )

    comparison_task
