# -*- coding: utf-8 -*-
"""
DAG 3: reddit_sentiment_llm_dag
----------------------------------------
每天 8:30 运行（在 NLP sentiment DAG 之后）：
- 从 GCS processed 目录读取 separation DAG 存的 reddit_comments_YYYYMMDD.json
- 使用 OpenAI LLM 对 comment_body 做情感分析
- 结果写入 GCS：gs://<bucket>/database/reddit_sentiment_llm_YYYYMMDD.csv

环境变量：
- GCS_REDDIT_BUCKET     默认 "reddit_sandbox"
- GCS_PROCESSED_PREFIX  默认 "processed"
- GCS_DATABASE_PREFIX   默认 "database"
- OpenAI-API-Ran        OpenAI API key (必需)
- OPENAI_MODEL          默认 "gpt-3.5-turbo" (可选择 gpt-4)
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


def _analyze_sentiment_with_llm(text: str, client) -> Dict[str, Any]:
    """
    使用 OpenAI API 分析单条文本的情感
    返回：{"sentiment": "positive/negative/neutral", "score": 0.0-1.0, "reasoning": "..."}
    """
    if not text or not text.strip():
        return {
            "llm_sentiment": None,
            "llm_score": None,
            "llm_reasoning": None
        }

    try:
        response = client.chat.completions.create(
            model=os.environ.get("OPENAI_MODEL", "gpt-3.5-turbo"),
            messages=[
                {
                    "role": "system",
                    "content": """You are a sentiment analysis expert. Analyze the sentiment of the given text and respond with ONLY a JSON object in this exact format:
{"sentiment": "positive/negative/neutral", "score": 0.85, "reasoning": "brief explanation"}

Where:
- sentiment: must be exactly "positive", "negative", or "neutral"
- score: a confidence score between 0.0 and 1.0
- reasoning: a brief explanation (max 100 characters)"""
                },
                {
                    "role": "user",
                    "content": f"Analyze this text: {text}"
                }
            ],
            temperature=0.3,
            max_tokens=150
        )

        result_text = response.choices[0].message.content.strip()
        # Parse JSON response
        result = json.loads(result_text)

        return {
            "llm_sentiment": result.get("sentiment"),
            "llm_score": result.get("score"),
            "llm_reasoning": result.get("reasoning")
        }
    except Exception as e:
        logging.error(f"Error analyzing sentiment with LLM: {e}")
        return {
            "llm_sentiment": "error",
            "llm_score": None,
            "llm_reasoning": str(e)[:100]
        }


def sentiment_from_commons_llm(**kwargs):
    """
    从 GCS 读取 reddit comments，使用 OpenAI LLM 做情感分析
    """
    # Import OpenAI here to avoid parse-time overhead
    from openai import OpenAI

    # 检查并解析 API key (可能是 JSON 格式)
    api_key_raw = os.environ.get("OpenAI-API-Ran")
    if not api_key_raw:
        raise ValueError("OpenAI-API-Ran environment variable not set")

    # Debug: 打印原始值的类型和前50个字符
    logging.info(f"Raw API key type: {type(api_key_raw)}")
    logging.info(f"Raw API key preview: {str(api_key_raw)[:50]}...")

    # 如果是 JSON 格式，提取 value 字段
    api_key = api_key_raw
    if isinstance(api_key_raw, str) and api_key_raw.strip().startswith('{'):
        try:
            api_key_json = json.loads(api_key_raw)
            logging.info(f"Parsed as JSON, keys: {list(api_key_json.keys())}")
            api_key = api_key_json.get("value") or api_key_json.get("api_key") or api_key_raw
            logging.info(f"Extracted API key type: {type(api_key)}, preview: {str(api_key)[:10]}...")
        except (json.JSONDecodeError, AttributeError, TypeError) as e:
            logging.warning(f"Failed to parse as JSON: {e}")
            api_key = api_key_raw

    # 最终验证
    if not api_key or not isinstance(api_key, str):
        raise ValueError(f"Invalid API key format: type={type(api_key)}")

    if not api_key.startswith("sk-"):
        logging.error(f"API key doesn't start with 'sk-': {api_key[:20]}...")
        raise ValueError("API key should start with 'sk-'")

    logging.info(f"Final API key ready, starts with: {api_key[:10]}...")
    client = OpenAI(api_key=api_key)

    # 使用 DAG 的执行日期（支持 backfill）
    execution_date = kwargs.get("logical_date") or kwargs.get("execution_date")
    today_str = execution_date.strftime("%Y%m%d")

    bucket_name = os.environ.get("GCS_REDDIT_BUCKET", "reddit_sandbox")
    processed_prefix = os.environ.get("GCS_PROCESSED_PREFIX", "processed").lstrip("/")
    db_prefix = os.environ.get("GCS_DATABASE_PREFIX", "database").lstrip("/")

    processed_object = f"{processed_prefix}/reddit_comments_{today_str}.json"
    target_object = f"{db_prefix}/reddit_sentiment_llm_{today_str}.csv"

    logging.info("Reading processed data from gs://%s/%s", bucket_name, processed_object)

    rows = _download_json_from_gcs(bucket_name, processed_object)
    logging.info("Loaded %d comments from commons", len(rows))

    enriched_rows: List[Dict[str, Any]] = []
    total = len(rows)

    for idx, r in enumerate(rows, 1):
        body = r.get("comment_body") or ""

        # 每处理10条记录打印一次进度
        if idx % 10 == 0:
            logging.info(f"Processing comment {idx}/{total}...")

        # 使用 LLM 分析情感
        sentiment_result = _analyze_sentiment_with_llm(body, client)

        new_r = dict(r)
        new_r.update(sentiment_result)
        enriched_rows.append(new_r)

    logging.info("Finished LLM sentiment analysis for %d comments", len(enriched_rows))

    logging.info(
        "Writing LLM sentiment CSV to gs://%s/%s", bucket_name, target_object
    )
    _upload_csv_to_gcs(enriched_rows, bucket_name, target_object)


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="reddit_sentiment_llm_dag",
    default_args=default_args,
    start_date=datetime(2024, 10, 1),
    schedule="30 8 * * *",  # 每天 8:30 跑，在 NLP sentiment DAG 之后
    catchup=False,
    tags=["reddit", "sentiment-analysis", "llm", "openai"],
) as dag:

    sentiment_llm_task = PythonOperator(
        task_id="sentiment_from_commons_llm",
        python_callable=sentiment_from_commons_llm,
    )

    sentiment_llm_task
