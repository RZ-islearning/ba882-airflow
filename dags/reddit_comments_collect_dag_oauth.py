
# -*- coding: utf-8 -*-
"""
DAG 1: reddit_comments_collect_dag
----------------------------------
每天 8:00 运行：
- 使用 Reddit API (OAuth) 为 10 首歌抓取帖子及其评论
- 只做“数据扒取”，不做情感分析
- 结果写入 GCS：gs://<bucket>/commons/reddit_comments_YYYYMMDD.json

需要的环境变量：
- REDDIT_CLIENT_ID
- REDDIT_CLIENT_SECRET
- USER_AGENT
- GCS_REDDIT_BUCKET (可选)
- GCS_COMMONS_PREFIX (可选)
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

from google.cloud import storage


def get_secret_value(name: str, default: Optional[str] = None) -> str:
    candidates = [name, name.upper(), name.replace("-", "_").upper()]
    for key in candidates:
        val = os.environ.get(key)
        if val:
            return val
    if default is not None:
        return default
    raise RuntimeError(f"Secret '{name}' not found.")


def _mask(value: Optional[str]) -> str:
    if not value:
        return "<empty>"
    if len(value) <= 6:
        return "***"
    return value[:3] + "***" + value[-3:]


def _get_reddit_token(client_id: str, client_secret: str, user_agent: str) -> str:
    auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
    data = {"grant_type": "client_credentials"}
    headers = {"User-Agent": user_agent}
    resp = requests.post(
        "https://www.reddit.com/api/v1/access_token",
        auth=auth,
        data=data,
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _fetch_posts_for_query(
    token: str, user_agent: str, query: str, limit: int = 20
) -> List[Dict[str, Any]]:
    headers = {
        "Authorization": f"bearer {token}",
        "User-Agent": user_agent,
    }
    params = {
        "q": query,
        "limit": limit,
        "sort": "new",
        "restrict_sr": False,
    }
    resp = requests.get(
        "https://oauth.reddit.com/search",
        headers=headers,
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    children = data.get("data", {}).get("children", [])
    posts = []
    for c in children:
        d = c.get("data", {})
        posts.append(
            {
                "id": d.get("id"),
                "subreddit": d.get("subreddit"),
                "title": d.get("title"),
                "score": d.get("score"),
                "num_comments": d.get("num_comments"),
                "created_utc": d.get("created_utc"),
                "permalink": d.get("permalink"),
                "url": d.get("url"),
            }
        )
    return posts


def _fetch_comments_for_post(
    token: str, user_agent: str, post_id: str, limit: int = 50
) -> List[Dict[str, Any]]:
    headers = {
        "Authorization": f"bearer {token}",
        "User-Agent": user_agent,
    }
    url = f"https://oauth.reddit.com/comments/{post_id}"
    params = {"limit": limit, "sort": "best"}
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list) or len(data) < 2:
        return []

    comments_block = data[1]
    comments: List[Dict[str, Any]] = []

    def _walk(node_list):
        for item in node_list:
            kind = item.get("kind")
            d = item.get("data", {})
            if kind == "t1":
                comments.append(
                    {
                        "comment_id": d.get("id"),
                        "comment_body": d.get("body"),
                        "comment_score": d.get("score"),
                        "comment_created_utc": d.get("created_utc"),
                        "comment_permalink": d.get("permalink"),
                    }
                )
                replies = d.get("replies")
                if isinstance(replies, dict):
                    children = replies.get("data", {}).get("children", [])
                    _walk(children)

    children = comments_block.get("data", {}).get("children", [])
    _walk(children)
    return comments


TARGET_SONGS: List[Dict[str, str]] = [
    {"title": "The Fate of Ophelia", "artist": "Taylor Swift", "note": "10/5"},
    {"title": "Camera", "artist": "Ed Sheeran", "note": "9/12"},
    {"title": "Gorgeous", "artist": "Doja Cat", "note": "10/10"},
    {"title": "Kiss", "artist": "Demi Lovato", "note": "10"},
    {"title": "DEPRESSED", "artist": "Anne-Marie", "note": "9/19"},
    {"title": "bittersweet", "artist": "Madison Beer", "note": "10/21"},
    {"title": "Safe", "artist": "Cardi B feat. Kehlani", "note": "9"},
    {"title": "Artificial Angels", "artist": "Grimes", "note": "10/23"},
    {"title": "CHANEL", "artist": "Tyla", "note": "10"},
    {"title": "Dracula", "artist": "Tame Impala", "note": "9/26"},
]


def collect_reddit_comments(**_kwargs):
    client_id = get_secret_value("REDDIT_CLIENT_ID")
    client_secret = get_secret_value("REDDIT_CLIENT_SECRET")
    user_agent = get_secret_value("USER_AGENT", default="ba882-reddit-pipeline")

    logging.info("REDDIT_CLIENT_ID: %s", _mask(client_id))
    logging.info("REDDIT_CLIENT_SECRET: %s", _mask(client_secret))
    logging.info("REDDIT_USER_AGENT: %s", _mask(user_agent))

    token = _get_reddit_token(client_id, client_secret, user_agent)
    logging.info("[reddit] OAuth token obtained.")

    all_rows: List[Dict[str, Any]] = []

    for song in TARGET_SONGS:
        q = f'"{song["title"]}" {song["artist"]}'
        posts = _fetch_posts_for_query(token, user_agent, q, limit=20)

        for p in posts:
            post_id = p.get("id")
            if not post_id:
                continue
            comments = _fetch_comments_for_post(token, user_agent, post_id, limit=50)

            for c in comments:
                row = {
                    "song_title": song["title"],
                    "song_artist": song["artist"],
                    "song_note": song["note"],
                    "query": q,
                    "post_id": post_id,
                    "post_subreddit": p.get("subreddit"),
                    "post_title": p.get("title"),
                    "post_score": p.get("score"),
                    "post_num_comments": p.get("num_comments"),
                    "post_created_utc": p.get("created_utc"),
                    "post_permalink": p.get("permalink"),
                    "post_url": p.get("url"),
                    "comment_id": c.get("comment_id"),
                    "comment_body": c.get("comment_body"),
                    "comment_score": c.get("comment_score"),
                    "comment_created_utc": c.get("comment_created_utc"),
                    "comment_permalink": c.get("comment_permalink"),
                    "ingest_time_utc": datetime.utcnow().isoformat(),
                }
                all_rows.append(row)

    bucket_name = os.environ.get("GCS_REDDIT_BUCKET", "reddit_sanbox")
    prefix = os.environ.get("GCS_COMMONS_PREFIX", "commons").lstrip("/")
    today_str = datetime.utcnow().strftime("%Y%m%d")
    object_name = f"{prefix}/reddit_comments_{today_str}.json"

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_string(json.dumps(all_rows), content_type="application/json")

    logging.info("Uploaded %d comments to gs://%s/%s", len(all_rows), bucket_name, object_name)


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="reddit_comments_collect_dag",
    default_args=default_args,
    start_date=datetime(2024, 10, 1),
    schedule="0 8 * * *",
    catchup=False,
    tags=["reddit", "data-collection", "etl"],
) as dag:

    collect_task = PythonOperator(
        task_id="collect_reddit_comments",
        python_callable=collect_reddit_comments,
    )

    collect_task
