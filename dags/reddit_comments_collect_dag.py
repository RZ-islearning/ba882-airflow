# -*- coding: utf-8 -*-
"""
DAG 1: reddit_comments_collect_dag
----------------------------------
每天 8:00 运行：
- 使用 Reddit API 为 10 首歌抓取帖子及其评论
- 只做“数据扒取”，不做情感分析
- 结果写入 GCS：gs://<bucket>/commons/reddit_comments_YYYYMMDD.json

Bucket 和前缀可通过环境变量覆盖：
- GCS_REDDIT_BUCKET  默认 "reddit_sanbox"
- GCS_COMMONS_PREFIX 默认 "commons"
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

import requests

from airflow import DAG

# PythonOperator (Airflow 2.x / 3.x)
try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator

# BaseHook 兼容导入
try:
    from airflow.hooks.base import BaseHook
except ImportError:
    from airflow.models.connection import Connection as BaseHook

# GCS client
from google.cloud import storage


# ------------------------ Secret Loader ------------------------

def get_secret_value(name: str, default: Optional[str] = None) -> str:
    """
    从多种来源读取密钥：
    1. GCP Secret Manager
    2. 环境变量 (直接匹配，或转大写匹配)
    3. Airflow Variable
    """
    # 1) GCP Secret Manager
    try:
        from google.cloud import secretmanager

        project_id = (
            os.environ.get("GCP_PROJECT")
            or os.environ.get("GOOGLE_CLOUD_PROJECT")
            or os.environ.get("ASTRO_GCP_PROJECT")
        )
        if project_id:
            client = secretmanager.SecretManagerServiceClient()
            secret_name = name
            if not secret_name.startswith("projects/"):
                secret_name = f"projects/{project_id}/secrets/{name}/versions/latest"
            response = client.access_secret_version(request={"name": secret_name})
            value = response.payload.data.decode("utf-8")
            logging.info("[secret] Loaded from GCP Secret Manager: %s", name)
            return value
    except Exception as e:
        # 这里只打印 warning，不阻断，因为可能在 ENV 里找到
        pass

    # 2) ENV (环境变量)
    # 优先找完全匹配的 (例如 REDDIT_CLIENT_ID)
    # 其次找大写的 (例如 reddit-client-id -> REDDIT-CLIENT-ID)
    # 最后找替换横杠为下划线并大写的 (例如 reddit-client-id -> REDDIT_CLIENT_ID)
    candidates = [name, name.upper(), name.replace("-", "_").upper()]
    for key in candidates:
        val = os.environ.get(key)
        if val:
            logging.info("[secret] Loaded from ENV: %s", key)
            return val

    # 3) Airflow Variable
    try:
        from airflow.models import Variable
        value = Variable.get(name)
        logging.info("[secret] Loaded from Airflow Variable: %s", name)
        return value
    except Exception:
        pass

    # 4) Airflow Connection (作为最后的兜底)
    try:
        conn = BaseHook.get_connection(name)
        if getattr(conn, "password", None):
            logging.info("[secret] Loaded from Connection.password: %s", name)
            return conn.password
        extra = getattr(conn, "extra_dejson", None) or {}
        for k in ["api_key", "token", "password", "secret", name]:
            if k in extra:
                logging.info("[secret] Loaded from Connection.extra: %s", name)
                return extra[k]
    except Exception:
        pass

    if default is not None:
        logging.info("[secret] Using default for: %s", name)
        return default

    raise RuntimeError(
        f"Secret '{name}' not found in any source (GCP / ENV / Variable / Connection)."
    )


def _mask(value: Optional[str]) -> str:
    if not value:
        return "<empty>"
    if len(value) <= 6:
        return "***"
    return value[:3] + "***" + value[-3:]


# ------------------------ Reddit helpers ------------------------

def _get_reddit_token(client_id: str, client_secret: str, user_agent: str) -> str:
    auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
    data = {"grant_type": "client_credentials"}
    headers = {"User-Agent": user_agent}
    
    # 打印一下正在尝试连接的 User Agent (方便调试)
    logging.info(f"[reddit] Requesting token with User-Agent: {user_agent}")
    
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
    """
    为某个帖子获取评论列表（扁平化顶层 & 二级评论）
    """
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
    comments = []

    def _walk(node_list):
        for item in node_list:
            kind = item.get("kind")
            d = item.get("data", {})
            if kind == "t1":  # comment
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
            # kind == more 等类型这里略过

    children = comments_block.get("data", {}).get("children", [])
    _walk(children)
    return comments


# ------------------------ Songs list (10 songs) ------------------------

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


# ------------------------ Main task: collect comments ------------------------

def collect_reddit_comments(**_kwargs):
    """
    任务函数：
    - 为 10 首歌搜索帖子
    - 为每个帖子抓评论
    - 把所有评论写入 GCS commons 目录（JSON）
    """
    # ⚠️ 修改点：直接使用 Astronomer 环境变量中配置的大写 KEY
    client_id = get_secret_value("REDDIT_CLIENT_ID")
    client_secret = get_secret_value("REDDIT_CLIENT_SECRET")
    
    # User Agent 也直接用环境变量里的 key
    user_agent = get_secret_value(
        "USER_AGENT", default="ba882-reddit-pipeline by u/Haojiang1"
    )

    logging.info("REDDIT_CLIENT_ID: %s", _mask(client_id))
    logging.info("REDDIT_CLIENT_SECRET: %s", _mask(client_secret))
    logging.info("USER_AGENT: %s", _mask(user_agent))

    token = _get_reddit_token(client_id, client_secret, user_agent)
    logging.info("[reddit] OAuth token obtained.")

    all_rows: List[Dict[str, Any]] = []
    for song in TARGET_SONGS:
        q = f'"{song["title"]}" {song["artist"]}'
        logging.info("[reddit] Searching posts for query: %s", q)
        try:
            posts = _fetch_posts_for_query(token, user_agent, q, limit=20)
        except Exception as e:
            logging.error("[reddit] Failed to fetch posts for %s: %s", q, e)
            continue

        logging.info("[reddit] Found %d posts for query %s", len(posts), q)

        for p in posts:
            post_id = p.get("id")
            if not post_id:
                continue
            try:
                comments = _fetch_comments_for_post(
                    token, user_agent, post_id, limit=50
                )
            except Exception as e:
                logging.error(
                    "[reddit] Failed to fetch comments for post %s: %s",
                    post_id,
                    e,
                )
                continue

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

    logging.info("[reddit] Total comments collected: %d", len(all_rows))

    # 写入 GCS commons 目录
    bucket_name = os.environ.get("GCS_REDDIT_BUCKET", "reddit_sanbox")
    prefix = os.environ.get("GCS_COMMONS_PREFIX", "commons").lstrip("/")
    today_str = datetime.utcnow().strftime("%Y%m%d")
    object_name = f"{prefix}/reddit_comments_{today_str}.json"

    # 如果没有 google-cloud-storage 库（本地测试情况），这段可能会报错
    # 但在 Astro 云端应该没问题
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_string(json.dumps(all_rows), content_type="application/json")

    logging.info(
        "Uploaded %d comments to gs://%s/%s",
        len(all_rows),
        bucket_name,
        object_name,
    )


# ------------------------ DAG definition ------------------------

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="reddit_comments_collect_dag",
    default_args=default_args,
    start_date=datetime(2024, 10, 1),
    # 使用 schedule 而不是 schedule_interval (Airflow 3 兼容)
    schedule="0 8 * * *",
    catchup=False,
    tags=["reddit", "data-collection", "etl"],
) as dag:

    collect_task = PythonOperator(
        task_id="collect_reddit_comments",
        python_callable=collect_reddit_comments,
    )

    collect_task