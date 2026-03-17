"""自动识别并删除 401 账号的脚本。

功能:
- 拉取 auth-files 列表
- 过滤 status_message 中包含 "status": 401 的条目
- 逐个调用 DELETE 接口删除对应账号文件

依赖:
    pip install curl_cffi python-dotenv

配置:
- 仅从环境变量 .env 读取
"""

from __future__ import annotations

import json
import os
import time
from typing import Iterable, List, Optional
from urllib.parse import quote

from curl_cffi import requests as curl_requests
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(ENV_PATH)


DEFAULT_CONFIG = {
    "management_api_url": "http://ai.meetlife.top:8318/v0/management/auth-files",
    "management_api_token": "",
    "proxy": "",
    "timeout": 20,
    "delete_delay": 0.2,
    "management_debug": False,
}


def _as_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _as_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def load_config() -> dict:
    config = DEFAULT_CONFIG.copy()
    config_path = os.path.join(BASE_DIR, "config.json")
    if os.path.exists(config_path):
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config.update(json.load(f))
        except Exception as exc:  # pragma: no cover
            print(f"⚠️ 加载 config.json 失败: {exc}")

    config["management_api_url"] = os.environ.get(
        "MANAGEMENT_API_URL", config["management_api_url"]
    )
    config["management_api_token"] = os.environ.get(
        "MANAGEMENT_API_TOKEN", config["management_api_token"]
    )
    config["proxy"] = os.environ.get("PROXY", config["proxy"])
    config["timeout"] = int(os.environ.get("MANAGEMENT_API_TIMEOUT", config["timeout"]))
    config["delete_delay"] = float(
        os.environ.get("MANAGEMENT_DELETE_DELAY", config["delete_delay"])
    )
    return config


def _build_headers(token: str) -> dict:
    headers = {
        "accept": "application/json, text/plain, */*",
    }
    if token:
        headers["authorization"] = f"Bearer {token}"
    return headers


def _extract_status_code(status_message: Optional[str]) -> Optional[int]:
    if not status_message:
        return None
    try:
        payload = json.loads(status_message)
    except json.JSONDecodeError:
        return None
    if isinstance(payload, dict):
        status = payload.get("status")
        if isinstance(status, int):
            return status
    return None


def _iter_auth_files(data: object) -> Iterable[dict]:
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                yield item
        return
    if isinstance(data, dict):
        for key in ("files", "data", "items", "records"):
            value = data.get(key)
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        yield item
                return


def fetch_auth_files(config: dict) -> List[dict]:
    url = config["management_api_url"].rstrip("/")
    proxies = {"http": config["proxy"], "https": config["proxy"]} if config["proxy"] else None
    response = curl_requests.get(
        url,
        headers=_build_headers(config["management_api_token"]),
        proxies=proxies,
        timeout=config["timeout"],
    )
    if config.get("management_debug"):
        print(f"🐞 请求 URL: {url}")
        print(f"🐞 状态码: {response.status_code}")
        print(f"🐞 Content-Type: {response.headers.get('content-type')}")
    response.raise_for_status()
    try:
        payload = response.json()
    except Exception:
        if config.get("management_debug"):
            print("🐞 JSON 解析失败，保留原始响应供排查")
        raise
    if config.get("management_debug"):
        if isinstance(payload, list):
            print(f"🐞 响应结构: list, 长度={len(payload)}")
            if payload:
                print(f"🐞 首条 keys: {list(payload[0].keys())}")
        elif isinstance(payload, dict):
            print(f"🐞 响应结构: dict, keys={list(payload.keys())}")
            for key in ("files", "data", "items", "records"):
                value = payload.get(key)
                if isinstance(value, list):
                    print(f"🐞 {key} 长度={len(value)}")
                    if value:
                        print(f"🐞 {key} 首条 keys: {list(value[0].keys())}")
                    break
        else:
            print(f"🐞 响应结构: {type(payload)}")
    return list(_iter_auth_files(payload))


def delete_auth_file(config: dict, name: str) -> bool:
    if not name:
        return False
    base_url = config["management_api_url"].rstrip("/")
    encoded = quote(name, safe="")
    url = f"{base_url}?name={encoded}"
    proxies = {"http": config["proxy"], "https": config["proxy"]} if config["proxy"] else None
    response = curl_requests.delete(
        url,
        headers=_build_headers(config["management_api_token"]),
        proxies=proxies,
        timeout=config["timeout"],
    )
    response.raise_for_status()
    return True


def main() -> None:
    config = load_config()
    if not config["management_api_token"]:
        print("⚠️ 未设置管理接口 Token，可在 .env 中设置")
    print("🔎 正在获取 auth-files 列表...")
    items = fetch_auth_files(config)
    print(f"✅ 获取到 {len(items)} 条记录")

    targets = []
    for item in items:
        status_message = item.get("status_message")
        status_code = _extract_status_code(status_message)
        if status_code == 401:
            name = item.get("name") or item.get("id")
            if name:
                targets.append(name)

    if not targets:
        print("🎉 未发现 401 账号")
        return

    print(f"🧹 发现 {len(targets)} 个 401 账号，准备删除...")
    success = 0
    for idx, name in enumerate(targets, start=1):
        try:
            delete_auth_file(config, name)
            success += 1
            print(f"✅ [{idx}/{len(targets)}] 删除成功: {name}")
        except Exception as exc:  # pragma: no cover
            print(f"❌ [{idx}/{len(targets)}] 删除失败: {name} -> {exc}")
        if config["delete_delay"]:
            time.sleep(config["delete_delay"])

    print(f"✅ 删除完成: {success}/{len(targets)}")


if __name__ == "__main__":
    main()
