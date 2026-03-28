#!/usr/bin/env python3
"""
Token Atlas API 工具

接口分类：
- 会话类接口（Cookie 认证）：/me, /me/claim, /me/claims, /me/claims/archive
- API Key 接口（X-API-Key 认证）：/api/claim, /api/claims/{request_id}, /api/download/{token_id}

重构说明：
- 获取队列：负责创建/恢复 request，并轮询等待进入可下载状态
- 下载队列：负责消费已完成 request 里的 token 下载任务，并按需保存到本地指定目录
- 同步/异步协同：HTTP 请求复用同步 httpx，整体由 asyncio 队列统一调度
"""

import asyncio
import json
import os
import ssl
import sys
import threading
import time
from pathlib import Path
from typing import Any

import httpx

# ============ 配置参数 ============
API_BASE = "https://gptfreetoken.pony.indevs.in"
SAVE_DIR = Path(__file__).parent
ENV_FILE = SAVE_DIR / ".env"
CLAIM_COUNT = 2  # 领取数量


def load_env() -> dict[str, str]:
    """从 .env 读取配置"""
    env = {}
    if ENV_FILE.exists():
        with open(ENV_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    env[key.strip()] = value.strip()
    return env


env_config = load_env()
SESSION_COOKIE = env_config.get("TOKEN_ATLAS_SESSION", "")
API_KEY = env_config.get("TOKEN_ATLAS_API_KEY", "")
TOKEN_SAVE_DIR = env_config.get("TOKEN_SAVE_DIR", "")
POLL_INTERVAL_SECONDS = int(env_config.get("TOKEN_ATLAS_POLL_INTERVAL_SECONDS", "30"))
POLL_MAX_ATTEMPTS = int(env_config.get("TOKEN_ATLAS_POLL_MAX_ATTEMPTS", "20"))
DOWNLOAD_CONCURRENCY = max(1, int(env_config.get("TOKEN_ATLAS_DOWNLOAD_CONCURRENCY", "3")))
VERIFY_SSL = env_config.get("TOKEN_ATLAS_VERIFY_SSL", os.environ.get("TOKEN_ATLAS_VERIFY_SSL", "true")).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
# =================================

HISTORY_FILE = SAVE_DIR / "request_history.json"
HISTORY_LOCK = threading.Lock()


def build_ssl_context(verify_ssl: bool) -> ssl.SSLContext:
    """构造 SSL Context，默认校验证书，必要时可关闭校验"""
    if verify_ssl:
        return ssl.create_default_context()

    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context

# ============ 基础请求函数 ============
def _send_http_request(
    method: str,
    url: str,
    *,
    headers: dict[str, str],
    json_data: dict[str, Any] | None = None,
    timeout: float = 30,
    fallback_message: str = "[Warning] SSL 证书校验失败，尝试关闭校验后重试",
) -> httpx.Response | None:
    """统一发送 HTTP 请求，必要时回退到关闭 SSL 校验"""
    try:
        with httpx.Client(verify=build_ssl_context(VERIFY_SSL), timeout=timeout) as client:
            response = client.request(method, url, headers=headers, json=json_data)
            response.raise_for_status()
            return response
    except httpx.HTTPStatusError as e:
        error_body = e.response.text if e.response is not None else ""
        print(f"[Error] HTTP {e.response.status_code}: {error_body}")
        return None
    except httpx.ConnectError as e:
        cause = e.__cause__
        if isinstance(cause, ssl.SSLCertVerificationError):
            if VERIFY_SSL:
                print(fallback_message)
                try:
                    with httpx.Client(verify=False, timeout=timeout) as client:
                        response = client.request(method, url, headers=headers, json=json_data)
                        response.raise_for_status()
                        print(f"[HTTP] {method} {url.removeprefix(API_BASE)} -> {response.status_code} (SSL verify disabled)")
                        return response
                except httpx.HTTPStatusError as retry_error:
                    retry_body = retry_error.response.text if retry_error.response is not None else ""
                    print(f"[Error] HTTP {retry_error.response.status_code}: {retry_body}")
                    return None
                except Exception as retry_error:
                    print(f"[Error] SSL 回退重试失败: {retry_error}")
                    return None
            print(f"[Error] SSL 证书校验失败: {e}")
            return None
        print(f"[Error] {e}")
        return None
    except httpx.HTTPError as e:
        print(f"[Error] {e}")
        return None
    except Exception as e:
        print(f"[Error] {e}")
        return None


def _request(
    method: str,
    endpoint: str,
    data: dict[str, Any] | None = None,
    *,
    use_api_key: bool = False,
    use_session: bool = False,
) -> dict[str, Any] | None:
    """统一请求函数"""
    url = f"{API_BASE}{endpoint}"
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": API_BASE,
    }

    if use_api_key and API_KEY:
        headers["X-API-Key"] = API_KEY
    elif use_session and SESSION_COOKIE:
        headers["Cookie"] = f"token_atlas_session={SESSION_COOKIE}"

    response = _send_http_request(method, url, headers=headers, json_data=data)
    if response is None:
        return None

    print(f"[HTTP] {method} {endpoint} -> {response.status_code}")
    try:
        return response.json()
    except json.JSONDecodeError as e:
        print(f"[Error] 响应 JSON 解析失败: {e}")
        return None


# ============ API Key / 会话接口 ============
def get_me() -> dict[str, Any] | None:
    """GET /me - 优先使用 Session，其次回退 API Key 获取用户信息、额度与领取统计"""
    print("[Me] 获取账户与额度信息")

    if SESSION_COOKIE:
        result = _request("GET", "/me", use_session=True)
        if result:
            return result
        print("[Me] Session 获取失败，尝试 API Key")

    if API_KEY:
        return _request("GET", "/me", use_api_key=True)

    return None


def session_claim(count: int = 1) -> dict[str, Any] | None:
    """POST /me/claim - 使用会话 Cookie 创建领取请求"""
    print(f"[Claim] 发起申请，count={count}")
    return _request("POST", "/me/claim", {"count": count}, use_session=True)


def get_claims() -> dict[str, Any] | None:
    """GET /me/claims - 列出当前会话已领取的 JSON 账号"""
    print("[Claim] 获取已领取账号列表")
    return _request("GET", "/me/claims", use_session=True)


def download_archive() -> bytes | None:
    """GET /me/claims/archive - 下载已领取 JSON 账号的 ZIP 打包"""
    print(f"\n{'=' * 60}")
    print("  [会话接口] GET /me/claims/archive - 下载 ZIP 打包")
    print(f"{'=' * 60}")

    url = f"{API_BASE}/me/claims/archive"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Cookie": f"token_atlas_session={SESSION_COOKIE}",
    }

    response = _send_http_request(
        "GET",
        url,
        headers=headers,
        timeout=60,
        fallback_message="[Warning] SSL 证书校验失败，尝试关闭校验后重试归档下载",
    )
    if response is None:
        return None

    data = response.content
    print(f"[GET /me/claims/archive] Status: {response.status_code}, Size: {len(data)} bytes")
    return data


# ============ API Key 接口 ============
def api_claim(count: int = 1) -> dict[str, Any] | None:
    """POST /api/claim - 使用 API Key 创建领取请求"""
    print(f"[Claim] API 申请，count={count}")
    return _request("POST", "/api/claim", {"count": count}, use_api_key=True)


def get_api_claim_status(request_id: str) -> dict[str, Any] | None:
    """GET /api/claims/{request_id} - 查询领取请求状态"""
    return _request("GET", f"/api/claims/{request_id}", use_api_key=True)


def download_token(token_id: str) -> dict[str, Any] | None:
    """GET /api/download/{token_id} - 下载 JSON"""
    return _request("GET", f"/api/download/{token_id}", use_api_key=True)


# ============ 历史记录管理 ============
TERMINAL_STATUSES = ("completed", "failed", "error", "success", "succeeded", "expired")
DOWNLOAD_SUCCESS_STATUSES = ("completed", "downloaded", "uploaded")


def normalize_request_status(data: dict[str, Any]) -> str:
    """统一解析 request 状态"""
    queue_status = str(data.get("queue_status") or "").strip().lower()
    if queue_status:
        if queue_status == "succeeded":
            return "completed"
        return queue_status

    status = str(data.get("status") or "").strip().lower()
    if status:
        return status

    queued = data.get("queued")
    if queued is True:
        return "queued_waiting"

    items = data.get("items")
    if isinstance(items, list) and items:
        return "completed"

    granted = data.get("granted")
    requested = data.get("requested")
    if isinstance(granted, int) and isinstance(requested, int) and granted >= requested and requested > 0:
        return "completed"

    return "unknown"


def load_request_history() -> dict[str, Any]:
    """加载 request_id 历史记录"""
    with HISTORY_LOCK:
        if HISTORY_FILE.exists():
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    return {"requests": []}


def save_request_history(history: dict[str, Any]) -> None:
    """保存 request_id 历史记录"""
    with HISTORY_LOCK:
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2, ensure_ascii=False)
    print(f"[History] 已更新 {HISTORY_FILE}")


def update_request_status(request_id: str, data: dict[str, Any]) -> None:
    """更新 request_id 状态到历史记录"""
    history = load_request_history()
    requests = history.get("requests", [])

    found = False
    for req in requests:
        if req.get("request_id") == request_id:
            existing_tokens = req.get("tokens", [])
            req.update(data)
            req["status"] = normalize_request_status(req)
            req.setdefault("tokens", existing_tokens)
            req["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            found = True
            break

    if not found:
        record = dict(data)
        record["request_id"] = request_id
        record["status"] = normalize_request_status(record)
        record["created_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        record.setdefault("tokens", [])
        requests.append(record)

    history["requests"] = requests
    save_request_history(history)


def remove_request_record(request_id: str) -> bool:
    """从历史记录中删除已完成处理的 request"""
    history = load_request_history()
    requests = history.get("requests", [])
    new_requests = [req for req in requests if req.get("request_id") != request_id]
    removed = len(new_requests) != len(requests)
    if removed:
        history["requests"] = new_requests
        save_request_history(history)
        print(f"[History] 已删除 request_id={request_id} 的历史记录")
    return removed


def find_pending_requests() -> list[dict[str, Any]]:
    """查找所有未完成的请求"""
    history = load_request_history()
    pending = []
    for req in history.get("requests", []):
        status = normalize_request_status(req)
        if status not in TERMINAL_STATUSES:
            req["status"] = status
            pending.append(req)
    return pending


def find_downloadable_requests() -> list[dict[str, Any]]:
    """查找已完成但尚未清理的请求"""
    history = load_request_history()
    downloadable = []
    for req in history.get("requests", []):
        status = normalize_request_status(req)
        if status in TERMINAL_STATUSES:
            req["status"] = status
            downloadable.append(req)
    return downloadable


def get_request_record(request_id: str) -> dict[str, Any] | None:
    """获取指定 request 的历史记录"""
    history = load_request_history()
    for req in history.get("requests", []):
        if req.get("request_id") == request_id:
            return req
    return None


def get_token_record(request_id: str, token_id: str | int) -> dict[str, Any] | None:
    """获取指定 token 的历史记录"""
    request_record = get_request_record(request_id)
    if not request_record:
        return None
    token_id_str = str(token_id)
    for token in request_record.get("tokens", []):
        if str(token.get("token_id")) == token_id_str:
            return token
    return None


def update_token_record(request_id: str, token_id: str | int, data: dict[str, Any]) -> None:
    """更新 token 下载/上传状态到历史记录"""
    history = load_request_history()
    requests = history.get("requests", [])
    token_id_str = str(token_id)

    for req in requests:
        if req.get("request_id") == request_id:
            tokens = req.setdefault("tokens", [])
            for token in tokens:
                if str(token.get("token_id")) == token_id_str:
                    token.update(data)
                    token["token_id"] = token_id
                    token["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
                    req["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
                    save_request_history(history)
                    return

            new_token = {
                "token_id": token_id,
                "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            new_token.update(data)
            tokens.append(new_token)
            req["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            save_request_history(history)
            return


def request_is_fully_processed(request_id: str) -> bool:
    """判断 request 下的所有 token 是否都已经完成下载处理"""
    request_record = get_request_record(request_id)
    if not request_record:
        return False

    status = normalize_request_status(request_record)
    if status not in TERMINAL_STATUSES:
        return False

    items = request_record.get("items", [])
    if not items:
        return True

    for item in items:
        token_id = item.get("token_id")
        if not token_id:
            return False
        token_record = get_token_record(request_id, token_id)
        if not token_record:
            return False
        if not token_record.get("downloaded"):
            return False
        if TOKEN_SAVE_DIR and not token_record.get("saved_to_local"):
            return False
    return True


# ============ 本地保存 ============
def save_to_local(filepath: str | Path) -> bool:
    """复制 JSON 文件到 TOKEN_SAVE_DIR 指定的本地目录"""
    if not TOKEN_SAVE_DIR:
        print("[Save] ⚠️ 未配置 TOKEN_SAVE_DIR，跳过保存")
        return False

    src = Path(filepath)
    if not src.exists():
        print(f"[Save] ❌ 源文件不存在: {src}")
        return False

    dest_dir = Path(TOKEN_SAVE_DIR)
    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / src.name
        import shutil
        shutil.copy2(src, dest_path)
        print(f"[Save] ✅ {src.name} 已保存到 {dest_path}")
        return True
    except Exception as e:
        print(f"[Save] ❌ {src.name} 保存失败: {e}")
        return False


# ============ 工具函数 ============
def save_json(data: dict[str, Any], filename: str) -> Path:
    """保存 JSON 到文件"""
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    filepath = SAVE_DIR / f"{filename}_{timestamp}.json"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"[Saved] {filepath}")
    return filepath


def save_json_fixed(data: dict[str, Any], filename: str) -> Path:
    """保存 JSON 到固定文件（覆盖）"""
    filepath = SAVE_DIR / f"{filename}.json"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"[Saved] {filepath}")
    return filepath


def save_binary(data: bytes, filename: str) -> Path:
    """保存二进制文件"""
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    filepath = SAVE_DIR / f"{filename}_{timestamp}.zip"
    with open(filepath, "wb") as f:
        f.write(data)
    print(f"[Saved] {filepath}")
    return filepath


def resolve_token_file_path(token_record: dict[str, Any] | None) -> Path | None:
    """从 token 历史中解析文件路径"""
    if not token_record:
        return None

    file_name = token_record.get("file_name")
    if file_name:
        path = SAVE_DIR / str(file_name)
        if path.exists():
            return path

    file_path = token_record.get("file_path")
    if file_path:
        path = Path(str(file_path))
        if not path.is_absolute():
            path = SAVE_DIR / path.name
        if path.exists():
            return path

    return None


def enqueue_request_if_needed(
    request_id: str,
    payload: dict[str, Any],
    fetch_queue: asyncio.Queue[dict[str, Any]],
    queued_ids: set[str],
) -> bool:
    """避免重复入队 request"""
    if request_id in queued_ids:
        return False
    fetch_queue.put_nowait({"request_id": request_id, "payload": payload})
    queued_ids.add(request_id)
    return True


def extract_me_quota(me_result: dict[str, Any] | None) -> tuple[int, int, int]:
    """从 /me 响应中提取额度摘要"""
    quota = me_result.get("quota") if isinstance(me_result, dict) else None
    quota_used = 0
    quota_limit = 0
    quota_remaining = 0
    if isinstance(quota, dict):
        try:
            quota_used = max(0, int(quota.get("used", 0)))
        except (TypeError, ValueError):
            quota_used = 0
        try:
            quota_limit = max(0, int(quota.get("limit", 0)))
        except (TypeError, ValueError):
            quota_limit = 0
        try:
            quota_remaining = max(0, int(quota.get("remaining", 0)))
        except (TypeError, ValueError):
            quota_remaining = 0
    return quota_used, quota_limit, quota_remaining


async def fetch_me_quota(log_prefix: str = "[Me]") -> tuple[dict[str, Any], int]:
    """查询 /me 并打印额度摘要，返回响应和剩余额度"""
    if not (SESSION_COOKIE or API_KEY):
        print("[Warning] 未配置 TOKEN_ATLAS_SESSION 或 TOKEN_ATLAS_API_KEY，跳过 /me 额度检查")
        return {}, 0

    me_result = await asyncio.to_thread(get_me)
    if not me_result:
        print("[Error] 获取用户信息失败")
        sys.exit(1)

    await asyncio.to_thread(save_json, me_result, "me")
    quota_used, quota_limit, quota_remaining = extract_me_quota(me_result)
    print(f"{log_prefix} 额度 used={quota_used}, limit={quota_limit}, remaining={quota_remaining}")
    return me_result, quota_remaining


async def create_new_claim_if_needed(fetch_queue: asyncio.Queue[dict[str, Any]], queued_ids: set[str]) -> None:
    """没有待处理任务时，先检查可申请额度，再创建新的 claim 请求"""
    if not (API_KEY or SESSION_COOKIE):
        print("[Error] 请在 .env 中至少配置 TOKEN_ATLAS_SESSION 或 TOKEN_ATLAS_API_KEY，用于调用 /me 查询额度")
        sys.exit(1)
    if not SESSION_COOKIE:
        print("[Error] 请在 .env 中配置 TOKEN_ATLAS_SESSION，用于调用 /me/claim 发起申请")
        sys.exit(1)

    _, quota_remaining = await fetch_me_quota("[Me]")

    if quota_remaining <= 0:
        print("[Info] 当前没有可申请额度，跳过本次申请")
        return

    claim_count = min(CLAIM_COUNT, quota_remaining)
    print(f"[Claim] 准备申请 {claim_count} 个")
    claim_result = await asyncio.to_thread(session_claim, claim_count)

    if not claim_result:
        print("[Error] 领取请求失败")
        sys.exit(1)

    await asyncio.to_thread(save_json, claim_result, "claim")
    request_id = claim_result.get("request_id")
    if not request_id:
        print("[Error] 领取响应中缺少 request_id")
        sys.exit(1)

    print(f"[Claim] 已创建 request_id={request_id}")
    await asyncio.to_thread(update_request_status, request_id, claim_result)
    enqueue_request_if_needed(request_id, claim_result, fetch_queue, queued_ids)


async def fetch_worker(
    fetch_queue: asyncio.Queue[dict[str, Any]],
    download_queue: asyncio.Queue[dict[str, Any]],
    queued_ids: set[str],
) -> None:
    """获取队列 worker：轮询 request，成功后把下载任务丢到下载队列"""
    while True:
        job = await fetch_queue.get()
        request_id = job["request_id"]
        payload = job.get("payload") or {}

        try:
            latest_payload = payload
            latest_status = normalize_request_status(latest_payload) if latest_payload else "unknown"

            if latest_status not in TERMINAL_STATUSES:
                if not API_KEY:
                    print(f"[Warning] request_id={request_id} 缺少 API_KEY，无法轮询状态")
                    continue

                print(f"[Poll] request_id={request_id} 开始轮询")
                for attempt in range(1, POLL_MAX_ATTEMPTS + 1):
                    await asyncio.sleep(POLL_INTERVAL_SECONDS)
                    query_result = await asyncio.to_thread(get_api_claim_status, request_id)
                    if not query_result:
                        print(f"[Poll] request_id={request_id} 第 {attempt} 次查询失败")
                        continue

                    latest_payload = query_result
                    latest_status = normalize_request_status(query_result)
                    print(f"[Poll] request_id={request_id} 第 {attempt} 次状态={latest_status}")
                    await asyncio.to_thread(save_json_fixed, query_result, f"request_{request_id}")
                    await asyncio.to_thread(update_request_status, request_id, query_result)
                    if latest_status in TERMINAL_STATUSES:
                        print(f"[Poll] request_id={request_id} 已完成")
                        break
                else:
                    print(f"[Warning] request_id={request_id} 轮询超时")
                    continue
            else:
                await asyncio.to_thread(update_request_status, request_id, latest_payload)

            latest_record = await asyncio.to_thread(get_request_record, request_id)
            if latest_record:
                items = latest_record.get("items", []) or latest_payload.get("items", []) or []
                if items:
                    print(f"[Queue] request_id={request_id} 加入下载队列，items={len(items)}")
                else:
                    print(f"[Queue] request_id={request_id} 无可下载项")
                await download_queue.put({"request_id": request_id, "items": items})
        finally:
            queued_ids.discard(request_id)
            fetch_queue.task_done()


async def download_worker(download_queue: asyncio.Queue[dict[str, Any]]) -> None:
    """下载队列 worker：并发下载 token，并在整单完成后删除历史记录"""
    while True:
        job = await download_queue.get()
        request_id = job["request_id"]
        items = job.get("items", [])

        try:
            if not items:
                if await asyncio.to_thread(request_is_fully_processed, request_id):
                    await asyncio.to_thread(remove_request_record, request_id)
                continue

            for item in items:
                token_id = item.get("token_id")
                if not token_id:
                    continue

                token_record = await asyncio.to_thread(get_token_record, request_id, token_id)
                token_file_path = resolve_token_file_path(token_record)
                already_downloaded = bool(
                    token_record and token_record.get("downloaded") and token_file_path and token_file_path.exists()
                )

                if already_downloaded:
                    print(f"[Skip] request_id={request_id}, token={token_id} 已下载，跳过重复下载")
                else:
                    token_data = await asyncio.to_thread(download_token, str(token_id))
                    if not token_data:
                        print(f"[Download] request_id={request_id}, token={token_id} 下载失败，保留历史等待下次续跑")
                        continue

                    saved_path = await asyncio.to_thread(save_json, token_data, f"token_{token_id}")
                    await asyncio.to_thread(
                        update_token_record,
                        request_id,
                        token_id,
                        {
                            "downloaded": True,
                            "status": "downloaded",
                            "downloaded_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                            "file_name": saved_path.name,
                            "file_path": str(saved_path),
                        },
                    )
                    token_file_path = saved_path

                token_record = await asyncio.to_thread(get_token_record, request_id, token_id)
                already_saved = bool(token_record and token_record.get("saved_to_local"))
                if TOKEN_SAVE_DIR and token_file_path and token_file_path.exists():
                    if already_saved:
                        print(f"[Skip] request_id={request_id}, token={token_id} 已保存到本地")
                    else:
                        saved = await asyncio.to_thread(save_to_local, token_file_path)
                        if saved:
                            await asyncio.to_thread(
                                update_token_record,
                                request_id,
                                token_id,
                                {
                                    "saved_to_local": True,
                                    "saved_to_local_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                                    "status": "saved",
                                },
                            )
                        else:
                            print(f"[Save] request_id={request_id}, token={token_id} 保存失败，保留历史等待重试")

            if await asyncio.to_thread(request_is_fully_processed, request_id):
                await asyncio.to_thread(remove_request_record, request_id)
        finally:
            download_queue.task_done()


async def save_history_snapshot_if_needed() -> None:
    """按需保存当前历史文件快照到 TOKEN_SAVE_DIR"""
    if TOKEN_SAVE_DIR and HISTORY_FILE.exists():
        print("\n[Save] 开始保存历史记录到本地...")
        await asyncio.to_thread(save_to_local, HISTORY_FILE)


async def async_main() -> None:
    print(f"\n{'=' * 60}")
    print("  Token Atlas API 工具")
    print(f"  API Base: {API_BASE}")
    print(f"  Session Cookie: {'已配置' if SESSION_COOKIE else '未配置'}")
    print(f"  API Key: {'已配置' if API_KEY else '未配置'}")
    print(f"  Save Dir: {SAVE_DIR}")
    print(f"  Download Concurrency: {DOWNLOAD_CONCURRENCY}")
    print(f"{'=' * 60}\n")

    fetch_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    download_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    queued_request_ids: set[str] = set()

    await fetch_me_quota("[Startup /me]")

    pending_requests = await asyncio.to_thread(find_pending_requests)
    downloadable_requests = await asyncio.to_thread(find_downloadable_requests)

    for req in pending_requests:
        request_id = req.get("request_id")
        if request_id:
            print(f"[Resume] 恢复等待中的请求: request_id={request_id}, status={req.get('status')}")
            enqueue_request_if_needed(request_id, req, fetch_queue, queued_request_ids)

    for req in downloadable_requests:
        request_id = req.get("request_id")
        items = req.get("items", [])
        if request_id and items:
            print(f"[Resume] 恢复可下载请求: request_id={request_id}, items={len(items)}")
            await download_queue.put({"request_id": request_id, "items": items})

    fetch_task = asyncio.create_task(fetch_worker(fetch_queue, download_queue, queued_request_ids))
    download_tasks = [
        asyncio.create_task(download_worker(download_queue)) for _ in range(DOWNLOAD_CONCURRENCY)
    ]

    try:
        while True:
            if fetch_queue.empty() and download_queue.empty() and not queued_request_ids:
                pending_requests = await asyncio.to_thread(find_pending_requests)
                downloadable_requests = await asyncio.to_thread(find_downloadable_requests)

                for req in pending_requests:
                    request_id = req.get("request_id")
                    if request_id:
                        print(f"[Resume] 恢复等待中的请求: request_id={request_id}, status={req.get('status')}")
                        enqueue_request_if_needed(request_id, req, fetch_queue, queued_request_ids)

                for req in downloadable_requests:
                    request_id = req.get("request_id")
                    items = req.get("items", [])
                    if request_id and items and not await asyncio.to_thread(request_is_fully_processed, request_id):
                        print(f"[Resume] 恢复可下载请求: request_id={request_id}, items={len(items)}")
                        await download_queue.put({"request_id": request_id, "items": items})

                if fetch_queue.empty() and download_queue.empty() and not queued_request_ids:
                    _, quota_remaining = await fetch_me_quota("[Loop /me]")
                    if quota_remaining <= 0:
                        print("[Info] 当前没有可申请额度，结束本轮运行")
                        break
                    await create_new_claim_if_needed(fetch_queue, queued_request_ids)

            await asyncio.sleep(1)

        await fetch_queue.join()
        await download_queue.join()
        await save_history_snapshot_if_needed()
    finally:
        fetch_task.cancel()
        for task in download_tasks:
            task.cancel()
        await asyncio.gather(fetch_task, *download_tasks, return_exceptions=True)


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
