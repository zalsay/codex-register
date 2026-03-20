import json
import os
import re
import time
import uuid
import random
import string
import secrets
import hashlib
import base64
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Any, Dict, Optional
import urllib.parse
import urllib.request
import urllib.error

from dotenv import load_dotenv
from curl_cffi import requests
from curl_cffi.requests import Session

# 加载 .env 环境变量
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

# 配置输出目录和请求UA
OUT_DIR = Path(__file__).parent.resolve()
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# ========== 1. DuckMail 临时邮箱处理模块 ==========

DUCKMAIL_API_BASE = os.environ.get("DUCKMAIL_API_BASE", "https://api.duckmail.sbs")
DUCKMAIL_BEARER = os.environ.get("DUCKMAIL_BEARER", "")

def _duckmail_session(proxies=None):
    """创建 DuckMail 请求会话"""
    session = Session(proxies=proxies)
    session.headers.update({
        "User-Agent": UA,
        "Accept": "application/json",
        "Content-Type": "application/json",
    })
    return session

def _create_duckmail_email(proxies=None):
    """创建 DuckMail 临时邮箱，返回 (email, password, mail_token)"""
    if not DUCKMAIL_BEARER:
        print("  [!] DUCKMAIL_BEARER 未设置")
        return None, None, None

    chars = string.ascii_lowercase + string.digits
    length = random.randint(8, 13)
    email_local = "".join(random.choice(chars) for _ in range(length))
    email = f"{email_local}@duckmail.sbs"
    password = _gen_password()

    api_base = DUCKMAIL_API_BASE.rstrip("/")
    headers = {"Authorization": f"Bearer {DUCKMAIL_BEARER}"}
    session = _duckmail_session(proxies)

    try:
        # 创建账号
        res = session.post(
            f"{api_base}/accounts",
            json={"address": email, "password": password},
            headers=headers,
            timeout=15,
        )
        if res.status_code not in [200, 201]:
            print(f"  [!] 创建邮箱失败: {res.status_code} - {res.text[:200]}")
            return None, None, None

        # 获取 Token
        time.sleep(0.5)
        token_res = session.post(
            f"{api_base}/token",
            json={"address": email, "password": password},
            timeout=15,
        )
        if token_res.status_code == 200:
            token_data = token_res.json()
            mail_token = token_data.get("token")
            if mail_token:
                return email, password, mail_token

        print(f"  [!] 获取邮件 Token 失败: {token_res.status_code}")
        return None, None, None
    except Exception as e:
        print(f"  [!] DuckMail 创建邮箱异常: {e}")
        return None, None, None

def _fetch_duckmail_messages(mail_token, proxies=None):
    """从 DuckMail 获取邮件列表"""
    try:
        api_base = DUCKMAIL_API_BASE.rstrip("/")
        headers = {"Authorization": f"Bearer {mail_token}"}
        session = _duckmail_session(proxies)

        res = session.get(
            f"{api_base}/messages",
            headers=headers,
            timeout=15,
        )
        if res.status_code == 200:
            data = res.json()
            messages = data.get("hydra:member") or data.get("member") or data.get("data") or []
            return messages
        return []
    except:
        return []

def _fetch_duckmail_email_detail(mail_token, msg_id, proxies=None):
    """获取 DuckMail 单封邮件详情"""
    try:
        api_base = DUCKMAIL_API_BASE.rstrip("/")
        headers = {"Authorization": f"Bearer {mail_token}"}
        session = _duckmail_session(proxies)

        if isinstance(msg_id, str) and msg_id.startswith("/messages/"):
            msg_id = msg_id.split("/")[-1]

        res = session.get(
            f"{api_base}/messages/{msg_id}",
            headers=headers,
            timeout=15,
        )
        if res.status_code == 200:
            return res.json()
    except:
        pass
    return None

def _extract_verification_code(email_content):
    """从邮件内容提取 6 位验证码"""
    if not email_content:
        return None
    patterns = [
        r"Verification code:?\s*(\d{6})",
        r"code is\s*(\d{6})",
        r"代码为[:：]?\s*(\d{6})",
        r"验证码[:：]?\s*(\d{6})",
        r">\s*(\d{6})\s*<",
        r"(?<![#&])\b(\d{6})\b",
    ]
    for pattern in patterns:
        matches = re.findall(pattern, email_content, re.IGNORECASE)
        for code in matches:
            if code == "177010":
                continue
            return code
    return None

def _getotp_duckmail(mail_token, proxies=None):
    """从 DuckMail 获取验证码 (最多等待约8分钟)"""
    for _ in range(60):
        messages = _fetch_duckmail_messages(mail_token, proxies)
        if messages and len(messages) > 0:
            first_msg = messages[0]
            msg_id = first_msg.get("id") or first_msg.get("@id")
            if msg_id:
                detail = _fetch_duckmail_email_detail(mail_token, msg_id, proxies)
                if detail:
                    content = detail.get("text") or detail.get("html") or ""
                    code = _extract_verification_code(content)
                    if code:
                        return code
        time.sleep(8)
    return None

def setup_duckmail(proxies=None):
    """创建 DuckMail 邮箱并返回所需数据"""
    print("  [*] 创建 DuckMail 临时邮箱...")
    email, email_pwd, mail_token = _create_duckmail_email(proxies)
    if not email or not mail_token:
        print("  [!] DuckMail 邮箱创建失败")
        return None, None, None

    openai_password = _gen_password()

    def fetch_code():
        print("  [*] 正在等待验证码 (最多等待约8分钟)...")
        return _getotp_duckmail(mail_token, proxies)

    return email, openai_password, fetch_code


# ========== 2. OpenAI OAuth2 授权与环境生成模块 ==========

AUTH_URL = "https://auth.openai.com/oauth/authorize"
TOKEN_URL = "https://auth.openai.com/oauth/token"
CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"
DEFAULT_REDIRECT_URI = "http://localhost:1455/auth/callback"
DEFAULT_SCOPE = "openid email profile offline_access"

def _gen_password() -> str:
    alphabet = string.ascii_letters + string.digits
    special = "!@#$%^&*.-"
    base = [
        random.choice(string.ascii_lowercase),
        random.choice(string.ascii_uppercase),
        random.choice(string.digits),
        random.choice(special),
    ]
    base += [random.choice(alphabet + special) for _ in range(12)]
    random.shuffle(base)
    return "".join(base)

def _random_name() -> str:
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(random.randint(5, 9))).capitalize()

def _random_birthdate() -> str:
    start = datetime(1970,1,1)
    end = datetime(1999,12,31)
    d = start + timedelta(days=random.randrange((end - start).days + 1))
    return d.strftime('%Y-%m-%d')

def _b64url_no_pad(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

def _sha256_b64url_no_pad(s: str) -> str:
    return _b64url_no_pad(hashlib.sha256(s.encode("ascii")).digest())

def _random_state(nbytes: int = 16) -> str:
    return secrets.token_urlsafe(nbytes)

def _pkce_verifier() -> str:
    return secrets.token_urlsafe(64)

def _parse_callback_url(callback_url: str) -> Dict[str, Any]:
    candidate = callback_url.strip()
    if not candidate:
        return {"code": "","state": "","error": "","error_description": ""}
    if "://" not in candidate:
        if candidate.startswith("?"): candidate = f"http://localhost{candidate}"
        elif any(ch in candidate for ch in "/?#") or ":" in candidate: candidate = f"http://{candidate}"
        elif "=" in candidate: candidate = f"http://localhost/?{candidate}"
    parsed = urllib.parse.urlparse(candidate)
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    fragment = urllib.parse.parse_qs(parsed.fragment, keep_blank_values=True)
    for key, values in fragment.items():
        if key not in query or not query[key] or not (query[key][0] or "").strip():
            query[key] = values
    def get1(k: str) -> str:
        v = query.get(k, [""])
        return (v[0] or "").strip()
    code = get1("code"); state = get1("state")
    error = get1("error"); error_description = get1("error_description")
    if code and not state and "#" in code:
        code, state = code.split("#",1)
    if not error and error_description:
        error, error_description = error_description, ""
    return {"code": code,"state": state,"error": error,"error_description": error_description}

def _jwt_claims_no_verify(id_token: str) -> Dict[str, Any]:
    if not id_token or id_token.count(".") < 2: return {}
    payload_b64 = id_token.split(".")[1]
    pad = "=" * ((4 - (len(payload_b64) % 4)) % 4)
    try:
        return json.loads(base64.urlsafe_b64decode((payload_b64 + pad).encode("ascii")).decode("utf-8"))
    except: return {}

def _decode_jwt_segment(seg: str) -> Dict[str, Any]:
    raw = (seg or "").strip()
    if not raw: return {}
    pad = "=" * ((4 - (len(raw) % 4)) % 4)
    try: return json.loads(base64.urlsafe_b64decode((raw + pad).encode("ascii")).decode("utf-8"))
    except: return {}

def _to_int(v: Any) -> int:
    try: return int(v)
    except: return 0

def _post_form(url: str, data: Dict[str, str], timeout: int = 30) -> Dict[str, Any]:
    body = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded","Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            if resp.status != 200: raise RuntimeError(f"token exchange failed: {resp.status}")
            return json.loads(raw.decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"token exchange failed: {exc.code}") from exc

@dataclass(frozen=True)
class OAuthStart:
    auth_url: str
    state: str
    code_verifier: str
    redirect_uri: str

def generate_oauth_url(*, redirect_uri: str = DEFAULT_REDIRECT_URI, scope: str = DEFAULT_SCOPE) -> OAuthStart:
    state = _random_state()
    code_verifier = _pkce_verifier()
    code_challenge = _sha256_b64url_no_pad(code_verifier)
    params = {
        "client_id": CLIENT_ID, "response_type": "code", "redirect_uri": redirect_uri,
        "scope": scope, "state": state, "code_challenge": code_challenge,
        "code_challenge_method": "S256", "prompt": "login",
        "id_token_add_organizations": "true", "codex_cli_simplified_flow": "true",
    }
    auth_url = f"{AUTH_URL}?{urllib.parse.urlencode(params)}"
    return OAuthStart(auth_url=auth_url, state=state, code_verifier=code_verifier, redirect_uri=redirect_uri)

def fetch_sentinel_token(*, flow: str, did: str, proxies: Any = None) -> Optional[str]:
    """获取 OpenAI 最新的反爬 Token (Sentinel)"""
    try:
        body = json.dumps({"p": "", "id": did, "flow": flow})
        resp = requests.post(
            "https://sentinel.openai.com/backend-api/sentinel/req",
            headers={
                "origin": "https://sentinel.openai.com",
                "referer": "https://sentinel.openai.com/backend-api/sentinel/frame.html?sv=20260219f9f6",
                "content-type": "text/plain;charset=UTF-8",
                "user-agent": UA
            },
            data=body, proxies=proxies, impersonate="chrome120", timeout=15,
        )
        if resp.status_code != 200: return None
        return resp.json().get("token")
    except: return None

def submit_callback_url(*, callback_url: str, expected_state: str, code_verifier: str, redirect_uri: str = DEFAULT_REDIRECT_URI) -> str:
    """提取重定向中的 Code 并换取最终的 Access / Refresh Token"""
    cb = _parse_callback_url(callback_url)
    if cb["error"]: raise RuntimeError(f"oauth error: {cb['error']}")
    if not cb["code"] or not cb["state"]: raise ValueError("callback missing code/state")
    if cb["state"] != expected_state: raise ValueError("state mismatch")

    token_resp = _post_form(TOKEN_URL, {
        "grant_type": "authorization_code", "client_id": CLIENT_ID,
        "code": cb["code"], "redirect_uri": redirect_uri, "code_verifier": code_verifier,
    })
    
    access_token = (token_resp.get("access_token") or "").strip()
    refresh_token = (token_resp.get("refresh_token") or "").strip()
    id_token = (token_resp.get("id_token") or "").strip()
    expires_in = _to_int(token_resp.get("expires_in"))

    claims = _jwt_claims_no_verify(id_token)
    email = str(claims.get("email") or "").strip()
    auth_claims = claims.get("https://api.openai.com/auth") or {}
    account_id = str(auth_claims.get("chatgpt_account_id") or "").strip()

    now = int(time.time())
    expired_rfc3339 = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now + max(expires_in, 0)))
    now_rfc3339 = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now))

    config = {
        "id_token": id_token, "access_token": access_token, "refresh_token": refresh_token,
        "account_id": account_id, "last_refresh": now_rfc3339, "email": email,
        "type": "codex", "expired": expired_rfc3339,
    }
    return json.dumps(config, ensure_ascii=False, separators=(",", ":"))


# ========== 3. 核心注册与提取流程 ==========

def run(proxy: Optional[str]) -> Optional[tuple[str, str, str]]:
    proxies = {"http": proxy, "https": proxy} if proxy else None
    s = requests.Session(proxies=proxies, impersonate="chrome120")
    s.headers.update({"user-agent": UA})

    print(f"[*] 初始化请求，准备获取临时邮箱...")
    mail_data = setup_duckmail(proxies)
    if not mail_data or not mail_data[0]:
        print("[Error] 获取 DuckMail 邮箱失败")
        return None
        
    email, password, code_fetcher = mail_data
    print(f"[*] 成功获取邮箱: {email}")
    print(f"[*] 生成高强度密码: {password}")

    oauth = generate_oauth_url()
    
    try:
        # 第一步：进入 OAuth
        resp = s.get(oauth.auth_url, timeout=15)
        did = s.cookies.get("oai-did")
        if not did:
            print("[Error] 未能获取到 OpenAI Device ID (oai-did)")
            return None

        # 第二步：获取 Sentinel Token (authorize_continue)
        sen_token = fetch_sentinel_token(flow="authorize_continue", did=did, proxies=proxies)
        sentinel = json.dumps({"p": "", "t": "", "c": sen_token, "id": did, "flow": "authorize_continue"}) if sen_token else None

        # 第三步：获取 Sentinel SO Token (oauth_create_account)
        so_token = fetch_sentinel_token(flow="oauth_create_account", did=did, proxies=proxies)

        # 第四步：提交邮箱授权
        signup_headers = {"referer": "https://auth.openai.com/create-account", "accept": "application/json", "content-type": "application/json"}
        if sentinel: signup_headers["openai-sentinel-token"] = sentinel
        signup_resp = s.post("https://auth.openai.com/api/accounts/authorize/continue", headers=signup_headers, data=json.dumps({"username": {"value": email, "kind": "email"}, "screen_hint": "signup"}))
        if signup_resp.status_code != 200:
            print(f"[Error] 提交邮箱失败: {signup_resp.status_code}")
            print(f"[Error] 响应内容: {signup_resp.text[:500]}")
            return None

        # 第五步：设置密码
        register_headers = {"referer": "https://auth.openai.com/create-account/password", "accept": "application/json", "content-type": "application/json"}
        if sentinel: register_headers["openai-sentinel-token"] = sentinel
        reg_resp = s.post("https://auth.openai.com/api/accounts/user/register", headers=register_headers, data=json.dumps({"password": password, "username": email}))
        if reg_resp.status_code != 200:
            print(f"[Error] 设置密码失败: {reg_resp.status_code}")
            return None

        # 第六步：触发并提取验证码
        s.get("https://auth.openai.com/api/accounts/email-otp/send", headers=register_headers, timeout=15)
        code = code_fetcher()
        if not code:
            print("[Error] 验证码等待超时或提取失败")
            return None
        print(f"[*] 成功提取验证码: {code}")

        # 第七步：校验验证码
        validate_headers = {"referer": "https://auth.openai.com/email-verification", "accept": "application/json", "content-type": "application/json"}
        if sentinel: validate_headers["openai-sentinel-token"] = sentinel
        code_resp = s.post("https://auth.openai.com/api/accounts/email-otp/validate", headers=validate_headers, data=json.dumps({"code": code}))
        if code_resp.status_code != 200:
            print(f"[Error] 验证码校验失败: {code_resp.status_code}")
            return None

        # 第八步：完成账号注册填写
        create_headers = {"referer": "https://auth.openai.com/about-you", "accept": "application/json", "content-type": "application/json"}
        if so_token: create_headers["openai-sentinel-so-token"] = so_token
        create_resp = s.post("https://auth.openai.com/api/accounts/create_account", headers=create_headers, data=json.dumps({"name": _random_name(), "birthdate": _random_birthdate()}))
        if create_resp.status_code != 200:
            print(f"[Error] 账户信息填写失败: {create_resp.status_code}")
            print(f"[Error] 响应内容: {create_resp.text[:500]}")
            return None

        # 第九步：选择工作区 Workspace
        auth_cookie = s.cookies.get("oai-client-auth-session")
        if not auth_cookie: return None
        auth_json = _decode_jwt_segment(auth_cookie.split(".")[0])
        workspace_id = str((auth_json.get("workspaces") or [{}])[0].get("id") or "").strip()
        
        select_resp = s.post("https://auth.openai.com/api/accounts/workspace/select", headers={"referer": "https://auth.openai.com/sign-in-with-chatgpt/codex/consent", "content-type": "application/json"}, data=json.dumps({"workspace_id": workspace_id}))
        if select_resp.status_code != 200: return None
        
        continue_url = str((select_resp.json() or {}).get("continue_url") or "").strip()

        # 第十步：拦截重定向，提取终极 Token
        current_url = continue_url
        for _ in range(6):
            final_resp = s.get(current_url, allow_redirects=False, timeout=15)
            location = final_resp.headers.get("Location") or ""
            if final_resp.status_code not in [301, 302, 303, 307, 308] or not location:
                break
            next_url = urllib.parse.urljoin(current_url, location)
            if "code=" in next_url and "state=" in next_url:
                token_json = submit_callback_url(callback_url=next_url, code_verifier=oauth.code_verifier, redirect_uri=oauth.redirect_uri, expected_state=oauth.state)
                return token_json, email, password
            current_url = next_url

        print("[Error] 未能在重定向链中捕获到最终 Token")
        return None

    except Exception as e:
        print(f"[Error] 运行时异常: {e}")
        return None


# ========== 4. 主程序轮询与保存 ==========

def main():
    parser = argparse.ArgumentParser(description="OpenAI 完美融合自动化注册脚本 (By Gemini)")
    parser.add_argument("--proxy", default=None, help="代理地址，如 http://127.0.0.1:7890")
    parser.add_argument("--once", action="store_true", help="只运行一次")
    args = parser.parse_args()

    count = 0
    print("========================================")
    print("🚀 OpenAI 终极注册机 (带 Token 提取及 DuckMail) ")
    print("========================================")
    
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    while True:
        count += 1
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] >>> 开始第 {count} 次注册流程 <<<")
        run_result = run(args.proxy)
        
        if run_result:
            token_json, email, password = run_result
            fname_email = email.replace("@", "_")

            # 保存机制 1：单独保存 Token JSON 文件
            tokens_dir = OUT_DIR / "tokens"
            tokens_dir.mkdir(parents=True, exist_ok=True)
            file_path = tokens_dir / f"token_{fname_email}_{int(time.time())}.json"
            file_path.write_text(token_json, encoding="utf-8")
            print(f"[🎉] 成功获取 Token！已保存至: {file_path}")

            # 保存机制 2：汇总账号密码信息
            acc_file = tokens_dir / "accounts.txt"
            with open(acc_file, "a", encoding="utf-8") as f:
                f.write(f"{email}----{password}\n")
            print(f"[📝] 账号已追加至: {acc_file}")
            
        else:
            print("[-] 本次注册流程断开。")

        if args.once:
            break
            
        wait_time = random.randint(5, 15)
        print(f"[*] 冷却 {wait_time} 秒...")
        time.sleep(wait_time)

if __name__ == "__main__":
    main()
