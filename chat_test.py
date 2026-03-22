#!/usr/bin/env python3
"""
OpenAI 兼容 API Chat 测试工具
支持自定义 API Base、API Key、Model 参数
"""

import sys
import json
import urllib.request
import urllib.error

# ============ 配置参数 ============
API_BASE = "http://ts.meetlife.top:8317/v1"  # API 地址
API_KEY = "sk-CJjRBm1rOkCOg3IYQ"  # API Key
MODEL = "gpt-5.4"  # 模型名称
PROXY = ""  # 代理地址 (如 http://127.0.0.1:7890)
SYSTEM = "你是一个有帮助的AI助手。"  # 系统提示词
PROMPT = "Hello, who are you?"  # 用户提示词
STREAM = False  # 流式输出
TEMPERATURE = 0.7  # 温度参数
MAX_TOKENS = 1000  # 最大 token 数
# =================================


def chat_test(api_base: str, api_key: str, model: str, messages: list, proxy: str = None, **kwargs):
    """测试 OpenAI 兼容 API"""
    import urllib.request
    import urllib.error

    url = f"{api_base.rstrip('/')}/chat/completions"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    payload = {
        "model": model,
        "messages": messages,
        **kwargs
    }

    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(url, data=data, headers=headers, method="POST")

    if proxy:
        req.set_proxy(proxy, "http")
        req.set_proxy(proxy, "https")

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            response_body = resp.read().decode("utf-8")
            print(f"[Response] Status: {resp.status}, Body: {response_body[:500]}")
            result = json.loads(response_body)
            return result
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8") if e.fp else ""
        print(f"[Error] HTTP {e.code}: {error_body}")
        return None
    except json.JSONDecodeError as e:
        print(f"[Error] JSON 解析失败: {e}")
        return None
    except Exception as e:
        print(f"[Error] {e}")
        return None


def main():
    if not API_KEY:
        print("[Error] 请在文件顶部配置 API_KEY")
        sys.exit(1)

    messages = [
        {"role": "system", "content": SYSTEM},
        {"role": "user", "content": PROMPT}
    ]

    print(f"\n{'='*60}")
    print(f"  API: {API_BASE}")
    print(f"  Model: {MODEL}")
    print(f"  Proxy: {PROXY or '无'}")
    print(f"{'='*60}\n")

    kwargs = {
        "temperature": TEMPERATURE,
        "max_tokens": MAX_TOKENS,
    }

    if STREAM:
        kwargs["stream"] = True
        result = chat_test(API_BASE, API_KEY, MODEL, messages, PROXY, **kwargs)
        if result is None:
            sys.exit(1)
        print("[Stream Response]")
        for line in result:
            if line.get("choices"):
                delta = line["choices"][0].get("delta", {})
                if delta.get("content"):
                    print(delta["content"], end="", flush=True)
        print("\n")
    else:
        result = chat_test(API_BASE, API_KEY, MODEL, messages, PROXY, **kwargs)
        if result is None:
            sys.exit(1)

        print("=" * 60)
        print("  Response:")
        print("=" * 60)

        if "choices" in result and len(result["choices"]) > 0:
            content = result["choices"][0]["message"]["content"]
            print(content)

            usage = result.get("usage", {})
            if usage:
                print(f"\n[Usage] Prompt: {usage.get('prompt_tokens', 0)}, "
                      f"Completion: {usage.get('completion_tokens', 0)}, "
                      f"Total: {usage.get('total_tokens', 0)}")
        else:
            print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
