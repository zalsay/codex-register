#!/usr/bin/env python3
"""
OpenAI 兼容 API Chat 测试工具
支持自定义 API Base、API Key、Model 参数
"""

import sys
import json
import time
import urllib.request
import urllib.error
from typing import Any

# ============ 配置参数 ============
API_BASE = "http://i.meetlife.top:8318/v1"  # API 地址
API_KEY = "sk-EZoBppkdFxprXuLbF9jeP76AVHjG6pRHD3M6oFvWECaECYO6"  # API Key
MODEL = "gpt-5.4"  # 模型名称
PROXY = ""  # 代理地址 (如 http://127.0.0.1:7890)
SYSTEM = "你是一个有帮助的AI助手。"  # 系统提示词
PROMPT = "Hello, who are you?"  # 用户提示词
STREAM = False  # 流式输出
TEMPERATURE = 0.7  # 温度参数
MAX_TOKENS = 1000  # 最大 token 数
TEST_TIMES = 5  # 循环测试次数
RETRY_TIMES = 3  # 重试次数
RETRY_DELAY = 1.0  # 重试间隔（秒）
# =================================


def chat_test(
    api_base: str,
    api_key: str,
    model: str,
    messages: list[dict[str, Any]],
    proxy: str | None = None,
    **kwargs: Any,
) -> dict[str, Any] | None:
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
        **kwargs,
    }

    data = json.dumps(payload).encode("utf-8")
    print(f"[Request] URL: {url}")
    print(f"[Request] Headers: {headers}")
    print(f"[Request] Body bytes: {len(data)}")
    print(f"[Request] Body: {json.dumps(payload, ensure_ascii=False)}")

    stream_enabled = bool(kwargs.get("stream"))

    for attempt in range(1, RETRY_TIMES + 1):
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")

        if proxy:
            req.set_proxy(proxy, "http")
            req.set_proxy(proxy, "https")

        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                if not stream_enabled:
                    response_body = resp.read().decode("utf-8")
                    print(f"[Response] Status: {resp.status}, Body: {response_body[:500]}")
                    result = json.loads(response_body)
                    return result

                print(f"[Response] Status: {resp.status}, Body: (streaming)")
                content = ""
                for raw_line in resp:
                    line = raw_line.decode("utf-8").strip()
                    if not line or not line.startswith("data:"):
                        continue
                    data_str = line[len("data:"):].strip()
                    if data_str == "[DONE]":
                        break

                    try:
                        event = json.loads(data_str)
                    except json.JSONDecodeError:
                        continue

                    choices = event.get("choices", [])
                    if not choices:
                        continue
                    delta = choices[0].get("delta", {})
                    text = delta.get("content", "")
                    if text:
                        print(text, end="", flush=True)
                        content += text

                print("\n")
                return {
                    "choices": [
                        {
                            "message": {
                                "role": "assistant",
                                "content": content,
                            }
                        }
                    ],
                    "usage": {},
                }
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            print(f"[Error] HTTP {e.code}: {error_body}")
        except json.JSONDecodeError as e:
            print(f"[Error] JSON 解析失败: {e}")
        except Exception as e:
            print(f"[Error] {e}")

        if attempt < RETRY_TIMES:
            print(f"[Retry] Waiting {RETRY_DELAY} seconds before retry {attempt + 1}/{RETRY_TIMES}...")
            time.sleep(RETRY_DELAY)

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
        print("[Info] 流式模式已开启，实时输出返回内容。")
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
    else:
        results: list[dict[str, Any] | None] = []
        for i in range(TEST_TIMES):
            print(f"\n[Run {i + 1}/{TEST_TIMES}]")
            result = chat_test(API_BASE, API_KEY, MODEL, messages, PROXY, **kwargs)
            results.append(result)

        print("\n" + "=" * 60)
        print(f"  Summary ({TEST_TIMES} runs):")
        print("=" * 60)

        success_count = 0
        for idx, result in enumerate(results, start=1):
            if result is None:
                print(f"[Run {idx}] Failed")
                continue

            if "choices" in result and len(result["choices"]) > 0:
                content = result["choices"][0]["message"]["content"]
                usage = result.get("usage", {})
                print(f"[Run {idx}] OK: {content}")
                if usage:
                    print(f"  Usage -> Prompt: {usage.get('prompt_tokens', 0)}, "
                          f"Completion: {usage.get('completion_tokens', 0)}, "
                          f"Total: {usage.get('total_tokens', 0)}")
                success_count += 1
            else:
                print(f"[Run {idx}] Unexpected response: {json.dumps(result, ensure_ascii=False)}")

        print(f"\n[Result] Success: {success_count}/{TEST_TIMES}")


if __name__ == "__main__":
    main()
