#!/bin/bash
cd "$(dirname "$0")"

# 初始化虚拟环境
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
fi

echo "Activating virtual environment..."
source .venv/bin/activate

# 安装依赖
# echo "Installing dependencies..."
# pip install --quiet curl_cffi python-dotenv

echo "Running main.py..."
python3 main.py
