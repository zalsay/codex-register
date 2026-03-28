#!/bin/bash
set -e

cd "$(dirname "$0")"

# 初始化虚拟环境
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
fi

echo "Activating virtual environment..."
source .venv/bin/activate

stop_script() {
    local script_name="$1"
    local script_path="$2"

    echo "Stopping ${script_name}..."
    pkill -f "python3 ${script_path}" 2>/dev/null || true
    pkill -f "python ${script_path}" 2>/dev/null || true
}

start_script() {
    local script_name="$1"
    local script_path="$2"

    echo "Starting ${script_name}..."
    nohup python3 "${script_path}" >> "${script_name}.log" 2>&1 &
    echo "${script_name} started, log: ${script_name}.log"
}

case "$1" in
    start|--yyds|"")
        stop_script "yyds" "yyds.py"
        start_script "yyds" "yyds.py"
        ;;
    --atlas)
        stop_script "atlas" "token_atlas/token_atlas.py"
        start_script "atlas" "token_atlas/token_atlas.py"
        ;;
    stop)
        stop_script "yyds" "yyds.py"
        ;;
    stop-atlas)
        stop_script "atlas" "token_atlas/token_atlas.py"
        ;;
    *)
        echo "Usage: $0 [start|--yyds|--atlas|stop|stop-atlas]"
        exit 1
        ;;
esac
