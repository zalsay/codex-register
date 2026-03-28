#!/bin/bash
set -e

cd "$(dirname "$0")"

ATLAS_BIN="token_atlas/dist/token-atlas"
ATLAS_PYTHON_SCRIPT="token_atlas/token_atlas.py"

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

stop_atlas() {
    echo "Stopping atlas..."
    pkill -f "python3 ${ATLAS_PYTHON_SCRIPT}" 2>/dev/null || true
    pkill -f "python ${ATLAS_PYTHON_SCRIPT}" 2>/dev/null || true
    pkill -f "${ATLAS_BIN}" 2>/dev/null || true
}

start_atlas_binary() {
    if [ ! -x "${ATLAS_BIN}" ]; then
        echo "Atlas binary not found: ${ATLAS_BIN}"
        echo "Please build it first: make -C token_atlas build"
        exit 1
    fi

    echo "Starting atlas..."
    nohup "$(pwd)/${ATLAS_BIN}" >> "atlas.log" 2>&1 &
    echo "atlas started, log: atlas.log"
}

case "$1" in
    start|--yyds|"")
        stop_script "yyds" "yyds.py"
        start_script "yyds" "yyds.py"
        ;;
    --atlas)
        stop_atlas
        start_atlas_binary
        ;;
    stop)
        stop_script "yyds" "yyds.py"
        ;;
    stop-atlas)
        stop_atlas
        ;;
    *)
        echo "Usage: $0 [start|--yyds|--atlas|stop|stop-atlas]"
        exit 1
        ;;
esac

