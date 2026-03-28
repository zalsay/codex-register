#!/bin/bash
set -e

cd "$(dirname "$0")"

ATLAS_BIN_DIR="token_atlas/dist"
ATLAS_BIN_NAME="token-atlas"
ATLAS_BIN_DEFAULT="${ATLAS_BIN_DIR}/${ATLAS_BIN_NAME}"
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

resolve_atlas_bin() {
    local candidates=(
        "${ATLAS_BIN_DEFAULT}"
        "${ATLAS_BIN_DIR}/${ATLAS_BIN_NAME}-darwin-arm64"
        "${ATLAS_BIN_DIR}/${ATLAS_BIN_NAME}-darwin-amd64"
        "${ATLAS_BIN_DIR}/${ATLAS_BIN_NAME}-linux-amd64"
    )

    for bin in "${candidates[@]}"; do
        if [ -x "${bin}" ]; then
            printf '%s\n' "${bin}"
            return 0
        fi
    done

    return 1
}

stop_atlas() {
    echo "Stopping atlas..."
    pkill -f "python3 ${ATLAS_PYTHON_SCRIPT}" 2>/dev/null || true
    pkill -f "python ${ATLAS_PYTHON_SCRIPT}" 2>/dev/null || true
    pkill -f "${ATLAS_BIN_DEFAULT}" 2>/dev/null || true
    pkill -f "${ATLAS_BIN_DIR}/${ATLAS_BIN_NAME}-darwin-arm64" 2>/dev/null || true
    pkill -f "${ATLAS_BIN_DIR}/${ATLAS_BIN_NAME}-darwin-amd64" 2>/dev/null || true
    pkill -f "${ATLAS_BIN_DIR}/${ATLAS_BIN_NAME}-linux-amd64" 2>/dev/null || true
}

start_atlas_binary() {
    local atlas_bin
    local atlas_dir
    local atlas_log
    if ! atlas_bin="$(resolve_atlas_bin)"; then
        echo "Atlas binary not found in: ${ATLAS_BIN_DIR}"
        echo "Please build it first: make -C token_atlas build"
        echo "Or for Apple Silicon: make -C token_atlas darwin-arm64"
        exit 1
    fi

    atlas_dir="$(dirname "${atlas_bin}")"
    atlas_log="${atlas_dir}/atlas.log"

    echo "Starting atlas with ${atlas_bin}..."
    nohup "$(pwd)/${atlas_bin}" >> "${atlas_log}" 2>&1 &
    echo "atlas started, log: ${atlas_log}"
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

