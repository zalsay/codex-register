#!/bin/bash
cd "$(dirname "$0")"

echo "Activating virtual environment..."
source .venv/bin/activate

echo "Running main.py..."
python main.py
