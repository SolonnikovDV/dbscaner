#!/bin/bash
# Запуск веб-приложения Database Object Dependency Graph Scanner

cd "$(dirname "$0")"

export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# Запуск через venv если активирован, иначе через python3
if [ -n "$VIRTUAL_ENV" ]; then
    python src/web/app.py
else
    python3 src/web/app.py
fi
