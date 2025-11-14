#!/bin/bash
# Запуск веб-приложения Database Object Dependency Graph Scanner

# Добавление текущей директории в PYTHONPATH для корректного импорта модулей
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# Запуск веб-приложения через python
python src/web/app.py