# DB Graph Explorer

Инструмент для анализа и визуализации **графа зависимостей** и **data lineage** объектов PostgreSQL / Greenplum. Позволяет исследовать взаимосвязи между таблицами, представлениями и функциями, отслеживать потоки данных на уровне колонок, симулировать impact при изменениях схемы и управлять несколькими подключениями к базам данных.

## Автор

**Dmitry Solonnikov**

- Telegram: [@Dmitry_as_SoloD](https://t.me/Dmitry_as_SoloD)

## Лицензия и авторские права

- Лицензия: **MIT** — см. [LICENSE](LICENSE)
- Copyright © 2025 **Dmitry Solonnikov**
- Подробности об авторских правах: [COPYRIGHT.md](COPYRIGHT.md)

## Возможности

### Граф зависимостей объектов

- Рекурсивный анализ **upstream** (кто использует объект) и **downstream** (от чего зависит объект)
- Поддержка TABLE, VIEW, MATERIALIZED VIEW, FUNCTION (включая `RETURNS TABLE` в Greenplum)
- Визуализация через D3.js + dagre с интерактивным zoom/pan
- Различение типов связей: `depends_on`, `used_by`, циклы, deep-scan
- Фильтрация шумовых объектов (`_log`, `_audit`, `_history`, `_archive`, `_tmp`)
- Deep Scan — полнотекстовый поиск cross-schema ссылок в телах функций
- Обнаружение циклических зависимостей (NetworkX)

### Column-level lineage

- Парсинг lineage колонок через `sqllineage` для VIEW, MATERIALIZED VIEW, FUNCTION
- Fallback через `pg_depend` при неразбираемом SQL
- Отображение lineage в Inspector → вкладка Columns

### Attribute alias tracking

- Трассировка эволюции имён атрибутов по цепочке объектов (BFS)
- Визуализация rename-точек на графе и в timeline-панели
- Pipeline view — полный upstream → downstream flow витрины

### Impact analysis

- Симуляция drop/rename колонки с классификацией **breaking** (подтверждено lineage) / **warning** (text search)
- Object-level impact — транзитивные upstream-зависимости при удалении объекта
- Маркеры impact на узлах графа

### Веб-интерфейс (macOS-style)

- Трёхпанельный layout: sidebar объектов, граф, inspector (Info / DDL / Columns / Pipeline / Impact)
- Nav-rail со страницами: **Graph**, **Instances**, **About**
- Глобальный поиск по объектам и колонкам (⌘K)
- Кликабельные `schema.object` ссылки в DDL с подсветкой
- Экспорт графа в JSON и PNG
- Управление глубиной, направлением и фильтрами из status bar

### Управление инстансами БД

- Несколько профилей подключения (host, port, database, user, password)
- Переключение активного инстанса без перезапуска приложения
- Test connection перед сохранением
- Конфигурация хранится локально в `data/instances.json` (не коммитится)

## Быстрый старт

### 1. Клонирование и установка

```bash
git clone <repository-url>
cd dbscaner
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Настройка подключения

**Вариант A — через UI (рекомендуется):** запустите приложение и настройте инстанс на странице **Instances**.

**Вариант B — через config:** отредактируйте `src/config.py` (используется для seed первого инстанса):

```python
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'postgres',
    'user': 'gpadmin',
    'password': 'gpadmin'
}
```

Для тестов: `src/config_test.py`.

### 3. Запуск

```bash
./run_web.sh
```

Откройте в браузере: **http://127.0.0.1:5001**

> Порт 5001 выбран по умолчанию, т.к. на macOS порт 5000 часто занят AirPlay Receiver.

## Использование

1. Выберите активный инстанс на странице **Instances** (или используйте Default)
2. На странице **Graph** выберите схему и объект в sidebar (группы по типам свёрнуты)
3. Изучите граф зависимостей; клик по узлу открывает Inspector
4. Вкладка **Columns** — lineage и trace alias evolution
5. **Simulate drop** — impact analysis для колонки
6. **Pipeline** — полный data flow объекта
7. Глобальный поиск ⌘K — быстрый переход к объекту или колонке

## REST API

| Endpoint | Описание |
|----------|----------|
| `GET /schemas` | Список схем |
| `GET /objects/<schema>` | Объекты в схеме |
| `GET /graph/<schema>/<object>` | Данные графа зависимостей |
| `GET /ddl/<schema>/<object>` | DDL объекта |
| `GET /search?q=...` | Поиск по объектам и колонкам |
| `GET /metadata/<schema>/<object>` | Метаданные объекта |
| `GET /columns/<schema>/<object>` | Column lineage |
| `GET /attribute/<schema>/<object>/<column>` | Alias evolution trace |
| `GET /impact/<schema>/<object>` | Object drop impact |
| `GET /impact/<schema>/<object>/column/<col>` | Column drop impact |
| `GET /pipeline/<schema>/<object>` | Pipeline view |
| `GET /api/instances` | Список инстансов |
| `POST /api/instances/<id>/activate` | Переключить инстанс |
| `GET /api/about` | Информация о приложении |

## Структура проекта

```
dbscaner/
├── src/
│   ├── config.py              # Seed-конфиг БД
│   ├── instance_store.py      # Хранение профилей подключений
│   ├── db/
│   │   └── connection.py      # Connection pool (psycopg2)
│   ├── db_scanner/
│   │   ├── scanner_recursive.py  # Рекурсивный сканер + graph builder
│   │   ├── column_lineage.py     # Column-level lineage (sqllineage)
│   │   ├── alias_tracker.py      # Attribute alias evolution
│   │   └── models.py
│   └── web/
│       ├── app.py             # Flask REST API
│       └── templates/index.html
├── data/
│   └── instances.json         # Локальные профили БД (gitignored)
├── tests/
├── requirements.txt
├── run_web.sh
├── LICENSE
└── COPYRIGHT.md
```

## Зависимости

| Пакет | Назначение |
|-------|-----------|
| flask | Веб-фреймворк |
| psycopg2-binary | PostgreSQL / Greenplum |
| networkx | Графы, cycle detection |
| sqllineage | Column-level SQL lineage |
| sqlparse | SQL parsing |
| matplotlib, PyQt5 | Legacy desktop GUI |
| pytest | Тестирование |

## Совместимость

- **PostgreSQL** 9.4+
- **Greenplum** — учтены особенности (`UNION ALL` reorder, отсутствие `p.prokind`, `relkind='f'` для RETURNS TABLE functions)

## Тестирование

```bash
pytest tests/
```

Тестовые SQL-скрипты: `sql/test_objects/`

## Десктопное приложение (legacy)

```bash
python src/main.py
```

PyQt5 GUI — список объектов, граф, DDL. Основной режим разработки — **веб-приложение**.
