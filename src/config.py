# Database configuration for PostgreSQL (Production)
# IMPORTANT: Replace these values with your actual production database credentials
DB_CONFIG = {
    'host': 'your_host',          # Database host
    'port': '5432',               # Database port
    'database': 'your_db',        # Database name
    'user': 'your_user',          # Database user
    'password': 'your_pass'       # Database password
}

# Настройки визуализации графа (Production)
GRAPH_CONFIG = {
    'node_size': 2000,
    'font_size': 8,
    'arrow_size': 20,
    'width': 1600,
    'height': 900
}

# Цвета для разных типов объектов (Production)
OBJECT_COLORS = {
    'table': '#4CAF50',      # Зеленый
    'view': '#2196F3',       # Синий
    'function': '#FFC107',   # Желтый
    'type': '#9C27B0',       # Фиолетовый
    'sequence': '#FF5722'    # Оранжевый
}