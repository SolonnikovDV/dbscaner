"""Test scanner functionality."""
import psycopg2
from src.db_scanner.scanner_test import DBScanner
from db_scanner.models import DBObject, ObjectType

# Параметры подключения
# Import database configuration from config.py
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.config_test import DB_CONFIG

def check_schema_exists(conn, schema_name):
    """Проверить существование схемы."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS(
                SELECT 1 FROM information_schema.schemata 
                WHERE schema_name = %s
            )
        """, (schema_name,))
        exists = cur.fetchone()[0]
        print(f"Схема {schema_name} {'существует' if exists else 'не существует'}")
        return exists

def check_view_exists(conn, schema_name, view_name):
    """Проверить существование представления."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS(
                SELECT 1 
                FROM information_schema.views
                WHERE table_schema = %s AND table_name = %s
            )
        """, (schema_name, view_name))
        exists = cur.fetchone()[0]
        print(f"Представление {schema_name}.{view_name} {'существует' if exists else 'не существует'}")
        return exists

def test_object(scanner: DBScanner, name: str, schema: str, obj_type: ObjectType):
    """Test scanner functionality for a specific object."""
    print(f"\n{'='*50}")
    print(f"Тестирование объекта {schema}.{name} ({obj_type})")
    print('='*50)
    
    obj = DBObject(
        name=name,
        schema=schema,
        obj_type=obj_type,
        definition=""
    )
    
    # Получение определения объекта
    print("\nПолучение определения объекта...")
    definition = scanner.get_object_definition(obj)
    if definition:
        print("Определение:")
        print("-" * 30)
        print(definition)
        print("-" * 30)
    else:
        print("Определение не найдено")
    
    # Поиск связанных объектов
    print("\nПоиск связанных объектов...")
    related = scanner.find_related_objects(obj)
    
    if related:
        print(f"Найдено связанных объектов: {len(related)}")
        
        # Группировка по глубине зависимости
        by_depth = {}
        for rel in related:
            if rel.depth not in by_depth:
                by_depth[rel.depth] = []
            by_depth[rel.depth].append(rel)
        
        # Вывод по уровням глубины
        for depth in sorted(by_depth.keys()):
            if depth == 1:
                print("\nПрямые зависимости:")
            else:
                print(f"\nКосвенные зависимости (уровень {depth}):")
            
            for rel in by_depth[depth]:
                target = rel.target
                print(f"- {target.obj_type}: {target.schema}.{target.name} ({rel.relationship_type})")
    else:
        print("Связанные объекты не найдены")

def main():
    """Main test function."""
    try:
        # Подключение к БД
        conn = psycopg2.connect(**DB_CONFIG)
        print("Подключение к БД установлено")

        # Список объектов для тестирования
        test_objects = [
            # Многоуровневые зависимости
            ('active_high_value_customers', 'test_graph', ObjectType.VIEW),
            ('yearly_product_summary', 'test_graph', ObjectType.MATERIALIZED_VIEW),
            ('calculate_customer_average', 'test_graph', ObjectType.FUNCTION),
            ('update_product_stats', 'test_graph', ObjectType.FUNCTION),
            
            # Сложные зависимости
            ('high_value_orders', 'test_graph', ObjectType.VIEW),
            ('get_top_products_by_month', 'test_graph', ObjectType.FUNCTION),
            
            # Базовые представления
            ('order_summary', 'test_graph', ObjectType.VIEW),
            ('active_users', 'test_graph', ObjectType.VIEW),
            ('monthly_sales', 'test_graph', ObjectType.MATERIALIZED_VIEW),
            
            # Таблицы
            ('orders', 'test_graph', ObjectType.TABLE),
            ('order_items', 'test_graph', ObjectType.TABLE),
            ('users', 'test_graph', ObjectType.TABLE),
            ('products', 'test_graph', ObjectType.TABLE),
            
            # Функции
            ('calculate_order_total', 'test_graph', ObjectType.FUNCTION),
            ('update_stock_quantity', 'test_graph', ObjectType.FUNCTION),
        ]
        
        # Проверка существования схемы
        schema_exists = check_schema_exists(conn, "test_graph")
        if not schema_exists:
            print("ОШИБКА: Схема test_graph не существует!")
            return
        
        # Создание сканера
        scanner = DBScanner(conn)
        
        # Запуск тестов
        for name, schema, obj_type in test_objects:
            test_object(scanner, name, schema, obj_type)
        
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        conn.close()
        print("\nПодключение закрыто")

if __name__ == '__main__':
    main()