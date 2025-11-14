"""Test scanner functionality."""
import psycopg2
from src.db_scanner.scanner_new import DBScanner  # Используем новую версию сканера
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

def test_object(scanner: DBScanner, name: str, schema: str, obj_type: ObjectType):
    """Test scanner functionality for a specific object."""
    print(f"\n{'='*80}")
    print(f"Тестирование объекта {schema}.{name} ({obj_type})")
    print('='*80)
    
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
        print("-" * 50)
        print(definition)
        print("-" * 50)
    else:
        print("Определение не найдено")
    
    # Поиск связанных объектов
    print("\nПоиск связанных объектов...")
    related = scanner.find_related_objects(obj)
    
    if related:
        # Разделение по типу зависимости
        depends_on = []
        used_by = []
        
        for rel in related:
            if rel.relationship_type == 'depends_on':
                depends_on.append(rel)
            else:
                used_by.append(rel)
        
        # Вывод зависимостей
        if depends_on:
            print("\nОбъект зависит от:")
            print("-" * 30)
            for rel in depends_on:
                target = rel.target
                print(f"- {target.obj_type}: {target.schema}.{target.name}")
        
        if used_by:
            print("\nОбъект используется в:")
            print("-" * 30)
            for rel in used_by:
                target = rel.target
                print(f"- {target.obj_type}: {target.schema}.{target.name}")
                
        print(f"\nВсего найдено связанных объектов: {len(related)}")
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
            # Сложные представления с множественными зависимостями
            ('active_high_value_customers', 'test_graph', ObjectType.VIEW),
            ('yearly_product_summary', 'test_graph', ObjectType.MATERIALIZED_VIEW),
            
            # Функции, работающие с представлениями
            ('calculate_customer_average', 'test_graph', ObjectType.FUNCTION),
            ('get_top_products_by_month', 'test_graph', ObjectType.FUNCTION),
            
            # Базовые представления
            ('order_summary', 'test_graph', ObjectType.VIEW),
            ('monthly_sales', 'test_graph', ObjectType.MATERIALIZED_VIEW),
            
            # Базовые таблицы
            ('orders', 'test_graph', ObjectType.TABLE),
            ('users', 'test_graph', ObjectType.TABLE),
            ('products', 'test_graph', ObjectType.TABLE),
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