"""
Сбор информации об объектах базы данных
"""
from typing import List, Dict
import psycopg2
from src.db.connection import get_connection
from src.db.objects import DbObject, Table, View, Function, Type, Sequence

class DbObjectCollector:
    def __init__(self):
        self.conn = get_connection()
        
    def collect_all_objects(self) -> List[DbObject]:
        """Собирает информацию обо всех объектах в базе данных"""
        objects = []
        objects.extend(self.collect_tables())
        objects.extend(self.collect_views())
        objects.extend(self.collect_functions())
        objects.extend(self.collect_types())
        objects.extend(self.collect_sequences())
        return objects
        
    def collect_tables(self) -> List[Table]:
        """Собирает информацию о таблицах"""
        query = """
            SELECT 
                schemaname,
                tablename,
                pg_get_tabledef(schemaname || '.' || tablename) as definition
            FROM pg_tables
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        """
        return self._execute_query(query, self._create_table)
        
    def collect_views(self) -> List[View]:
        """Собирает информацию о представлениях"""
        query = """
            SELECT 
                schemaname,
                viewname,
                definition
            FROM pg_views
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        """
        return self._execute_query(query, self._create_view)
        
    def collect_functions(self) -> List[Function]:
        """Собирает информацию о функциях"""
        query = """
            SELECT 
                n.nspname as schemaname,
                p.proname as funcname,
                pg_get_functiondef(p.oid) as definition
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
        """
        return self._execute_query(query, self._create_function)
        
    def collect_types(self) -> List[Type]:
        """Собирает информацию о пользовательских типах"""
        query = """
            SELECT 
                n.nspname as schemaname,
                t.typname as typename,
                pg_catalog.format_type(t.oid, NULL) as definition
            FROM pg_type t
            JOIN pg_namespace n ON t.typnamespace = n.oid
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
            AND t.typtype = 'c'
        """
        return self._execute_query(query, self._create_type)
        
    def collect_sequences(self) -> List[Sequence]:
        """Собирает информацию о последовательностях"""
        query = """
            SELECT 
                sequence_schema,
                sequence_name,
                pg_get_serial_sequence(sequence_schema || '.' || sequence_name) as definition
            FROM information_schema.sequences
            WHERE sequence_schema NOT IN ('pg_catalog', 'information_schema')
        """
        return self._execute_query(query, self._create_sequence)
        
    def _execute_query(self, query: str, factory_method) -> List[DbObject]:
        """Выполняет запрос и создает объекты с помощью фабричного метода"""
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
                return [factory_method(row) for row in cur.fetchall()]
        except psycopg2.Error as e:
            print(f"Ошибка при выполнении запроса: {e}")
            return []
            
    def _create_table(self, row: tuple) -> Table:
        schema, name, definition = row
        return Table(name=name, schema=schema, type='table',
                    definition=definition, columns=[], constraints=[])
                    
    def _create_view(self, row: tuple) -> View:
        schema, name, definition = row
        return View(name=name, schema=schema, type='view',
                   definition=definition, source_tables=[])
                   
    def _create_function(self, row: tuple) -> Function:
        schema, name, definition = row
        return Function(name=name, schema=schema, type='function',
                       definition=definition, arguments=[], return_type='', language='')
                       
    def _create_type(self, row: tuple) -> Type:
        schema, name, definition = row
        return Type(name=name, schema=schema, type='type',
                   definition=definition)
                   
    def _create_sequence(self, row: tuple) -> Sequence:
        schema, name, definition = row
        return Sequence(name=name, schema=schema, type='sequence',
                       definition=definition, current_value=0, increment=1)
                       
    def __del__(self):
        """Закрываем соединение при уничтожении объекта"""
        if hasattr(self, 'conn'):
            self.conn.close()