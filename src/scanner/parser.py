"""
Парсинг SQL-кода и анализ зависимостей
"""
import sqlparse
from typing import List, Set

class SqlParser:
    @staticmethod
    def parse_sql(sql: str) -> List[sqlparse.sql.Statement]:
        """Парсинг SQL-кода"""
        return sqlparse.parse(sql)
    
    @staticmethod
    def extract_dependencies(sql: str) -> Set[str]:
        """
        Извлекает зависимости из SQL-кода
        Возвращает множество имен объектов, от которых зависит данный код
        """
        dependencies = set()
        statements = SqlParser.parse_sql(sql)
        
        for statement in statements:
            # Ищем FROM clause
            from_seen = False
            for token in statement.tokens:
                if token.is_keyword and token.value.upper() == 'FROM':
                    from_seen = True
                elif from_seen and token.ttype is None:
                    # Добавляем таблицы после FROM
                    for identifier in token.get_identifiers():
                        dependencies.add(str(identifier))
                        
            # Ищем JOIN clause
            for token in statement.tokens:
                if token.is_keyword and 'JOIN' in token.value.upper():
                    next_token = token.next_sibling
                    if next_token and next_token.ttype is None:
                        for identifier in next_token.get_identifiers():
                            dependencies.add(str(identifier))
        
        return dependencies
    
    @staticmethod
    def get_object_type(sql: str) -> str:
        """Определяет тип объекта по его DDL"""
        statements = SqlParser.parse_sql(sql)
        if not statements:
            return "unknown"
            
        first_token = statements[0].token_first()
        if not first_token or not first_token.is_keyword:
            return "unknown"
            
        keyword = first_token.value.upper()
        if 'CREATE TABLE' in keyword:
            return "table"
        elif 'CREATE VIEW' in keyword:
            return "view"
        elif 'CREATE FUNCTION' in keyword:
            return "function"
        elif 'CREATE TYPE' in keyword:
            return "type"
        elif 'CREATE SEQUENCE' in keyword:
            return "sequence"
        
        return "unknown"