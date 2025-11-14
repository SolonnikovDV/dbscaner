"""
Модели объектов базы данных
"""
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class DbObject:
    """Базовый класс для объектов БД"""
    name: str
    schema: str
    type: str
    definition: str

@dataclass
class Table(DbObject):
    """Таблица"""
    columns: List[str]
    constraints: List[str]

@dataclass
class View(DbObject):
    """Представление"""
    source_tables: List[str]
    is_materialized: bool = False

@dataclass
class Function(DbObject):
    """Функция"""
    arguments: List[str]
    return_type: str
    language: str

@dataclass
class Type(DbObject):
    """Пользовательский тип данных"""
    attributes: Optional[List[str]] = None

@dataclass
class Sequence(DbObject):
    """Последовательность"""
    current_value: int
    increment: int