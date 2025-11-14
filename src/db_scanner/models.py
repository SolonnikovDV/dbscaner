"""Database object models."""
from dataclasses import dataclass
from typing import List, Optional
from enum import Enum


class ObjectType(Enum):
    """Types of database objects."""
    TABLE = "table"
    VIEW = "view"
    MATERIALIZED_VIEW = "materialized_view"
    FUNCTION = "function"
    PROCEDURE = "procedure"
    TRIGGER = "trigger"
    SEQUENCE = "sequence"
    TYPE = "type"
    INDEX = "index"
    CONSTRAINT = "constraint"


@dataclass
class DBObject:
    """Base class for database objects."""
    name: str
    schema: str
    obj_type: ObjectType
    definition: str


@dataclass
class Relationship:
    """Represents a relationship between database objects."""
    source: DBObject
    target: DBObject
    relationship_type: str  # e.g., "references", "uses", "depends_on"
    description: Optional[str] = None
    depth: int = 1  # Глубина зависимости: 1 для прямых, >1 для косвенных