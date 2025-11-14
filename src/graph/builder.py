"""
Построение графа зависимостей
"""
import networkx as nx
from typing import Dict, List
from src.db.objects import DbObject

class GraphBuilder:
    def __init__(self):
        self.graph = nx.DiGraph()
        
    def add_object(self, obj: DbObject):
        """Добавляет объект в граф"""
        self.graph.add_node(
            f"{obj.schema}.{obj.name}",
            type=obj.type,
            definition=obj.definition
        )
        
    def add_dependency(self, source: str, target: str):
        """Добавляет зависимость между объектами"""
        self.graph.add_edge(source, target)
        
    def get_dependencies(self, object_name: str) -> Dict[str, List[str]]:
        """Возвращает входящие и исходящие зависимости объекта"""
        if object_name not in self.graph:
            return {"in": [], "out": []}
            
        return {
            "in": list(self.graph.predecessors(object_name)),
            "out": list(self.graph.successors(object_name))
        }
        
    def get_subgraph(self, object_name: str, depth: int = 1) -> nx.DiGraph:
        """Возвращает подграф для указанного объекта с заданной глубиной"""
        if object_name not in self.graph:
            return nx.DiGraph()
            
        nodes = {object_name}
        current_depth = 0
        
        while current_depth < depth:
            new_nodes = set()
            for node in nodes:
                new_nodes.update(self.graph.predecessors(node))
                new_nodes.update(self.graph.successors(node))
            nodes.update(new_nodes)
            current_depth += 1
            
        return self.graph.subgraph(nodes).copy()