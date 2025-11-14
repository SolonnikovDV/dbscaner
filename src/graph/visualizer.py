"""
Визуализация графа зависимостей
"""
import networkx as nx
import matplotlib.pyplot as plt
from PyQt5.QtWidgets import QWidget
from src.config import GRAPH_CONFIG, OBJECT_COLORS

class GraphVisualizer:
    def __init__(self, graph: nx.DiGraph):
        self.graph = graph
        
    def draw_graph(self, selected_node: str = None):
        """Отрисовка графа"""
        plt.figure(figsize=(
            GRAPH_CONFIG['width'] / 100,
            GRAPH_CONFIG['height'] / 100
        ))
        
        # Позиционирование узлов
        pos = nx.spring_layout(self.graph)
        
        # Рисуем узлы для каждого типа объектов
        for node_type, color in OBJECT_COLORS.items():
            nodes = [
                node for node, attrs in self.graph.nodes(data=True)
                if attrs.get('type') == node_type
            ]
            nx.draw_networkx_nodes(
                self.graph, pos,
                nodelist=nodes,
                node_color=color,
                node_size=GRAPH_CONFIG['node_size']
            )
            
        # Выделяем выбранный узел
        if selected_node and selected_node in self.graph:
            nx.draw_networkx_nodes(
                self.graph, pos,
                nodelist=[selected_node],
                node_color='red',
                node_size=GRAPH_CONFIG['node_size'] * 1.2
            )
        
        # Рисуем связи
        nx.draw_networkx_edges(
            self.graph, pos,
            arrowsize=GRAPH_CONFIG['arrow_size']
        )
        
        # Добавляем подписи
        nx.draw_networkx_labels(
            self.graph, pos,
            font_size=GRAPH_CONFIG['font_size']
        )
        
        plt.title("Graph of Database Objects")
        plt.axis('off')
        plt.tight_layout()
        
    def save_graph(self, filename: str):
        """Сохранение графа в файл"""
        plt.savefig(filename)
        
    def clear(self):
        """Очистка текущего графа"""
        plt.clf()
        plt.close()

class GraphWidget(QWidget):
    """Виджет для отображения графа в GUI"""
    def __init__(self, parent=None):
        super().__init__(parent)
        # TODO: Реализовать интерактивный виджет с графом