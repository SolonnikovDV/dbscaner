"""
Главный файл приложения

Database Object Dependency Graph Scanner
Copyright (c) 2025 Dmitry Solonnikov

Licensed under the MIT License.
"""
from PyQt5.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QHBoxLayout, QWidget, QListWidget, QTextEdit
from src.graph.visualizer import GraphWidget
from src.scanner.collector import DbObjectCollector
from src.graph.builder import GraphBuilder

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Database Object Graph Scanner © Dmitry Solonnikov")
        self.setGeometry(100, 100, 1200, 800)
        
        # Создаем главный виджет и лейаут
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        layout = QHBoxLayout(main_widget)
        
        # Список объектов БД
        objects_layout = QVBoxLayout()
        self.object_list = QListWidget()
        self.object_list.itemClicked.connect(self.on_object_selected)
        objects_layout.addWidget(self.object_list)
        
        # Область для отображения графа
        self.graph_widget = GraphWidget()
        
        # Область для отображения DDL
        self.ddl_view = QTextEdit()
        self.ddl_view.setReadOnly(True)
        
        # Добавляем виджеты в лейаут
        left_panel = QWidget()
        left_panel.setLayout(objects_layout)
        left_panel.setMaximumWidth(300)
        
        layout.addWidget(left_panel)
        layout.addWidget(self.graph_widget)
        layout.addWidget(self.ddl_view)
        
        # Инициализируем коллектор и строитель графа
        self.collector = DbObjectCollector()
        self.graph_builder = GraphBuilder()
        
        # Загружаем объекты
        self.load_objects()
        
    def load_objects(self):
        """Загрузка списка объектов из БД"""
        objects = self.collector.collect_all_objects()
        for obj in objects:
            self.object_list.addItem(f"{obj.schema}.{obj.name} ({obj.type})")
            self.graph_builder.add_object(obj)
            
    def on_object_selected(self, item):
        """Обработка выбора объекта из списка"""
        object_name = item.text().split(" (")[0]
        
        # Получаем подграф для выбранного объекта
        subgraph = self.graph_builder.get_subgraph(object_name)
        
        # Обновляем визуализацию
        self.graph_widget.update_graph(subgraph, object_name)
        
        # Показываем DDL объекта
        node_data = self.graph_builder.graph.nodes[object_name]
        self.ddl_view.setText(node_data['definition'])

def main():
    app = QApplication([])
    window = MainWindow()
    window.show()
    app.exec_()

if __name__ == '__main__':
    main()