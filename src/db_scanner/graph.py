"""Graph visualization module."""
from typing import List, Dict, Any
import networkx as nx
import matplotlib.pyplot as plt

from .models import DBObject, Relationship


class GraphBuilder:
    """Builds and visualizes database object graphs."""

    def __init__(self):
        """Initialize graph builder."""
        self.graph = nx.DiGraph()
        self.pos = None

    def build_graph(self, relationships: List[Relationship]):
        """Build graph from relationships.
        
        Args:
            relationships: List of relationships between database objects
        """
        self.graph.clear()
        
        # Add nodes and edges
        for rel in relationships:
            source_id = f"{rel.source.schema}.{rel.source.name}"
            target_id = f"{rel.target.schema}.{rel.target.name}"
            
            # Add nodes if they don't exist
            if not self.graph.has_node(source_id):
                self.graph.add_node(source_id, 
                                  obj_type=rel.source.obj_type.value,
                                  definition=rel.source.definition)
            
            if not self.graph.has_node(target_id):
                self.graph.add_node(target_id,
                                  obj_type=rel.target.obj_type.value,
                                  definition=rel.target.definition)
            
            # Add edge
            self.graph.add_edge(source_id, target_id,
                              relationship_type=rel.relationship_type,
                              description=rel.description)

        # Calculate layout
        self.pos = nx.spring_layout(self.graph)

    def visualize(self, highlight_node: str = None):
        """Visualize the graph.
        
        Args:
            highlight_node: Node to highlight (optional)
        """
        if not self.pos:
            return

        plt.figure(figsize=(12, 8))
        
        # Draw nodes
        node_colors = []
        for node in self.graph.nodes():
            color = 'lightblue'
            if highlight_node and node == highlight_node:
                color = 'red'
            elif highlight_node and (self.graph.has_edge(highlight_node, node) or 
                                   self.graph.has_edge(node, highlight_node)):
                color = 'orange'
            node_colors.append(color)
        
        nx.draw_networkx_nodes(self.graph, self.pos,
                             node_color=node_colors,
                             node_size=2000)
        
        # Draw edges
        nx.draw_networkx_edges(self.graph, self.pos,
                             edge_color='gray',
                             arrows=True,
                             arrowsize=20)
        
        # Add labels
        nx.draw_networkx_labels(self.graph, self.pos)
        
        # Add edge labels
        edge_labels = nx.get_edge_attributes(self.graph, 'relationship_type')
        nx.draw_networkx_edge_labels(self.graph, self.pos,
                                   edge_labels=edge_labels)
        
        plt.title("Database Objects Relationship Graph")
        plt.axis('off')
        plt.tight_layout()
        
        return plt.gcf()  # Return figure for further handling

    def get_object_details(self, node_id: str) -> Dict[str, Any]:
        """Get details of a specific node.
        
        Args:
            node_id: ID of the node to get details for
            
        Returns:
            Dictionary with node details
        """
        if self.graph.has_node(node_id):
            return {
                'id': node_id,
                'type': self.graph.nodes[node_id]['obj_type'],
                'definition': self.graph.nodes[node_id]['definition'],
                'incoming': list(self.graph.predecessors(node_id)),
                'outgoing': list(self.graph.successors(node_id))
            }
        return None