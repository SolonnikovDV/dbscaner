"""Tests for graph visualization functionality."""
import pytest
import networkx as nx
from db_scanner.graph import GraphBuilder
from db_scanner.models import DBObject, Relationship, ObjectType


@pytest.fixture
def graph_builder():
    """Create graph builder instance."""
    return GraphBuilder()


@pytest.fixture
def sample_relationships():
    """Create sample relationships for testing."""
    obj1 = DBObject(
        name="users",
        schema="test_graph",
        obj_type=ObjectType.TABLE,
        definition="CREATE TABLE users..."
    )
    
    obj2 = DBObject(
        name="orders",
        schema="test_graph",
        obj_type=ObjectType.TABLE,
        definition="CREATE TABLE orders..."
    )
    
    obj3 = DBObject(
        name="order_summary",
        schema="test_graph",
        obj_type=ObjectType.VIEW,
        definition="CREATE VIEW order_summary..."
    )
    
    return [
        Relationship(
            source=obj1,
            target=obj2,
            relationship_type="referenced_by"
        ),
        Relationship(
            source=obj3,
            target=obj1,
            relationship_type="uses"
        ),
        Relationship(
            source=obj3,
            target=obj2,
            relationship_type="uses"
        )
    ]


def test_build_graph(graph_builder, sample_relationships):
    """Test building graph from relationships."""
    graph_builder.build_graph(sample_relationships)
    
    assert len(graph_builder.graph.nodes()) == 3
    assert len(graph_builder.graph.edges()) == 3
    
    # Check node attributes
    for node in graph_builder.graph.nodes():
        attrs = graph_builder.graph.nodes[node]
        assert 'obj_type' in attrs
        assert 'definition' in attrs


def test_get_object_details(graph_builder, sample_relationships):
    """Test getting object details."""
    graph_builder.build_graph(sample_relationships)
    
    details = graph_builder.get_object_details('test_graph.users')
    assert details is not None
    assert details['type'] == 'table'
    assert len(details['incoming']) > 0
    assert len(details['outgoing']) > 0


def test_highlight_node(graph_builder, sample_relationships):
    """Test graph visualization with highlighted node."""
    graph_builder.build_graph(sample_relationships)
    
    # This mainly tests that the method runs without errors
    # Visual testing would need to be done manually
    fig = graph_builder.visualize(highlight_node='test_graph.users')
    assert fig is not None