import unittest
from unittest.mock import patch
from dag_builder import DAGBuilder
from function_registry import get_operation_function, function_registry
from dependency_resolver import execute_node

class TestFunctionRegistry(unittest.TestCase):
    def setUp(self):
        self.dag_builder = DAGBuilder()

    def test_supported_node_types(self):
        # Test adding supported node types
        with patch.dict(function_registry, {'ADD': (lambda x: None, ['columns', 'value']), 'SMA': (lambda x: None, ['window_size', 'column'])}):
            self.dag_builder.add_node("1", "ADD", [{"column": "col1"}, {"column": "col2"}, {"value": 5}])
            self.dag_builder.add_node("2", "SMA", [{"window_size": 5}, {"column": "col3"}])
            self.assertIn("1", self.dag_builder.graph)
            self.assertIn("2", self.dag_builder.graph)

    def test_unsupported_node_type(self):
        # Test adding an unsupported node type
        with self.assertRaises(ValueError):
            self.dag_builder.add_node("3", "UNSUPPORTED_TYPE", [{"param": "value"}])

    @patch('function_registry.add_operation')
    @patch('function_registry.sma_operation')
    def test_correct_function_calls(self, mock_sma_operation, mock_add_operation):
        # Test that the correct functions are called with expected parameters
        node_data_add = {"type": "ADD", "parameters": {"columns": ["col1", "col2"], "value": 5}}
        execute_node(node_data_add, "1")
        mock_add_operation.assert_called_once_with({"columns": ["col1", "col2"], "value": 5})

        node_data_sma = {"type": "SMA", "parameters": {"window_size": 5, "column": "col3"}}
        execute_node(node_data_sma, "2")
        mock_sma_operation.assert_called_once_with({"window_size": 5, "column": "col3"})

if __name__ == '__main__':
    unittest.main()