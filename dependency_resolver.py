from dask import delayed
import networkx as nx
from collections import deque
import logging
from function_registry import get_operation_function
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

def resolve_dependencies(graph):
    """
    Analyzes the DAG structure to determine an optimal execution order using Dask.
    Parameters:
        graph (nx.DiGraph): The DAG graph object from dag_builder.
    Returns:
        execution_plan (list): A list of delayed objects representing the execution plan.
    """
    try:
        if not isinstance(graph, nx.DiGraph):
            raise ValueError("The graph must be an instance of nx.DiGraph.")

        execution_plan = []
        visited = set()
        indegree_map = {node: graph.in_degree(node) for node in graph}

        # Using deque for efficient pop from the left
        queue = deque([node for node in graph if indegree_map[node] == 0])

        while queue:
            node = queue.popleft()
            visited.add(node)
            operation = delayed(execute_node)(graph.nodes[node], node)
            execution_plan.append(operation)

            for successor in graph.successors(node):
                indegree_map[successor] -= 1
                if indegree_map[successor] == 0:
                    queue.append(successor)

        # Ensure all nodes were visited (i.e., the graph is a DAG)
        if len(visited) != len(graph.nodes):
            raise ValueError("The graph contains a cycle, which is not allowed in a DAG.")

        logging.info("Dependency resolution completed successfully.")
        return execution_plan
    except Exception as e:
        logging.error("Failed to resolve dependencies: ", exc_info=True)
        raise

def execute_node(node_data, node_id):
    """
    Dynamically calls the function associated with a node's type from the function registry
    based on the node data passed to it.
    """
    try:
        logging.info(f"Starting execution of node {node_id} with data: {node_data}")
        
        node_type = node_data['type']
        operation_function, required_params = get_operation_function(node_type)
        
        # Extract parameters from node_data
        parameters = node_data.get('parameters', {})
        
        # Validate that all required parameters are provided
        missing_params = [param for param in required_params if param not in parameters]
        if missing_params:
            raise ValueError(f"Missing required parameters for node {node_id} of type {node_type}: {missing_params}")
        
        # Execute the operation function with the parameters
        operation_function(parameters)
        
        # Simulate execution with a sleep
        time.sleep(1)  # Simulating execution time for the operation
        
        logging.info(f"Completed execution of node {node_id}")
    except Exception as e:
        logging.error(f"Failed to execute node {node_id}: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    try:
        # This is a placeholder example. Integration with dag_builder.py is necessary for practical use.
        G = nx.DiGraph()
        G.add_node(1, operation="dummy_op", parameters={"param1": "value1"})
        G.add_node(2, operation="another_op", parameters={"param2": "value2"})
        G.add_edge(1, 2)

        plan = resolve_dependencies(G)
        # Execute the plan (for demonstration purposes, in actual use this should be handled more appropriately)
        for step in plan:
            result = step.compute()
            logging.info(f"Step result: {result}")
    except Exception as e:
        logging.error("An error occurred in the main execution block: ", exc_info=True)