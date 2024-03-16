import json
import networkx as nx
import logging
from sourcemeta import MetaSource
from function_registry import get_operation_function

class DAGBuilder:
    def __init__(self):
        self.graph = nx.DiGraph()

    def add_node(self, node_id, node_type, parameters):
        try:
            # Verify if the node type is supported by checking against the function registry
            try:
                get_operation_function(node_type)
            except ValueError as e:
                raise ValueError(f"Unsupported node type: {node_type}. Error: {e}")

            if node_id in self.graph:
                raise ValueError(f"Node with id {node_id} already exists.")
            if not self._validate_parameters(node_type, parameters):
                raise ValueError(f"Invalid parameters for node type {node_type}.")
            # Updating internal representation to accommodate new parameter format
            internal_parameters = self._convert_to_internal_parameters(parameters)
            self.graph.add_node(node_id, type=node_type, parameters=internal_parameters)
            logging.info(f"Node {node_id} added successfully.")
        except Exception as e:
            logging.error(f"Failed to add node {node_id}: {e}", exc_info=True)
            raise

    def _validate_parameters(self, node_type, parameters):
        """
        Validates parameters based on node type. This method should be extended
        to handle specific validation logic for different node types and parameter formats.
        """
        # Updated validation logic to handle new parameter format
        if node_type == "ADD":
            if not isinstance(parameters, list) or not all(isinstance(param, dict) for param in parameters):
                return False
            for param in parameters:
                if "column" in param:
                    if not isinstance(param["column"], str):
                        return False
                elif "value" in param:
                    if not isinstance(param["value"], (int, float)):
                        return False
                else:
                    return False
        # Add more validation logic for other operations as needed
        return True

    def _convert_to_internal_parameters(self, parameters) -> MetaSource:
        """
        Converts the new parameter format to the internal representation.
        """
        internal_params = {"sources": []}
        for param in parameters:
            if "column" in param:
                internal_params["columns"].append(param["column"])
            elif "value" in param:
                internal_params["value"] = param["value"]
        return internal_params

    def remove_node(self, node_id):
        try:
            if node_id in self.graph:
                self.graph.remove_node(node_id)
                logging.info(f"Node {node_id} removed successfully.")
            else:
                logging.info(f"Node {node_id} not found.")
        except Exception as e:
            logging.error(f"Failed to remove node {node_id}: {e}", exc_info=True)
            raise

    def add_edge(self, source_id, target_id):
        try:
            if source_id not in self.graph or target_id not in self.graph:
                raise ValueError("One of the nodes in the edge does not exist.")
            self.graph.add_edge(source_id, target_id)
            if list(nx.simple_cycles(self.graph)):
                self.graph.remove_edge(source_id, target_id)
                raise ValueError("Adding this edge would create a cycle.")
            logging.info(f"Edge from {source_id} to {target_id} added successfully.")
        except Exception as e:
            logging.error(f"Failed to add edge from {source_id} to {target_id}: {e}", exc_info=True)
            raise

    def remove_edge(self, source_id, target_id):
        try:
            if self.graph.has_edge(source_id, target_id):
                self.graph.remove_edge(source_id, target_id)
                logging.info(f"Edge from {source_id} to {target_id} removed successfully.")
            else:
                logging.info(f"Edge from {source_id} to {target_id} not found.")
        except Exception as e:
            logging.error(f"Failed to remove edge from {source_id} to {target_id}: {e}", exc_info=True)
            raise

    def to_json(self):
        try:
            nodes = [{"id": n, "type": self.graph.nodes[n]['type'], "parameters": self._convert_from_internal_parameters(self.graph.nodes[n]['parameters'])} for n in self.graph.nodes()]
            edges = [{"source": u, "target": v} for u, v in self.graph.edges()]
            dag_json = json.dumps({"nodes": nodes, "edges": edges}, indent=4)
            logging.info("DAG exported to JSON successfully.")
            return dag_json
        except Exception as e:
            logging.error(f"Failed to export DAG to JSON: {e}", exc_info=True)
            raise

    def _convert_from_internal_parameters(self, parameters):
        """
        Converts the internal parameter representation back into the user-specified format for JSON export.
        """
        params = []
        for col in parameters["columns"]:
            params.append({"column": col})
        if parameters["value"] is not None:
            params.append({"value": parameters["value"]})
        return params

    def save_to_file(self, filepath):
        """
        Saves the DAG configuration to a JSON file.
        Parameters:
            filepath (str): The path to the file where the DAG configuration will be saved.
        """
        try:
            dag_json = self.to_json()
            with open(filepath, 'w') as file:
                file.write(dag_json)
            logging.info(f"DAG configuration saved to {filepath} successfully.")
        except Exception as e:
            logging.error(f"Failed to save DAG to {filepath}: {e}", exc_info=True)
            raise

    def load_from_file(self, filepath):
        """
        Loads the DAG configuration from a JSON file and reconstructs the DAG.
        Parameters:
            filepath (str): The path to the file from which the DAG configuration will be loaded.
        """
        try:
            with open(filepath, 'r') as file:
                dag_config = json.load(file)

            # Clear existing DAG before loading new configuration
            self.graph.clear()
            for node in dag_config["nodes"]:
                # Correctly convert the parameters for loading
                converted_parameters = [{"column": p["column"]} if "column" in p else {"value": p["value"]} for p in node["parameters"]]
                self.add_node(node["id"], node["type"], converted_parameters)
            for edge in dag_config["edges"]:
                self.add_edge(edge["source"], edge["target"])

            logging.info(f"DAG configuration loaded from {filepath} successfully.")
        except Exception as e:
            logging.error(f"Failed to load DAG from {filepath}: {e}", exc_info=True)
            raise

# Example usage
if __name__ == "__main__":
    try:
        dag = DAGBuilder()
        dag.add_node("1", "ADD", [{"column": "col1"}, {"column": "col2"}, {"value": "5"}])
        dag.add_node("2", "SMA", [{"window_size": 5}, {"column": "col3"}])
        dag.add_edge("1", "2")
        dag.save_to_file("dag_configuration.json")  # Example save operation
        dag.load_from_file("dag_configuration.json")  # Example load operation
        print(dag.to_json())
    except Exception as e:
        logging.error(f"An error occurred during DAGBuilder usage: {e}", exc_info=True)