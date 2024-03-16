from flask import Flask, jsonify, request
from dag_builder import DAGBuilder
from executor import execute_operations
from dependency_resolver import resolve_dependencies
import logging
import json

app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

dag_builder = DAGBuilder()

@app.route('/add_node', methods=['POST'])
def add_node():
    data = request.json
    try:
        dag_builder.add_node(data['id'], data['type'], data['parameters'])
        logging.info(f"Node {data['id']} added successfully.")
        return jsonify({"message": "Node added successfully"}), 200
    except Exception as e:
        logging.error("Failed to add node: ", exc_info=True)
        return jsonify({"error": str(e)}), 400

@app.route('/remove_node/<node_id>', methods=['DELETE'])
def remove_node(node_id):
    try:
        dag_builder.remove_node(node_id)
        logging.info(f"Node {node_id} removed successfully.")
        return jsonify({"message": "Node removed successfully"}), 200
    except Exception as e:
        logging.error("Failed to remove node: ", exc_info=True)
        return jsonify({"error": str(e)}), 400

@app.route('/add_edge', methods=['POST'])
def add_edge():
    data = request.json
    try:
        dag_builder.add_edge(data['source'], data['target'])
        logging.info(f"Edge from {data['source']} to {data['target']} added successfully.")
        return jsonify({"message": "Edge added successfully"}), 200
    except Exception as e:
        logging.error("Failed to add edge: ", exc_info=True)
        return jsonify({"error": str(e)}), 400

@app.route('/remove_edge', methods=['POST'])
def remove_edge():
    data = request.json
    try:
        dag_builder.remove_edge(data['source'], data['target'])
        logging.info(f"Edge from {data['source']} to {data['target']} removed successfully.")
        return jsonify({"message": "Edge removed successfully"}), 200
    except Exception as e:
        logging.error("Failed to remove edge: ", exc_info=True)
        return jsonify({"error": str(e)}), 400

@app.route('/execute', methods=['GET'])
def execute():
    try:
        execution_plan = resolve_dependencies(dag_builder.graph)
        execute_operations(execution_plan)
        logging.info("Execution started successfully.")
        return jsonify({"message": "Execution started"}), 202
    except Exception as e:
        logging.error("Failed to start execution: ", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/dag', methods=['GET'])
def get_dag():
    try:
        dag_json = dag_builder.to_json()
        logging.info("DAG fetched successfully.")
        return jsonify({"dag": json.loads(dag_json)}), 200
    except Exception as e:
        logging.error("Failed to fetch DAG: ", exc_info=True)
        return jsonify({"error": str(e)}), 400

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080)