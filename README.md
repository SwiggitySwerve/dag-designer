# DAG-Designer

DAG-Designer is a Python application designed to automate and optimize data transformations on dataframes using a Directed Acyclic Graph (DAG) structure. It aims to enhance efficiency through parallel execution of transformations and provides flexibility and adaptability with real-time DAG modifications.

## Overview

The application leverages Python, with Pandas for efficient dataframe manipulation, and Dask for parallel computation, ensuring fast and scalable data processing. The architecture includes a DAG Builder for setting up and modifying the DAG, a Dependency Resolver to optimize execution order, and an Executor for running the operations in parallel. The application is designed to be flexible, allowing for real-time updates to the DAG and supports monitoring of execution status.

## Features

- **DAG Representation:** Automates data transformation steps including logical, comparison, and arithmetic operations, as well as complex analyses.
- **Custom Parameters:** Operations can be customized with dataframe column names or primitive values as parameters.
- **Automatic Dependency Resolution:** Ensures optimal execution order for efficiency.
- **Real-time DAG Modification:** Enables dynamic updates to the DAG structure.
- **Execution Status Monitoring:** Offers real-time insights into each operation's execution status.

## Getting Started

### Requirements

Ensure Python is installed along with the following packages:
- pandas
- dask
- flask
- networkx
- gunicorn

### Quickstart

1. Clone the repository to your local machine.
2. Install the required packages with `pip install -r requirements.txt`.
3. Execute `python app.py` to start the Flask server.
4. Access the application API endpoints for adding nodes, removing nodes, adding edges, removing edges, executing the DAG, and fetching the DAG's current state.

### License

Copyright (c) 2024. All rights reserved. This project and its contents are proprietary and confidential.