from dask.distributed import Client, as_completed
import logging
from dependency_resolver import resolve_dependencies
from dag_builder import DAGBuilder
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

def execute_operations(execution_plan, retry_attempts=3):
    """
    Executes the given operation plan in parallel using Dask, with retries for failed operations.
    Parameters:
        execution_plan (list): A list of delayed objects representing the operations to be executed.
        retry_attempts (int): The number of times to retry failed operations.
    """
    try:
        client = Client(memory_limit='4GB', n_workers=4, threads_per_worker=1)
        logging.info("Dask client initialized for parallel execution. Starting operations...")
        futures = client.compute(execution_plan)
        operation_retry_attempts = {future: retry_attempts for future in futures}  # Track retries per operation
        all_futures = list(futures)  # Keep track of all futures, including retries

        while all_futures:
            for future in as_completed(all_futures):
                if future in operation_retry_attempts:  # If future is still being tracked for retries
                    try:
                        result = future.result()  # Blocks until the future is done
                        logging.info(f"Operation completed with result: {result}")
                        all_futures.remove(future)  # Remove from tracking once succeeded
                        del operation_retry_attempts[future]  # No longer tracking retries for this future
                    except Exception as e:
                        logging.error(f"Operation failed: {e}", exc_info=True)
                        current_attempts = operation_retry_attempts[future]
                        if current_attempts > 1:
                            logging.info("Retrying failed operation.")
                            retry_future = client.retry(future)
                            operation_retry_attempts[retry_future] = current_attempts - 1  # Update retry attempts for the new future
                            all_futures.append(retry_future)  # Add new future to the list for tracking
                        else:
                            logging.error("Max retry attempts reached. Operation permanently failed.", exc_info=True)
                            raise
                        all_futures.remove(future)  # Remove failed attempt from tracking
                        del operation_retry_attempts[future]  # Stop tracking retries for the failed future
                else:
                    all_futures.remove(future)  # Clean up any futures that are no longer being tracked
    except Exception as e:
        logging.error(f"Failed to execute operations: {e}", exc_info=True)
        raise
    finally:
        client.shutdown()
        logging.info("Dask client shutdown after completing operations.")

if __name__ == "__main__":
    try:
        # Example usage - This block is for demonstration purposes. 
        # In practice, `execution_plan` will come from the Dependency Resolver.
        dag_builder = DAGBuilder()
        # Example nodes and edges added to dag_builder as per your DAG configuration
        # These lines are placeholders and should be modified according to the actual operations and dependencies in your DAG.
        dag_builder.add_node("1", "ADD", {"columns": ["col1", "col2"], "value": 5})
        dag_builder.add_node("2", "SMA", {"window_size": 5, "column": "col3"})
        dag_builder.add_edge("1", "2")

        execution_plan = resolve_dependencies(dag_builder.graph)
        execute_operations(execution_plan)
    except Exception as e:
        logging.error("An error occurred in the main execution block: ", exc_info=True)