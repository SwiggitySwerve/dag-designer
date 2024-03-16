import logging
import talib
import numpy as np

# Setup basic configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def add_operation(parameters):
    logging.info(f"Executing ADD operation with parameters: {parameters}")

def sma_operation(parameters):
    logging.info(f"Executing SMA operation with parameters: {parameters}")

def adx_operation(parameters):
    """
    Computes the Average Directional Movement Index (ADX).
    
    Parameters:
    - parameters (dict): A dictionary containing 'high', 'low', 'close' price arrays and 'time_period'.
    """
    try:
        high = np.array(parameters['high'])
        low = np.array(parameters['low'])
        close = np.array(parameters['close'])
        time_period = parameters['time_period']
        
        adx = talib.ADX(high, low, close, time_period=time_period)
        logging.info(f"Executing ADX operation with time_period: {time_period}")
        return adx
    except Exception as e:
        logging.error(f"Failed to compute ADX: {e}", exc_info=True)
        raise

# Function registry mapping node types to (function, list of required parameters)
function_registry = {
    'ADD': (add_operation, ['columns', 'value']),  # Assuming 'columns' is a list of column names and 'value' is the value to add
    'SMA': (sma_operation, ['window_size', 'column']),  # Assuming 'window_size' is an integer and 'column' is the name of the column
    'ADX': (adx_operation, ['high', 'low', 'close', 'time_period'])  # Adding ADX to the function registry
}

def get_operation_function(node_type):
    """
    Retrieves the operation function and required parameters for a given node type.
    
    Parameters:
        node_type (str): The type of the node for which the operation function is requested.
        
    Returns:
        tuple: A tuple containing the operation function and a list of required parameters names.
        
    Raises:
        ValueError: If the node type is not supported.
    """
    try:
        if node_type in function_registry:
            return function_registry[node_type]
        else:
            error_message = f"Unsupported node type: {node_type}"
            logging.error(error_message)
            raise ValueError(error_message)
    except Exception as e:
        logging.error("Error retrieving operation function: ", exc_info=True)
        raise e