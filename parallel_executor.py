"""
Parallel Executor
=================
This module executes tasks in parallel using Python multiprocessing.

Features:
- Create worker pool with configurable process count
- Execute independent tasks in parallel
- Synchronize dependent tasks
- Handle data distribution and collection
"""

import pandas as pd
import multiprocessing as mp
from typing import List, Dict, Any, Tuple
import time


def execute_filter_task(data: pd.DataFrame, task: Dict[str, Any]) -> pd.DataFrame:
    """
    Execute a filter operation on the dataset.
    
    Args:
        data: Input DataFrame
        task: Task dictionary with filter conditions
        
    Returns:
        Filtered DataFrame
    """
    result = data.copy()
    
    conditions = task.get('conditions', [])
    for condition in conditions:
        field = condition['field']
        operator = condition['operator']
        value = condition['value']
        
        if operator == '=':
            result = result[result[field] == value]
        elif operator == '>':
            result = result[result[field] > value]
        elif operator == '<':
            result = result[result[field] < value]
        elif operator == '>=':
            result = result[result[field] >= value]
        elif operator == '<=':
            result = result[result[field] <= value]
        elif operator == '!=':
            result = result[result[field] != value]
    
    return result


def execute_group_task(data: pd.DataFrame, task: Dict[str, Any]) -> pd.DataFrame:
    """
    Execute a grouping operation on the dataset.
    
    Args:
        data: Input DataFrame
        task: Task dictionary with group_by fields
        
    Returns:
        Grouped DataFrame
    """
    group_by = task.get('group_by', [])
    
    if not group_by:
        return data
    
    # Group and aggregate (we'll aggregate in next task, just mark groups here)
    # For now, just return the data with grouping applied
    grouped = data.groupby(group_by, as_index=False)
    
    return grouped


def execute_aggregate_task(data, task: Dict[str, Any]) -> pd.DataFrame:
    """
    Execute an aggregation operation on the dataset.
    
    Args:
        data: Input DataFrame or GroupBy object
        task: Task dictionary with aggregation details
        
    Returns:
        Aggregated DataFrame
    """
    agg_type = task.get('agg_type', 'sum')
    agg_field = task.get('agg_field', 'sales')
    
    # If data is already grouped, use it
    if isinstance(data, pd.core.groupby.DataFrameGroupBy):
        if agg_type == 'sum':
            result = data[agg_field].sum().reset_index()
        elif agg_type == 'avg' or agg_type == 'mean':
            result = data[agg_field].mean().reset_index()
        elif agg_type == 'count':
            result = data[agg_field].count().reset_index()
        elif agg_type == 'max':
            result = data[agg_field].max().reset_index()
        elif agg_type == 'min':
            result = data[agg_field].min().reset_index()
        else:
            result = data[agg_field].sum().reset_index()
    else:
        # If not grouped, perform global aggregation
        if agg_type == 'sum':
            value = data[agg_field].sum()
        elif agg_type == 'avg' or agg_type == 'mean':
            value = data[agg_field].mean()
        elif agg_type == 'count':
            value = len(data)
        elif agg_type == 'max':
            value = data[agg_field].max()
        elif agg_type == 'min':
            value = data[agg_field].min()
        else:
            value = data[agg_field].sum()
        
        result = pd.DataFrame({agg_field: [value]})
    
    return result


def execute_fetch_task(data: pd.DataFrame, task: Dict[str, Any]) -> pd.DataFrame:
    """
    Execute a simple fetch/select operation.
    
    Args:
        data: Input DataFrame
        task: Task dictionary
        
    Returns:
        DataFrame
    """
    return data


def worker_execute_task(args: Tuple[pd.DataFrame, Dict[str, Any]]) -> Tuple[str, Any, float]:
    """
    Worker function to execute a single task.
    This function is called by each worker process.
    
    Args:
        args: Tuple of (data, task)
        
    Returns:
        Tuple of (task_id, result, execution_time)
    """
    data, task = args
    task_id = task['task_id']
    operation = task['operation']
    
    start_time = time.time()
    
    try:
        if operation == 'filter':
            result = execute_filter_task(data, task)
        elif operation == 'group':
            result = execute_group_task(data, task)
        elif operation == 'aggregate':
            result = execute_aggregate_task(data, task)
        elif operation == 'fetch':
            result = execute_fetch_task(data, task)
        else:
            result = data
        
        execution_time = time.time() - start_time
        return (task_id, result, execution_time)
    
    except Exception as e:
        execution_time = time.time() - start_time
        return (task_id, f"Error: {str(e)}", execution_time)


class ParallelExecutor:
    """
    Executes tasks in parallel using multiprocessing.
    """
    
    def __init__(self, data: pd.DataFrame, num_processes: int = None):
        """
        Initialize the parallel executor.
        
        Args:
            data: Input dataset (pandas DataFrame)
            num_processes: Number of parallel processes (default: CPU count)
        """
        self.data = data
        self.num_processes = num_processes or mp.cpu_count()
        self.results = {}
        self.execution_times = {}
    
    def execute_layer(self, layer_tasks: List[Dict[str, Any]], 
                     current_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute all tasks in a single layer in parallel.
        
        Args:
            layer_tasks: List of tasks in this layer
            current_results: Results from previous layers
            
        Returns:
            Dictionary mapping task_id to result
        """
        layer_results = {}
        
        # Prepare arguments for each task
        task_args = []
        for task in layer_tasks:
            # Get input data for this task
            depends_on = task.get('depends_on', [])
            
            if depends_on:
                # Use result from dependency
                input_data = current_results[depends_on[0]]
            else:
                # Use original data
                input_data = self.data
            
            task_args.append((input_data, task))
        
        # Execute tasks in parallel
        if len(task_args) == 1:
            # Single task - no need for multiprocessing
            task_id, result, exec_time = worker_execute_task(task_args[0])
            layer_results[task_id] = result
            self.execution_times[task_id] = exec_time
        else:
            # Multiple tasks - use multiprocessing
            with mp.Pool(processes=min(self.num_processes, len(task_args))) as pool:
                results = pool.map(worker_execute_task, task_args)
            
            for task_id, result, exec_time in results:
                layer_results[task_id] = result
                self.execution_times[task_id] = exec_time
        
        return layer_results
    
    def execute_plan(self, execution_plan: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the complete execution plan layer by layer.
        
        Args:
            execution_plan: Execution plan from TaskPlanner
            
        Returns:
            Dictionary containing final results and execution metadata
        """
        start_time = time.time()
        current_results = {}
        
        layers = execution_plan['execution_layers']
        
        for layer_info in layers:
            layer_tasks = []
            
            # Extract task dictionaries with all required info
            for task_info in layer_info['tasks']:
                task = {
                    'task_id': task_info['task_id'],
                    'operation': task_info['operation'],
                    **task_info['details']
                }
                
                # Add depends_on from original tasks if needed
                # (This should already be in details, but let's ensure)
                if 'depends_on' not in task:
                    task['depends_on'] = []
                
                layer_tasks.append(task)
            
            # Execute this layer
            layer_results = self.execute_layer(layer_tasks, current_results)
            
            # Update current results
            current_results.update(layer_results)
        
        total_time = time.time() - start_time
        
        # Get final result (last task's output)
        final_task_id = layers[-1]['tasks'][-1]['task_id']
        final_result = current_results[final_task_id]
        
        return {
            'final_result': final_result,
            'all_results': current_results,
            'execution_times': self.execution_times,
            'total_execution_time': total_time,
            'num_processes_used': self.num_processes
        }


# Example usage
if __name__ == '__main__':
    # Create sample dataset
    data = pd.DataFrame({
        'product': ['A', 'B', 'A', 'B', 'A', 'B'] * 100,
        'region': ['North', 'South', 'East', 'West'] * 150,
        'year': [2022, 2023] * 300,
        'sales': [10000, 15000, 12000, 18000, 11000, 16000] * 100,
        'quantity': [100, 150, 120, 180, 110, 160] * 100
    })
    
    # Sample execution plan
    execution_plan = {
        'execution_layers': [
            {
                'layer_id': 1,
                'tasks': [
                    {
                        'task_id': 'T1',
                        'operation': 'filter',
                        'details': {
                            'conditions': [{'field': 'year', 'operator': '=', 'value': 2023}],
                            'depends_on': []
                        }
                    }
                ]
            },
            {
                'layer_id': 2,
                'tasks': [
                    {
                        'task_id': 'T2',
                        'operation': 'group',
                        'details': {
                            'group_by': ['region'],
                            'depends_on': ['T1']
                        }
                    }
                ]
            },
            {
                'layer_id': 3,
                'tasks': [
                    {
                        'task_id': 'T3',
                        'operation': 'aggregate',
                        'details': {
                            'agg_type': 'sum',
                            'agg_field': 'sales',
                            'depends_on': ['T2']
                        }
                    }
                ]
            }
        ]
    }
    
    executor = ParallelExecutor(data, num_processes=4)
    results = executor.execute_plan(execution_plan)
    
    print("Final Result:")
    print(results['final_result'])
    print(f"\nTotal Execution Time: {results['total_execution_time']:.4f} seconds")
    print(f"Processes Used: {results['num_processes_used']}")