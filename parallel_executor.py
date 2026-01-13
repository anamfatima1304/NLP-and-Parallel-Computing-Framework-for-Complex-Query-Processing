"""
Parallel Executor (UPDATED VERSION)
====================================
This module executes tasks in parallel using Python multiprocessing.
Now supports LEVEL-BASED execution from the enhanced NLP processor.

Features:
- Execute tasks by dependency levels (not sequential pipeline)
- Create worker pool with configurable process count
- Execute independent tasks in parallel at each level
- Handle data distribution and collection
- Track performance metrics per level

FIXED: 
- Proper handling of multiple dependencies by merging results
- Case-insensitive column matching
- Better error messages
"""

import re
import json
import pandas as pd
import multiprocessing as mp
from typing import List, Dict, Any, Tuple
from collections import defaultdict, deque
import time
import numpy as np


def simulate_computation(complexity: int = 500000):
    """Simulate computational workload"""
    result = 0
    for _ in range(complexity):
        result += np.random.random()
    return result


def find_column(data: pd.DataFrame, field_name: str) -> str:
    """
    Find column name in DataFrame (case-insensitive).
    Returns actual column name or raises KeyError.
    """
    # Try exact match first
    if field_name in data.columns:
        return field_name
    
    # Try case-insensitive match
    field_lower = field_name.lower()
    for col in data.columns:
        if col.lower() == field_lower:
            return col
    
    # Try partial match
    for col in data.columns:
        if field_lower in col.lower() or col.lower() in field_lower:
            return col
    
    raise KeyError(f"Column '{field_name}' not found. Available columns: {list(data.columns)}")


def execute_filter_task(data: pd.DataFrame, task: Dict[str, Any]) -> pd.DataFrame:
    """Execute filter operation with case-insensitive column matching"""
    simulate_computation(500000)
    result = data.copy()
    conditions = task.get('conditions', [])
    
    for condition in conditions:
        field = condition['field']
        operator = condition['operator']
        value = condition['value']
        
        # Find actual column name (case-insensitive)
        try:
            actual_column = find_column(result, field)
        except KeyError as e:
            raise KeyError(f"Filter error: {str(e)}")
        
        # Apply filter
        if operator == '=':
            result = result[result[actual_column] == value]
        elif operator == '>':
            result = result[result[actual_column] > value]
        elif operator == '<':
            result = result[result[actual_column] < value]
        elif operator == '>=':
            result = result[result[actual_column] >= value]
        elif operator == '<=':
            result = result[result[actual_column] <= value]
    
    return result


def execute_group_task(data: pd.DataFrame, task: Dict[str, Any]) -> pd.DataFrame:
    """Execute grouping operation with case-insensitive column matching"""
    simulate_computation(300000)
    group_by = task.get('group_by', [])
    if not group_by:
        return data
    
    # Find actual column names
    actual_columns = []
    for field in group_by:
        try:
            actual_col = find_column(data, field)
            actual_columns.append(actual_col)
        except KeyError as e:
            raise KeyError(f"Group error: {str(e)}")
    
    return data.groupby(actual_columns, as_index=False)


def execute_aggregate_task(data, task: Dict[str, Any]) -> pd.DataFrame:
    """Execute aggregation operation with case-insensitive column matching"""
    simulate_computation(400000)
    agg_type = task.get('agg_type', 'sum')
    agg_field = task.get('agg_field', 'sales')
    
    # Handle GroupBy objects
    if isinstance(data, pd.core.groupby.DataFrameGroupBy):
        # Get the actual DataFrame to check columns
        temp_df = data.obj
        try:
            actual_field = find_column(temp_df, agg_field)
        except KeyError as e:
            raise KeyError(f"Aggregate error: {str(e)}")
        
        if agg_type == 'sum':
            result = data[actual_field].sum().reset_index()
        elif agg_type in ['avg', 'mean']:
            result = data[actual_field].mean().reset_index()
        elif agg_type == 'count':
            result = data[actual_field].count().reset_index()
        elif agg_type == 'max':
            result = data[actual_field].max().reset_index()
        elif agg_type == 'min':
            result = data[actual_field].min().reset_index()
        else:
            result = data[actual_field].sum().reset_index()
    
    # Handle regular DataFrames
    else:
        try:
            actual_field = find_column(data, agg_field)
        except KeyError as e:
            raise KeyError(f"Aggregate error: {str(e)}")
        
        if agg_type == 'sum':
            value = data[actual_field].sum()
        elif agg_type in ['avg', 'mean']:
            value = data[actual_field].mean()
        elif agg_type == 'count':
            value = len(data)
        elif agg_type == 'max':
            value = data[actual_field].max()
        elif agg_type == 'min':
            value = data[actual_field].min()
        else:
            value = data[actual_field].sum()
        
        result = pd.DataFrame({actual_field: [value]})
    
    return result


def worker_execute_task(args: Tuple[pd.DataFrame, Dict[str, Any], int]) -> Tuple[str, Any, float, int]:
    """Worker function for parallel task execution"""
    data, task, process_id = args
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
        else:
            result = data
        
        execution_time = time.time() - start_time
        return (task_id, result, execution_time, process_id)
    
    except Exception as e:
        execution_time = time.time() - start_time
        return (task_id, f"Error: {str(e)}", execution_time, process_id)


class ParallelExecutor:
    """Executes tasks in parallel using execution plan from TaskPlanner"""
    
    def __init__(self, data: pd.DataFrame, num_processes: int = None):
        self.data = data
        self.num_processes = num_processes or mp.cpu_count()
        self.results = {}
        self.execution_times = {}
        self.task_process_map = {}
        self.layer_times = {}
    
    def merge_dependencies(self, dependency_results: List[Any]) -> Any:
        """
        Merge results from multiple dependencies.
        FIXED: Properly handle different result types.
        """
        if len(dependency_results) == 1:
            return dependency_results[0]
        
        # Check if all are GroupBy objects
        if all(isinstance(r, pd.core.groupby.DataFrameGroupBy) for r in dependency_results):
            # For GroupBy objects, they should be equivalent, take the first one
            return dependency_results[0]
        
        # Check if all are DataFrames
        if all(isinstance(r, pd.DataFrame) for r in dependency_results):
            # Merge DataFrames by finding intersection (AND logic for filters)
            result = dependency_results[0]
            for df in dependency_results[1:]:
                # Find common rows by index intersection
                common_indices = result.index.intersection(df.index)
                result = result.loc[common_indices]
            return result
        
        # Check if all are errors (strings)
        if all(isinstance(r, str) and r.startswith("Error:") for r in dependency_results):
            # Return combined error message
            return "Error: " + "; ".join([r.replace("Error: ", "") for r in dependency_results])
        
        # Mixed types or other cases - take first non-error result
        for result in dependency_results:
            if not (isinstance(result, str) and result.startswith("Error:")):
                return result
        
        # All failed - return first error
        return dependency_results[0]
    
    def execute_layer(self, layer: Dict[str, Any], 
                      current_results: Dict[str, Any]) -> Tuple[Dict[str, Any], float]:
        """Execute all tasks in a layer IN PARALLEL"""
        layer_start = time.time()
        layer_results = {}
        
        task_args = []
        for idx, task in enumerate(layer['tasks']):
            depends_on = task.get('depends_on', [])
            
            # FIXED: Handle multiple dependencies properly
            if depends_on:
                if len(depends_on) == 1:
                    input_data = current_results[depends_on[0]]
                else:
                    # Merge results from multiple dependencies
                    dependency_results = [current_results[dep] for dep in depends_on]
                    input_data = self.merge_dependencies(dependency_results)
            else:
                input_data = self.data
            
            task_args.append((input_data, task, idx))
        
        # Execute tasks
        if len(task_args) == 1:
            task_id, result, exec_time, proc_id = worker_execute_task(task_args[0])
            layer_results[task_id] = result
            self.execution_times[task_id] = exec_time
            self.task_process_map[task_id] = proc_id
        else:
            with mp.Pool(processes=min(self.num_processes, len(task_args))) as pool:
                results = pool.map(worker_execute_task, task_args)
            
            for task_id, result, exec_time, proc_id in results:
                layer_results[task_id] = result
                self.execution_times[task_id] = exec_time
                self.task_process_map[task_id] = proc_id
        
        layer_time = time.time() - layer_start
        self.layer_times[f'Layer_{layer["layer_id"]}'] = layer_time
        
        return layer_results, layer_time
    
    def execute_plan(self, execution_plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the complete execution plan"""
        total_start = time.time()
        current_results = {}
        
        print(f"\n{'='*80}")
        print(f"🚀 PARALLEL EXECUTION with {self.num_processes} PROCESSES")
        print(f"{'='*80}\n")
        
        for layer in execution_plan['execution_layers']:
            print(f"⏱️  Executing LAYER {layer['layer_id']}: {layer['num_tasks']} tasks")
            print(f"   Tasks: {layer['task_ids']}")
            
            layer_results, layer_time = self.execute_layer(layer, current_results)
            current_results.update(layer_results)
            
            print(f"   ✓ Layer {layer['layer_id']} completed in {layer_time:.3f}s\n")
        
        total_time = time.time() - total_start
        
        final_layer = execution_plan['execution_layers'][-1]
        final_results = {tid: current_results[tid] for tid in final_layer['task_ids']}
        
        return {
            'final_result': list(final_results.values())[0] if len(final_results) == 1 else final_results,
            'final_results': final_results,
            'all_results': current_results,
            'execution_times': self.execution_times,
            'layer_times': self.layer_times,
            'task_process_map': self.task_process_map,
            'total_execution_time': total_time,
            'num_processes_used': self.num_processes,
            'total_tasks': execution_plan['total_tasks']
        }
    
    def print_performance_report(self, results: Dict[str, Any]):
        """Print detailed performance metrics"""
        print(f"\n{'='*80}")
        print("📊 PERFORMANCE REPORT")
        print(f"{'='*80}\n")
        
        print(f"Configuration:")
        print(f"  • Number of Processes: {results['num_processes_used']}")
        print(f"  • Total Tasks: {results['total_tasks']}")
        print(f"  • Total Execution Time: {results['total_execution_time']:.3f}s\n")
        
        print(f"Layer-by-Layer Breakdown:")
        for level, time_taken in results['layer_times'].items():
            print(f"  • {level}: {time_taken:.3f}s")
        
        print(f"\nIndividual Task Times:")
        for task_id, time_taken in sorted(results['execution_times'].items()):
            proc_id = results['task_process_map'].get(task_id, 'N/A')
            print(f"  • {task_id}: {time_taken:.3f}s (Process {proc_id})")
        
        sequential_time = sum(results['execution_times'].values())
        speedup = sequential_time / results['total_execution_time']
        efficiency = (speedup / results['num_processes_used']) * 100
        
        print(f"\n{'='*80}")
        print(f"⚡ SPEEDUP ANALYSIS")
        print(f"{'='*80}")
        print(f"  • Sequential Time: {sequential_time:.3f}s")
        print(f"  • Parallel Time: {results['total_execution_time']:.3f}s")
        print(f"  • Speedup: {speedup:.2f}x")
        print(f"  • Efficiency: {efficiency:.1f}%")
        print(f"  • Time Saved: {sequential_time - results['total_execution_time']:.3f}s")
        print(f"{'='*80}\n")