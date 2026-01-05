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
"""

import pandas as pd
import multiprocessing as mp
from typing import List, Dict, Any, Tuple
import time
import numpy as np


def simulate_computation(complexity: int = 500000):
    """
    Simulate computational workload to make parallelization benefits visible.
    This represents real-world data processing operations.
    """
    result = 0
    for _ in range(complexity):
        result += np.random.random()
    return result


def execute_filter_task(data: pd.DataFrame, task: Dict[str, Any]) -> pd.DataFrame:
    """
    Execute a filter operation on the dataset.
    """
    # Simulate computation time
    simulate_computation(500000)
    
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
    """
    # Simulate computation time
    simulate_computation(300000)
    
    group_by = task.get('group_by', [])
    
    if not group_by:
        return data
    
    # Return grouped data
    grouped = data.groupby(group_by, as_index=False)
    return grouped


def execute_aggregate_task(data, task: Dict[str, Any]) -> pd.DataFrame:
    """
    Execute an aggregation operation on the dataset.
    """
    # Simulate computation time
    simulate_computation(400000)
    
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
    """
    return data


def worker_execute_task(args: Tuple[pd.DataFrame, Dict[str, Any], int]) -> Tuple[str, Any, float, int]:
    """
    Worker function to execute a single task.
    This function is called by each worker process.
    
    Args:
        args: Tuple of (data, task, process_id)
        
    Returns:
        Tuple of (task_id, result, execution_time, process_id)
    """
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
        elif operation == 'fetch':
            result = execute_fetch_task(data, task)
        else:
            result = data
        
        execution_time = time.time() - start_time
        return (task_id, result, execution_time, process_id)
    
    except Exception as e:
        execution_time = time.time() - start_time
        return (task_id, f"Error: {str(e)}", execution_time, process_id)


class ParallelExecutor:
    """
    Executes tasks in parallel using multiprocessing.
    UPDATED: Now supports level-based execution from NLP processor.
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
        self.task_process_map = {}
        self.level_times = {}
    
    def execute_level(self, level_tasks: List[Dict[str, Any]], 
                      current_results: Dict[str, Any],
                      level_id: int) -> Tuple[Dict[str, Any], float]:
        """
        Execute all tasks in a level IN PARALLEL.
        
        Args:
            level_tasks: List of tasks at this level
            current_results: Results from previous levels
            level_id: Current level number
            
        Returns:
            Tuple of (results_dict, level_execution_time)
        """
        level_start = time.time()
        level_results = {}
        
        # Prepare arguments for each task
        task_args = []
        for idx, task in enumerate(level_tasks):
            depends_on = task.get('depends_on', [])
            
            # Get input data
            if depends_on:
                # Use first dependency's result (all dependencies should have same data)
                input_data = current_results[depends_on[0]]
            else:
                # Use original data
                input_data = self.data
            
            task_args.append((input_data, task, idx))
        
        # Execute in parallel
        if len(task_args) == 1:
            # Single task - no need for multiprocessing
            task_id, result, exec_time, proc_id = worker_execute_task(task_args[0])
            level_results[task_id] = result
            self.execution_times[task_id] = exec_time
            self.task_process_map[task_id] = proc_id
        else:
            # Multiple tasks - TRUE PARALLEL EXECUTION
            with mp.Pool(processes=min(self.num_processes, len(task_args))) as pool:
                results = pool.map(worker_execute_task, task_args)
            
            for task_id, result, exec_time, proc_id in results:
                level_results[task_id] = result
                self.execution_times[task_id] = exec_time
                self.task_process_map[task_id] = proc_id
        
        level_time = time.time() - level_start
        self.level_times[f'Level_{level_id}'] = level_time
        
        return level_results, level_time
    
    def execute_plan(self, tasks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Execute tasks organized by dependency levels.
        
        This method receives tasks directly from nlp_processor.py
        and executes them level by level.
        
        Args:
            tasks: List of task dictionaries from NLP processor
            
        Returns:
            Dictionary containing execution results and metrics
        """
        total_start = time.time()
        
        # Organize tasks by level
        levels = {}
        for task in tasks:
            level = task.get('level', 0)
            if level not in levels:
                levels[level] = []
            levels[level].append(task)
        
        current_results = {}
        
        print(f"\n{'='*80}")
        print(f"🚀 PARALLEL EXECUTION with {self.num_processes} PROCESSES")
        print(f"{'='*80}\n")
        
        # Execute each level
        for level_id in sorted(levels.keys()):
            level_tasks = levels[level_id]
            
            print(f"⏱️  Executing LEVEL {level_id}: {len(level_tasks)} tasks in PARALLEL")
            print(f"   Tasks: {[t['task_id'] for t in level_tasks]}")
            
            level_results, level_time = self.execute_level(
                level_tasks, current_results, level_id
            )
            
            current_results.update(level_results)
            
            print(f"   ✓ Level {level_id} completed in {level_time:.3f}s")
            print()
        
        total_time = time.time() - total_start
        
        # Get final results (last level's tasks)
        final_level = max(levels.keys())
        final_results = {tid: current_results[tid] for tid in [t['task_id'] for t in levels[final_level]]}
        
        return {
            'final_result': list(final_results.values())[0] if len(final_results) == 1 else final_results,
            'final_results': final_results,
            'all_results': current_results,
            'execution_times': self.execution_times,
            'level_times': self.level_times,
            'task_process_map': self.task_process_map,
            'total_execution_time': total_time,
            'num_processes_used': self.num_processes,
            'total_tasks': len(tasks)
        }
    
    def print_performance_report(self, results: Dict[str, Any]):
        """
        Print detailed performance metrics.
        """
        print(f"\n{'='*80}")
        print("📊 PERFORMANCE REPORT")
        print(f"{'='*80}\n")
        
        print(f"Configuration:")
        print(f"  • Number of Processes: {results['num_processes_used']}")
        print(f"  • Total Tasks: {results['total_tasks']}")
        print(f"  • Total Execution Time: {results['total_execution_time']:.3f}s\n")
        
        print(f"Level-by-Level Breakdown:")
        for level, time_taken in results['level_times'].items():
            print(f"  • {level}: {time_taken:.3f}s")
        
        print(f"\nIndividual Task Times:")
        for task_id, time_taken in sorted(results['execution_times'].items()):
            proc_id = results['task_process_map'].get(task_id, 'N/A')
            print(f"  • {task_id}: {time_taken:.3f}s (Process {proc_id})")
        
        # Calculate sequential time (sum of all task times)
        sequential_time = sum(results['execution_times'].values())
        speedup = sequential_time / results['total_execution_time']
        efficiency = (speedup / results['num_processes_used']) * 100
        
        print(f"\n{'='*80}")
        print(f"⚡ SPEEDUP ANALYSIS")
        print(f"{'='*80}")
        print(f"  • Sequential Time (sum of all tasks): {sequential_time:.3f}s")
        print(f"  • Parallel Time (actual): {results['total_execution_time']:.3f}s")
        print(f"  • Speedup: {speedup:.2f}x")
        print(f"  • Efficiency: {efficiency:.1f}%")
        print(f"  • Time Saved: {sequential_time - results['total_execution_time']:.3f}s")
        print(f"{'='*80}\n")


# Example usage
if __name__ == '__main__':
    # Import the NLP processor to get tasks
    from nlp_processor import ParallelNLPQueryProcessor
    
    # Create sample dataset
    print("Creating sample dataset...")
    data = pd.DataFrame({
        'product': ['A', 'B', 'C', 'D'] * 2500,
        'region': ['North', 'South', 'East', 'West'] * 2500,
        'category': ['Electronics', 'Furniture', 'Office'] * 3333 + ['Electronics'],
        'segment': ['Consumer', 'Corporate', 'Home Office'] * 3333 + ['Consumer'],
        'year': [2015, 2016, 2017, 2018] * 2500,
        'sales': np.random.uniform(1000, 50000, 10000),
        'profit': np.random.uniform(100, 5000, 10000),
        'quantity': np.random.randint(1, 100, 10000),
        'discount': np.random.uniform(0, 0.3, 10000)
    })
    
    print(f"Dataset size: {len(data)} rows\n")
    
    # Test with NLP processor
    processor = ParallelNLPQueryProcessor()
    
    # Test query
    query = "Show me total sales and average profit by region and category for year 2017 where sales greater than 5000 and discount less than 0.2"
    
    print(f"Query: {query}\n")
    print("="*80)
    
    # Get tasks from NLP processor
    result = processor.decompose_query_parallel(query)
    tasks = result['tasks']
    
    print(f"\n✓ NLP Processing completed")
    print(f"  Total Tasks: {len(tasks)}")
    print(f"  Tasks: {[t['task_id'] for t in tasks]}\n")
    
    # Execute with different processor counts
    for num_proc in [1, 2, 4]:
        print(f"\n{'='*80}")
        print(f"Testing with {num_proc} processor(s)")
        print(f"{'='*80}")
        
        executor = ParallelExecutor(data, num_processes=num_proc)
        execution_results = executor.execute_plan(tasks)
        executor.print_performance_report(execution_results)
        
        print(f"\n✓ Final Result Preview:")
        final_result = execution_results['final_result']
        if isinstance(final_result, pd.DataFrame):
            print(final_result.head())