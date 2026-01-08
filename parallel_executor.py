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


def execute_filter_task(data: pd.DataFrame, task: Dict[str, Any]) -> pd.DataFrame:
    """Execute filter operation"""
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
    
    return result


def execute_group_task(data: pd.DataFrame, task: Dict[str, Any]) -> pd.DataFrame:
    """Execute grouping operation"""
    simulate_computation(300000)
    group_by = task.get('group_by', [])
    if not group_by:
        return data
    return data.groupby(group_by, as_index=False)


def execute_aggregate_task(data, task: Dict[str, Any]) -> pd.DataFrame:
    """Execute aggregation operation"""
    simulate_computation(400000)
    agg_type = task.get('agg_type', 'sum')
    agg_field = task.get('agg_field', 'sales')
    
    if isinstance(data, pd.core.groupby.DataFrameGroupBy):
        if agg_type == 'sum':
            result = data[agg_field].sum().reset_index()
        elif agg_type in ['avg', 'mean']:
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
        if agg_type == 'sum':
            value = data[agg_field].sum()
        elif agg_type in ['avg', 'mean']:
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
    
    def execute_layer(self, layer: Dict[str, Any], 
                      current_results: Dict[str, Any]) -> Tuple[Dict[str, Any], float]:
        """Execute all tasks in a layer IN PARALLEL"""
        layer_start = time.time()
        layer_results = {}
        
        task_args = []
        for idx, task in enumerate(layer['tasks']):
            depends_on = task.get('depends_on', [])
            
            if depends_on:
                input_data = current_results[depends_on[0]]
            else:
                input_data = self.data
            
            task_args.append((input_data, task, idx))
        
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
