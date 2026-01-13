import re
import json
import pandas as pd
import multiprocessing as mp
from typing import List, Dict, Any, Tuple
from collections import defaultdict, deque
import time
import numpy as np

class TaskPlanner:
    """
    Creates execution plan with DAG analysis.
    NOW handles ALL dependency and level logic (removed from NLP processor).
    """
    
    def __init__(self):
        self.tasks = []
        self.dag = defaultdict(list)
        self.in_degree = defaultdict(int)
    
    def create_tasks_from_query(self, query_components: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Convert parsed query components into tasks with dependencies.
        This is the NEW responsibility moved from nlp_processor.
        
        FIXED: Create single group task with all grouping fields instead of separate tasks.
        """
        tasks = []
        task_id = 1
        filter_task_ids = []
        group_task_id = None
        
        # Create INDEPENDENT filter tasks
        for condition in query_components['filters']:
            tasks.append({
                'task_id': f'T{task_id}',
                'operation': 'filter',
                'conditions': [condition],
                'depends_on': []
            })
            filter_task_ids.append(f'T{task_id}')
            task_id += 1
        
        # Create SINGLE grouping task with ALL grouping fields (FIXED)
        if query_components['groupings']:
            tasks.append({
                'task_id': f'T{task_id}',
                'operation': 'group',
                'group_by': query_components['groupings'],  # All fields in one task
                'depends_on': filter_task_ids if filter_task_ids else []
            })
            group_task_id = f'T{task_id}'
            task_id += 1
        
        # Create INDEPENDENT aggregation tasks
        for agg in query_components['aggregations']:
            # Determine dependency: group task if exists, otherwise filter tasks
            if group_task_id:
                depends_on = [group_task_id]
            elif filter_task_ids:
                depends_on = filter_task_ids
            else:
                depends_on = []
            
            tasks.append({
                'task_id': f'T{task_id}',
                'operation': 'aggregate',
                'agg_type': agg['type'],
                'agg_field': agg['field'],
                'depends_on': depends_on
            })
            task_id += 1
        
        # If no tasks, create default fetch
        if not tasks:
            tasks.append({
                'task_id': 'T1',
                'operation': 'fetch',
                'conditions': [],
                'depends_on': []
            })
        
        self.tasks = {task['task_id']: task for task in tasks}
        return tasks
    
    def build_dag(self):
        """Build DAG from task dependencies"""
        for task_id in self.tasks:
            self.in_degree[task_id] = 0
        
        for task_id, task in self.tasks.items():
            depends_on = task.get('depends_on', [])
            for dependency in depends_on:
                self.dag[dependency].append(task_id)
                self.in_degree[task_id] += 1
    
    def topological_sort(self) -> List[List[str]]:
        """
        Perform topological sort to get execution layers.
        Returns tasks grouped by layers (parallel execution within layer).
        """
        queue = deque([task_id for task_id in self.tasks if self.in_degree[task_id] == 0])
        layers = []
        visited = set()
        
        while queue:
            current_layer = []
            layer_size = len(queue)
            
            for _ in range(layer_size):
                task_id = queue.popleft()
                current_layer.append(task_id)
                visited.add(task_id)
                
                for dependent in self.dag[task_id]:
                    self.in_degree[dependent] -= 1
                    if self.in_degree[dependent] == 0:
                        queue.append(dependent)
            
            if current_layer:
                layers.append(current_layer)
        
        if len(visited) != len(self.tasks):
            raise ValueError("Cycle detected in task dependencies!")
        
        return layers
    
    def create_execution_plan(self, query_components: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main function: Create complete execution plan with parallel layers.
        """
        # Step 1: Create tasks from parsed query
        tasks = self.create_tasks_from_query(query_components)
        
        # Step 2: Build DAG
        self.build_dag()
        
        # Step 3: Get execution layers via topological sort
        layers = self.topological_sort()
        
        # Step 4: Build detailed execution plan
        execution_layers = []
        for layer_num, layer_tasks in enumerate(layers):
            layer_info = {
                'layer_id': layer_num,
                'can_parallel': len(layer_tasks) > 1,
                'num_tasks': len(layer_tasks),
                'task_ids': layer_tasks,
                'tasks': [self.tasks[tid] for tid in layer_tasks]
            }
            execution_layers.append(layer_info)
        
        return {
            'original_query': query_components['original_query'],
            'total_layers': len(execution_layers),
            'total_tasks': len(self.tasks),
            'max_parallelism': max(len(layer['task_ids']) for layer in execution_layers),
            'execution_layers': execution_layers,
            'all_tasks': list(self.tasks.values())
        }
    
    def visualize_plan(self, plan: Dict[str, Any]) -> str:
        """Create visual representation of execution plan"""
        output = "\n" + "="*80 + "\n"
        output += "EXECUTION PLAN (DAG-based)\n"
        output += "="*80 + "\n\n"
        
        for layer in plan['execution_layers']:
            output += f"⏱️  LAYER {layer['layer_id']} - "
            output += f"{'PARALLEL' if layer['can_parallel'] else 'SEQUENTIAL'} "
            output += f"({layer['num_tasks']} tasks)\n"
            output += "-" * 80 + "\n"
            
            for task in layer['tasks']:
                output += f"  [{task['task_id']}] {task['operation'].upper()}"
                
                if task['operation'] == 'filter':
                    cond = task['conditions'][0]
                    output += f": {cond['field']} {cond['operator']} {cond['value']}"
                elif task['operation'] == 'group':
                    output += f": by {', '.join(task['group_by'])}"
                elif task['operation'] == 'aggregate':
                    output += f": {task['agg_type']}({task['agg_field']})"
                
                if task.get('depends_on'):
                    output += f"  [depends on: {', '.join(task['depends_on'])}]"
                
                output += "\n"
            output += "\n"
        
        output += "📊 SUMMARY\n"
        output += "-" * 80 + "\n"
        output += f"Total Tasks: {plan['total_tasks']}\n"
        output += f"Execution Layers: {plan['total_layers']}\n"
        output += f"Max Parallel Tasks: {plan['max_parallelism']}\n"
        
        return output
    
    