"""
Task Planner
============
This module builds a Directed Acyclic Graph (DAG) from decomposed tasks
and creates an execution plan for parallel processing.

Features:
- Build DAG from task dependencies
- Identify tasks that can run in parallel
- Create execution layers
- Validate task dependencies
"""

from typing import List, Dict, Any, Set
from collections import defaultdict, deque


class TaskPlanner:
    """
    Creates an execution plan from decomposed tasks using DAG analysis.
    """
    
    def __init__(self, tasks: List[Dict[str, Any]]):
        """
        Initialize the planner with a list of tasks.
        
        Args:
            tasks: List of task dictionaries from NLP processor
        """
        self.tasks = {task['task_id']: task for task in tasks}
        self.dag = defaultdict(list)  # adjacency list
        self.in_degree = defaultdict(int)  # incoming edges count
        self.execution_plan = []
        
    def build_dag(self):
        """
        Build the DAG from task dependencies.
        Each task lists tasks it depends on (incoming edges).
        We need to reverse this to create outgoing edges for the DAG.
        """
        # Initialize in_degree for all tasks
        for task_id in self.tasks:
            self.in_degree[task_id] = 0
        
        # Build adjacency list and calculate in-degrees
        for task_id, task in self.tasks.items():
            depends_on = task.get('depends_on', [])
            
            for dependency in depends_on:
                # dependency -> task_id edge
                self.dag[dependency].append(task_id)
                self.in_degree[task_id] += 1
    
    def topological_sort(self) -> List[List[str]]:
        """
        Perform topological sort using Kahn's algorithm.
        Returns tasks grouped by execution layers (tasks in same layer can run in parallel).
        
        Returns:
            List of layers, where each layer is a list of task IDs
        """
        # Find all tasks with no dependencies (in_degree = 0)
        queue = deque([task_id for task_id in self.tasks if self.in_degree[task_id] == 0])
        
        layers = []
        visited = set()
        
        while queue:
            # All tasks in current queue can run in parallel
            current_layer = []
            layer_size = len(queue)
            
            for _ in range(layer_size):
                task_id = queue.popleft()
                current_layer.append(task_id)
                visited.add(task_id)
                
                # Reduce in-degree for dependent tasks
                for dependent in self.dag[task_id]:
                    self.in_degree[dependent] -= 1
                    
                    # If in-degree becomes 0, add to queue for next layer
                    if self.in_degree[dependent] == 0:
                        queue.append(dependent)
            
            if current_layer:
                layers.append(current_layer)
        
        # Check for cycles (if not all tasks visited)
        if len(visited) != len(self.tasks):
            raise ValueError("Cycle detected in task dependencies!")
        
        return layers
    
    def create_execution_plan(self) -> Dict[str, Any]:
        """
        Create a complete execution plan with parallel execution groups.
        
        Returns:
            Dictionary containing execution layers and metadata
        """
        self.build_dag()
        layers = self.topological_sort()
        
        # Build detailed execution plan
        execution_layers = []
        
        for layer_num, layer_tasks in enumerate(layers, 1):
            layer_info = {
                'layer_id': layer_num,
                'can_parallel': len(layer_tasks) > 1,
                'num_tasks': len(layer_tasks),
                'tasks': []
            }
            
            for task_id in layer_tasks:
                task = self.tasks[task_id]
                layer_info['tasks'].append({
                    'task_id': task_id,
                    'operation': task['operation'],
                    'details': {k: v for k, v in task.items() 
                               if k not in ['task_id', 'operation', 'depends_on']}
                })
            
            execution_layers.append(layer_info)
        
        plan = {
            'total_layers': len(execution_layers),
            'total_tasks': len(self.tasks),
            'max_parallelism': max(len(layer) for layer in layers),
            'execution_layers': execution_layers,
            'dag_structure': dict(self.dag)
        }
        
        self.execution_plan = plan
        return plan
    
    def get_parallel_groups(self) -> List[List[str]]:
        """
        Get groups of tasks that can be executed in parallel.
        
        Returns:
            List of task groups (each group can run in parallel)
        """
        if not self.execution_plan:
            self.create_execution_plan()
        
        return [[task['task_id'] for task in layer['tasks']] 
                for layer in self.execution_plan['execution_layers']]
    
    def visualize_dag(self) -> str:
        """
        Create a simple text visualization of the DAG.
        
        Returns:
            String representation of the DAG
        """
        if not self.execution_plan:
            self.create_execution_plan()
        
        viz = "Task Execution DAG:\n"
        viz += "=" * 50 + "\n\n"
        
        for layer in self.execution_plan['execution_layers']:
            viz += f"Layer {layer['layer_id']} "
            viz += f"(Parallel: {layer['can_parallel']}, Tasks: {layer['num_tasks']})\n"
            viz += "-" * 50 + "\n"
            
            for task in layer['tasks']:
                viz += f"  [{task['task_id']}] {task['operation']}\n"
                
                # Show dependencies
                original_task = self.tasks[task['task_id']]
                if original_task.get('depends_on'):
                    viz += f"    Depends on: {', '.join(original_task['depends_on'])}\n"
            
            viz += "\n"
        
        return viz


# Example usage
if __name__ == '__main__':
    # Sample tasks from NLP processor
    sample_tasks = [
        {
            'task_id': 'T1',
            'operation': 'filter',
            'conditions': [{'field': 'year', 'operator': '=', 'value': 2023}],
            'depends_on': []
        },
        {
            'task_id': 'T2',
            'operation': 'group',
            'group_by': ['region'],
            'depends_on': ['T1']
        },
        {
            'task_id': 'T3',
            'operation': 'aggregate',
            'agg_type': 'sum',
            'agg_field': 'sales',
            'depends_on': ['T2']
        }
    ]
    
    planner = TaskPlanner(sample_tasks)
    plan = planner.create_execution_plan()
    
    print("Execution Plan:")
    print("=" * 70)
    import json
    print(json.dumps(plan, indent=2))
    
    print("\n" + planner.visualize_dag())