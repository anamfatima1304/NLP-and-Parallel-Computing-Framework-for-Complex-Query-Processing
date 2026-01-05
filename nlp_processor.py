"""
Enhanced NLP Query Processor with Parallel Task Generation
===========================================================
This version creates INDEPENDENT tasks that can execute in parallel,
rather than sequential pipelines.
"""

import re
import json
from typing import List, Dict, Any


class ParallelNLPQueryProcessor:
    """
    Processes natural language queries and decomposes them into PARALLEL tasks.
    Key difference: Creates independent filter/group/aggregate tasks instead of pipelines.
    """
    
    def __init__(self):
        # Keywords for different operations
        self.filter_keywords = ['where', 'filter', 'with', 'having', 'that have', 'between']
        self.group_keywords = ['group by', 'grouped by', 'by', 'per', 'for each']
        self.aggregate_keywords = ['sum', 'total', 'average', 'avg', 'count', 'max', 'min', 'mean']
        self.compare_keywords = ['compare', 'difference', 'versus', 'vs', 'between', 'and']
        
        # Common fields matching Kaggle Superstore dataset
        self.known_fields = [
            'product', 'region', 'year', 'month', 'date', 'order_date',
            'sales', 'quantity', 'revenue', 'profit', 'discount',
            'category', 'customer', 'segment', 'ship_mode',
            'city', 'state', 'country', 'postal_code',
            'product_name', 'customer_id', 'product_id'
        ]
        
        self.numeric_fields = ['sales', 'quantity', 'revenue', 'profit', 'discount', 'year']
    
    def preprocess(self, query: str) -> str:
        """Preprocess the query by converting to lowercase and cleaning."""
        query = query.lower().strip()
        return query
    
    def extract_all_conditions(self, query: str) -> List[Dict[str, Any]]:
        """
        Extract ALL filter conditions from the query.
        This creates MULTIPLE independent filter tasks.
        """
        conditions = []
        
        # Extract year conditions (can be multiple years)
        years = re.findall(r'\b(19|20)\d{2}\b', query)
        for year in years:
            conditions.append({
                'field': 'year',
                'operator': '=',
                'value': int(year)
            })
        
        # Extract "field > value" patterns
        greater_patterns = re.finditer(r'(sales|profit|quantity|discount|revenue)\s*(?:greater than|>)\s*(\d+\.?\d*)', query)
        for match in greater_patterns:
            field, value = match.groups()
            conditions.append({
                'field': field,
                'operator': '>',
                'value': float(value)
            })
        
        # Extract "field < value" patterns
        less_patterns = re.finditer(r'(sales|profit|quantity|discount|revenue)\s*(?:less than|<)\s*(\d+\.?\d*)', query)
        for match in less_patterns:
            field, value = match.groups()
            conditions.append({
                'field': field,
                'operator': '<',
                'value': float(value)
            })
        
        # Extract "field between X and Y" patterns
        between_patterns = re.finditer(
            r'(sales|profit|quantity|discount|revenue)\s+between\s+(\d+\.?\d*)\s+and\s+(\d+\.?\d*)', 
            query
        )
        for match in between_patterns:
            field, lower, upper = match.groups()
            conditions.append({
                'field': field,
                'operator': '>=',
                'value': float(lower)
            })
            conditions.append({
                'field': field,
                'operator': '<=',
                'value': float(upper)
            })
        
        # Extract "quantity greater than X" style patterns
        qty_patterns = re.finditer(r'quantity\s+(?:greater than|>)\s+(\d+)', query)
        for match in qty_patterns:
            value = match.group(1)
            conditions.append({
                'field': 'quantity',
                'operator': '>',
                'value': float(value)
            })
        
        return conditions
    
    def extract_grouping_fields(self, query: str) -> List[str]:
        """
        Extract ALL grouping fields from the query.
        Each unique grouping creates a separate task.
        """
        group_fields = []
        
        # Look for explicit "by" patterns
        by_patterns = re.finditer(r'by\s+(region|category|product|segment|ship_mode|state|year|month)', query)
        for match in by_patterns:
            field = match.group(1)
            if field not in group_fields:
                group_fields.append(field)
        
        # Look for "per" patterns
        per_patterns = re.finditer(r'per\s+(region|category|product|segment|customer)', query)
        for match in per_patterns:
            field = match.group(1)
            if field not in group_fields:
                group_fields.append(field)
        
        # Look for implicit grouping in phrase structure
        for field in ['region', 'category', 'product', 'segment', 'ship_mode', 'state']:
            if field in query and field not in group_fields:
                # Check if it's in a grouping context (not just a filter)
                if any(kw in query for kw in self.group_keywords):
                    group_fields.append(field)
        
        return group_fields
    
    def extract_aggregations(self, query: str) -> List[Dict[str, Any]]:
        """
        Extract ALL aggregation operations from the query.
        Each aggregation becomes a separate task.
        """
        aggregations = []
        
        # Pattern: "sum of sales", "total sales", "average profit", etc.
        agg_patterns = [
            (r'\b(sum|total)\s+(?:of\s+)?(sales|revenue|profit|quantity|discount)', 'sum'),
            (r'\b(average|avg|mean)\s+(?:of\s+)?(sales|revenue|profit|quantity|discount)', 'avg'),
            (r'\b(count|number)\s+(?:of\s+)?(sales|orders|customers|products|quantity)', 'count'),
            (r'\b(max|maximum|highest)\s+(?:of\s+)?(sales|revenue|profit|quantity)', 'max'),
            (r'\b(min|minimum|lowest)\s+(?:of\s+)?(sales|revenue|profit|quantity|discount)', 'min'),
        ]
        
        for pattern, agg_type in agg_patterns:
            matches = re.finditer(pattern, query)
            for match in matches:
                field = match.group(2)
                aggregations.append({
                    'type': agg_type,
                    'field': field
                })
        
        # If no explicit aggregations found, infer from context
        if not aggregations:
            if 'total' in query or 'sum' in query:
                aggregations.append({'type': 'sum', 'field': 'sales'})
            if 'average' in query or 'avg' in query:
                aggregations.append({'type': 'avg', 'field': 'profit'})
            if 'count' in query:
                aggregations.append({'type': 'count', 'field': 'quantity'})
        
        return aggregations
    
    def decompose_query_parallel(self, query: str) -> Dict[str, Any]:
        """
        Main function to decompose query into PARALLEL tasks.
        
        Key difference from original: Creates independent tasks at same level
        instead of sequential dependencies.
        """
        query = self.preprocess(query)
        
        tasks = []
        task_id = 1
        filter_task_ids = []
        group_task_ids = []
        
        # LEVEL 0: Create INDEPENDENT filter tasks (can run in parallel)
        conditions = self.extract_all_conditions(query)
        for condition in conditions:
            tasks.append({
                'task_id': f'T{task_id}',
                'operation': 'filter',
                'conditions': [condition],  # ONE condition per task
                'depends_on': [],  # NO dependencies = can run in parallel
                'level': 0
            })
            filter_task_ids.append(f'T{task_id}')
            task_id += 1
        
        # LEVEL 1: Create INDEPENDENT grouping tasks (can run in parallel after filters)
        group_fields = self.extract_grouping_fields(query)
        for field in group_fields:
            tasks.append({
                'task_id': f'T{task_id}',
                'operation': 'group',
                'group_by': [field],  # ONE field per task
                'depends_on': filter_task_ids,  # Wait for ALL filters
                'level': 1
            })
            group_task_ids.append(f'T{task_id}')
            task_id += 1
        
        # LEVEL 2: Create INDEPENDENT aggregation tasks (can run in parallel after grouping)
        aggregations = self.extract_aggregations(query)
        for agg in aggregations:
            tasks.append({
                'task_id': f'T{task_id}',
                'operation': 'aggregate',
                'agg_type': agg['type'],
                'agg_field': agg['field'],
                'depends_on': group_task_ids if group_task_ids else filter_task_ids,
                'level': 2
            })
            task_id += 1
        
        # If no tasks created, create a default fetch task
        if not tasks:
            tasks.append({
                'task_id': 'T1',
                'operation': 'fetch',
                'conditions': [],
                'depends_on': [],
                'level': 0
            })
        
        # Calculate parallelization metrics
        levels = {}
        for task in tasks:
            level = task.get('level', 0)
            if level not in levels:
                levels[level] = []
            levels[level].append(task['task_id'])
        
        result = {
            'original_query': query,
            'tasks': tasks,
            'total_tasks': len(tasks),
            'parallelization_info': {
                'total_levels': len(levels),
                'tasks_per_level': {f'Level {k}': len(v) for k, v in levels.items()},
                'max_parallel_tasks': max(len(v) for v in levels.values()) if levels else 0,
                'theoretical_speedup': round(len(tasks) / len(levels), 2) if levels else 1.0
            }
        }
        
        return result
    
    def visualize_parallel_execution(self, result: Dict[str, Any]) -> str:
        """
        Create a visual representation showing parallel execution.
        """
        output = "\n" + "="*80 + "\n"
        output += "PARALLEL EXECUTION PLAN\n"
        output += "="*80 + "\n\n"
        
        # Group by level
        levels = {}
        for task in result['tasks']:
            level = task.get('level', 0)
            if level not in levels:
                levels[level] = []
            levels[level].append(task)
        
        # Display each level
        for level in sorted(levels.keys()):
            output += f"⏱️  LEVEL {level} - Execute in PARALLEL ({len(levels[level])} tasks)\n"
            output += "-" * 80 + "\n"
            
            for task in levels[level]:
                output += f"  [{task['task_id']}] {task['operation'].upper()}: "
                
                if task['operation'] == 'filter':
                    cond = task['conditions'][0]
                    output += f"{cond['field']} {cond['operator']} {cond['value']}"
                elif task['operation'] == 'group':
                    output += f"by {task['group_by'][0]}"
                elif task['operation'] == 'aggregate':
                    output += f"{task['agg_type']}({task['agg_field']})"
                
                if task.get('depends_on'):
                    output += f"  [waits for: {', '.join(task['depends_on'])}]"
                
                output += "\n"
            
            output += "\n"
        
        # Summary
        info = result['parallelization_info']
        output += "📊 PARALLELIZATION SUMMARY\n"
        output += "-" * 80 + "\n"
        output += f"Total Tasks: {result['total_tasks']}\n"
        output += f"Execution Levels: {info['total_levels']}\n"
        output += f"Max Parallel Tasks: {info['max_parallel_tasks']}\n"
        output += f"Theoretical Speedup: {info['theoretical_speedup']}x\n"
        output += f"Tasks per Level: {info['tasks_per_level']}\n"
        
        return output


# Test with complex queries
if __name__ == '__main__':
    processor = ParallelNLPQueryProcessor()
    
    test_queries = [
        """Show me the total sales and average profit by region and category 
        for year 2023 where sales greater than 5000 and discount less than 0.2, 
        and also count the number of orders for each segment""",
        
        # """Compare the sum of sales, maximum profit, minimum discount, and count of quantity
        # grouped by region, category, and segment for years 2022 and 2023
        # where profit greater than 1000""",
        
        # """Find total revenue and average discount by region, product, and ship_mode
        # for year 2023 where sales between 1000 and 50000 and quantity greater than 5"""
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n{'='*80}")
        print(f"TEST QUERY {i}")
        print(f"{'='*80}")
        print(f"Query: {query}\n")
        
        result = processor.decompose_query_parallel(query)
        
        # Show JSON output
        print("JSON OUTPUT:")
        print(json.dumps(result, indent=2))
        
        # Show visualization
        print(processor.visualize_parallel_execution(result))
        print("\n")