"""
NLP Query Processor
===================
This module handles natural language query understanding and decomposition.

Features:
- Tokenization and entity extraction
- Intent recognition (filter, group, aggregate, compare)
- Query decomposition into executable sub-tasks
- Output structured JSON format
"""

import re
import json
from typing import List, Dict, Any


class NLPQueryProcessor:
    """
    Processes natural language queries and decomposes them into structured tasks.
    """
    
    def __init__(self):
        # Keywords for different operations
        self.filter_keywords = ['where', 'filter', 'with', 'having', 'that have']
        self.group_keywords = ['group by', 'grouped by', 'by', 'per', 'for each']
        self.aggregate_keywords = ['sum', 'total', 'average', 'avg', 'count', 'max', 'min', 'mean']
        self.compare_keywords = ['compare', 'difference', 'versus', 'vs', 'between']
        
        # Common fields in dataset
        self.known_fields = ['product', 'region', 'year', 'month', 'sales', 'quantity', 
                            'revenue', 'profit', 'category', 'customer']
        
        # Stop words to remove
        self.stop_words = ['the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 
                          'to', 'for', 'of', 'with', 'by', 'from', 'show', 'me', 
                          'get', 'find', 'all']
    
    def preprocess(self, query: str) -> str:
        """
        Preprocess the query by converting to lowercase and cleaning.
        """
        query = query.lower().strip()
        return query
    
    def extract_entities(self, query: str) -> Dict[str, Any]:
        """
        Extract entities like fields, values, and operators from the query.
        
        Args:
            query: Natural language query string
            
        Returns:
            Dictionary containing extracted entities
        """
        entities = {
            'fields': [],
            'values': [],
            'operators': [],
            'years': [],
            'conditions': []
        }
        
        # Extract years (4-digit numbers)
        years = re.findall(r'\b(19|20)\d{2}\b', query)
        entities['years'] = [int(year) for year in years]
        
        # Extract known fields
        for field in self.known_fields:
            if field in query:
                entities['fields'].append(field)
        
        # Extract comparison operators
        if '>' in query:
            entities['operators'].append('>')
        if '<' in query:
            entities['operators'].append('<')
        if '>=' in query or 'greater than or equal' in query:
            entities['operators'].append('>=')
        if '<=' in query or 'less than or equal' in query:
            entities['operators'].append('<=')
        if '=' in query or 'equal' in query:
            entities['operators'].append('=')
        
        # Extract numeric values
        numbers = re.findall(r'\b\d+\.?\d*\b', query)
        entities['values'] = [float(n) for n in numbers if float(n) not in entities['years']]
        
        return entities
    
    def detect_intent(self, query: str) -> List[str]:
        """
        Detect the intent/operations in the query.
        
        Returns:
            List of detected intents (e.g., ['filter', 'group', 'aggregate'])
        """
        intents = []
        
        # Check for filtering intent
        if any(keyword in query for keyword in self.filter_keywords):
            intents.append('filter')
        
        # Check for grouping intent
        if any(keyword in query for keyword in self.group_keywords):
            intents.append('group')
        
        # Check for aggregation intent
        if any(keyword in query for keyword in self.aggregate_keywords):
            intents.append('aggregate')
        
        # Check for comparison intent
        if any(keyword in query for keyword in self.compare_keywords):
            intents.append('compare')
        
        # Default to filter if no intent detected
        if not intents:
            intents.append('filter')
        
        return intents
    
    def extract_aggregation_type(self, query: str) -> str:
        """
        Determine the type of aggregation (sum, avg, count, etc.)
        """
        if 'sum' in query or 'total' in query:
            return 'sum'
        elif 'average' in query or 'avg' in query or 'mean' in query:
            return 'avg'
        elif 'count' in query or 'number of' in query:
            return 'count'
        elif 'max' in query or 'maximum' in query or 'highest' in query:
            return 'max'
        elif 'min' in query or 'minimum' in query or 'lowest' in query:
            return 'min'
        else:
            return 'sum'  # Default
    
    def decompose_query(self, query: str) -> Dict[str, Any]:
        """
        Main function to decompose a natural language query into structured tasks.
        
        Args:
            query: Natural language query string
            
        Returns:
            Dictionary containing decomposed tasks and metadata
        """
        # Preprocess
        query = self.preprocess(query)
        
        # Extract entities and intents
        entities = self.extract_entities(query)
        intents = self.detect_intent(query)
        
        # Build tasks based on intents
        tasks = []
        task_id = 1
        
        # Create filter task if filtering is needed
        if 'filter' in intents or entities['years'] or entities['operators']:
            filter_conditions = []
            
            # Add year filters
            if entities['years']:
                for year in entities['years']:
                    filter_conditions.append({
                        'field': 'year',
                        'operator': '=',
                        'value': year
                    })
            
            # Add value filters
            if entities['operators'] and entities['values']:
                for i, op in enumerate(entities['operators']):
                    if i < len(entities['values']):
                        # Try to match with a field
                        field = 'sales'  # Default field
                        if entities['fields'] and len(entities['fields']) > i:
                            field = entities['fields'][i]
                        
                        filter_conditions.append({
                            'field': field,
                            'operator': op,
                            'value': entities['values'][i]
                        })
            
            if filter_conditions:
                tasks.append({
                    'task_id': f'T{task_id}',
                    'operation': 'filter',
                    'conditions': filter_conditions,
                    'depends_on': []
                })
                task_id += 1
        
        # Create group task if grouping is needed
        if 'group' in intents:
            group_by = []
            
            # Determine grouping fields
            if 'region' in query:
                group_by.append('region')
            if 'product' in query or 'category' in query:
                group_by.append('product')
            if 'year' in query and 'year' not in [e['field'] for e in tasks[0]['conditions']] if tasks else True:
                group_by.append('year')
            if 'month' in query:
                group_by.append('month')
            
            if not group_by:
                group_by = ['region']  # Default grouping
            
            tasks.append({
                'task_id': f'T{task_id}',
                'operation': 'group',
                'group_by': group_by,
                'depends_on': [tasks[-1]['task_id']] if tasks else []
            })
            task_id += 1
        
        # Create aggregate task if aggregation is needed
        if 'aggregate' in intents or any(kw in query for kw in self.aggregate_keywords):
            agg_type = self.extract_aggregation_type(query)
            agg_field = 'sales'  # Default
            
            # Determine aggregation field
            if 'revenue' in query:
                agg_field = 'revenue'
            elif 'profit' in query:
                agg_field = 'profit'
            elif 'quantity' in query:
                agg_field = 'quantity'
            
            tasks.append({
                'task_id': f'T{task_id}',
                'operation': 'aggregate',
                'agg_type': agg_type,
                'agg_field': agg_field,
                'depends_on': [tasks[-1]['task_id']] if tasks else []
            })
            task_id += 1
        
        # If no tasks created, create a default "fetch all" task
        if not tasks:
            tasks.append({
                'task_id': 'T1',
                'operation': 'fetch',
                'conditions': [],
                'depends_on': []
            })
        
        # Build result structure
        result = {
            'original_query': query,
            'intents': intents,
            'entities': entities,
            'tasks': tasks,
            'total_tasks': len(tasks)
        }
        
        return result
    
    def get_task_json(self, query: str) -> str:
        """
        Get decomposed tasks in JSON format.
        """
        decomposed = self.decompose_query(query)
        return json.dumps(decomposed, indent=2)


# Example usage
if __name__ == '__main__':
    processor = NLPQueryProcessor()
    
    # Test queries
    test_queries = [
        "Show me total sales by region for 2023",
        "Find all products where sales > 10000 and group by category",
        "Compare average revenue between 2022 and 2023",
        "What is the total profit by region where year = 2023"
    ]
    
    for query in test_queries:
        print(f"\nQuery: {query}")
        print("=" * 70)
        result = processor.decompose_query(query)
        print(json.dumps(result, indent=2))