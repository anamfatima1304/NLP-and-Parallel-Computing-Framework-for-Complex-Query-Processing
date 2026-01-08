import re
import json
import pandas as pd
import multiprocessing as mp
from typing import List, Dict, Any, Tuple
from collections import defaultdict, deque
import time
import numpy as np

class NLPQueryProcessor:
    """
    Processes natural language queries and extracts components.
    SIMPLIFIED: Only extracts WHAT to do, not HOW (no level assignment).
    """
    
    def __init__(self):
        self.filter_keywords = ['where', 'filter', 'with', 'having', 'that have', 'between']
        self.group_keywords = ['group by', 'grouped by', 'by', 'per', 'for each']
        self.aggregate_keywords = ['sum', 'total', 'average', 'avg', 'count', 'max', 'min', 'mean']
        
        self.known_fields = [
            'product', 'region', 'year', 'month', 'date', 'order_date',
            'sales', 'quantity', 'revenue', 'profit', 'discount',
            'category', 'customer', 'segment', 'ship_mode',
            'city', 'state', 'country', 'postal_code',
            'product_name', 'customer_id', 'product_id'
        ]
        
        self.numeric_fields = ['sales', 'quantity', 'revenue', 'profit', 'discount', 'year']
    
    def extract_conditions(self, query: str) -> List[Dict[str, Any]]:
        """Extract ALL filter conditions from the query"""
        query = query.lower().strip()
        conditions = []
        
        # Extract year conditions
        years = re.findall(r'\b(19|20)\d{2}\b', query)
        for year in years:
            conditions.append({
                'field': 'year',
                'operator': '=',
                'value': int(year)
            })
        
        # Extract "field > value" patterns
        greater_patterns = re.finditer(
            r'(sales|profit|quantity|discount|revenue)\s*(?:greater than|>)\s*(\d+\.?\d*)', 
            query
        )
        for match in greater_patterns:
            field, value = match.groups()
            conditions.append({
                'field': field,
                'operator': '>',
                'value': float(value)
            })
        
        # Extract "field < value" patterns
        less_patterns = re.finditer(
            r'(sales|profit|quantity|discount|revenue)\s*(?:less than|<)\s*(\d+\.?\d*)', 
            query
        )
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
        
        return conditions
    
    def extract_grouping_fields(self, query: str) -> List[str]:
        """Extract ALL grouping fields from the query"""
        query = query.lower().strip()
        group_fields = []
        
        # Look for explicit "by" patterns
        by_patterns = re.finditer(
            r'by\s+(region|category|product|segment|ship_mode|state|year|month)', 
            query
        )
        for match in by_patterns:
            field = match.group(1)
            if field not in group_fields:
                group_fields.append(field)
        
        # Look for "per" patterns
        per_patterns = re.finditer(
            r'per\s+(region|category|product|segment|customer)', 
            query
        )
        for match in per_patterns:
            field = match.group(1)
            if field not in group_fields:
                group_fields.append(field)
        
        return group_fields
    
    def extract_aggregations(self, query: str) -> List[Dict[str, Any]]:
        """Extract ALL aggregation operations from the query"""
        query = query.lower().strip()
        aggregations = []
        
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
        
        # Infer from context if no explicit aggregations
        if not aggregations:
            if 'total' in query or 'sum' in query:
                aggregations.append({'type': 'sum', 'field': 'sales'})
            if 'average' in query or 'avg' in query:
                aggregations.append({'type': 'avg', 'field': 'profit'})
            if 'count' in query:
                aggregations.append({'type': 'count', 'field': 'quantity'})
        
        return aggregations
    
    def parse_query(self, query: str) -> Dict[str, Any]:
        """
        Main function: Parse query into components WITHOUT dependency logic.
        Task planner will handle dependencies.
        """
        return {
            'original_query': query,
            'filters': self.extract_conditions(query),
            'groupings': self.extract_grouping_fields(query),
            'aggregations': self.extract_aggregations(query)
        }
