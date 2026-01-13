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
    
    FIXED:
    - Proper year extraction (2017, not 20)
    - Better grouping field detection (finds "category" now)
    - Column names match CSV structure
    """
    
    def __init__(self):
        self.filter_keywords = ['where', 'filter', 'with', 'having', 'that have', 'between']
        self.group_keywords = ['group by', 'grouped by', 'by', 'per', 'for each']
        self.aggregate_keywords = ['sum', 'total', 'average', 'avg', 'count', 'max', 'min', 'mean']
        
        self.known_fields = [
            'product', 'region', 'year', 'month', 'date', 'order_date', 'order date',
            'sales', 'quantity', 'revenue', 'profit', 'discount',
            'category', 'customer', 'segment', 'ship_mode', 'ship mode',
            'city', 'state', 'country', 'postal_code', 'postal code',
            'product_name', 'product name', 'customer_id', 'customer id', 'product_id', 'product id'
        ]
        
        self.numeric_fields = ['sales', 'quantity', 'revenue', 'profit', 'discount', 'year']
    
    def extract_conditions(self, query: str) -> List[Dict[str, Any]]:
        """Extract ALL filter conditions from the query"""
        query_lower = query.lower().strip()
        conditions = []
        
        # FIXED: Extract full year (2017, not 20)
        year_patterns = re.finditer(r'\b(year|for)\s+(19|20)(\d{2})\b', query_lower)
        for match in year_patterns:
            year = match.group(2) + match.group(3)  # Combine to get full year like "2017"
            conditions.append({
                'field': 'year',
                'operator': '=',
                'value': int(year)
            })
        
        # If no explicit "year" keyword, try standalone 4-digit years
        if not any(c['field'] == 'year' for c in conditions):
            standalone_years = re.findall(r'\b(19|20)\d{2}\b', query_lower)
            for year in standalone_years:
                conditions.append({
                    'field': 'year',
                    'operator': '=',
                    'value': int(year)
                })
        
        # Extract "field > value" patterns
        greater_patterns = re.finditer(
            r'(sales|profit|quantity|discount|revenue)\s*(?:greater than|>|above)\s*(\d+\.?\d*)', 
            query_lower
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
            r'(sales|profit|quantity|discount|revenue)\s*(?:less than|<|below)\s*(\d+\.?\d*)', 
            query_lower
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
            query_lower
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
        query_lower = query.lower().strip()
        group_fields = []
        
        # FIXED: Better pattern to catch "by region and category"
        # Look for "by X and Y" pattern first
        combined_pattern = re.search(
            r'by\s+([\w\s]+?)\s+(?:where|for|with|$)', 
            query_lower
        )
        if combined_pattern:
            fields_text = combined_pattern.group(1)
            # Split on "and", "," or whitespace
            potential_fields = re.split(r'\s+and\s+|,\s*|\s+', fields_text.strip())
            
            for field in potential_fields:
                field = field.strip()
                if field in ['region', 'category', 'product', 'segment', 'ship_mode', 'ship mode',
                            'state', 'year', 'month', 'city', 'country', 'customer']:
                    if field not in group_fields:
                        # Normalize field names
                        if field == 'ship mode':
                            field = 'ship_mode'
                        group_fields.append(field)
        
        # Fallback: Look for individual "by X" patterns if nothing found
        if not group_fields:
            by_patterns = re.finditer(
                r'by\s+(region|category|product|segment|ship_mode|ship mode|state|year|month)', 
                query_lower
            )
            for match in by_patterns:
                field = match.group(1)
                if field == 'ship mode':
                    field = 'ship_mode'
                if field not in group_fields:
                    group_fields.append(field)
        
        # Look for "per" patterns
        per_patterns = re.finditer(
            r'per\s+(region|category|product|segment|customer)', 
            query_lower
        )
        for match in per_patterns:
            field = match.group(1)
            if field not in group_fields:
                group_fields.append(field)
        
        return group_fields
    
    def extract_aggregations(self, query: str) -> List[Dict[str, Any]]:
        """Extract ALL aggregation operations from the query"""
        query_lower = query.lower().strip()
        aggregations = []
        
        agg_patterns = [
            (r'\b(sum|total)\s+(?:of\s+)?(sales|revenue|profit|quantity|discount)', 'sum'),
            (r'\b(average|avg|mean)\s+(?:of\s+)?(sales|revenue|profit|quantity|discount)', 'avg'),
            (r'\b(count|number)\s+(?:of\s+)?(sales|orders|customers|products|quantity)', 'count'),
            (r'\b(max|maximum|highest)\s+(?:of\s+)?(sales|revenue|profit|quantity)', 'max'),
            (r'\b(min|minimum|lowest)\s+(?:of\s+)?(sales|revenue|profit|quantity|discount)', 'min'),
        ]
        
        for pattern, agg_type in agg_patterns:
            matches = re.finditer(pattern, query_lower)
            for match in matches:
                field = match.group(2)
                # Check if this aggregation is already added
                if not any(a['type'] == agg_type and a['field'] == field for a in aggregations):
                    aggregations.append({
                        'type': agg_type,
                        'field': field
                    })
        
        # Infer from context if no explicit aggregations
        if not aggregations:
            if 'total' in query_lower or 'sum' in query_lower:
                aggregations.append({'type': 'sum', 'field': 'sales'})
            if 'average' in query_lower or 'avg' in query_lower:
                aggregations.append({'type': 'avg', 'field': 'profit'})
            if 'count' in query_lower:
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