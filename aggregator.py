"""
Aggregator
==========
This module handles result aggregation and final response formatting.

Features:
- Collect results from all workers
- Merge partial results
- Apply final aggregations
- Format output for user display
"""

import re
import json
import pandas as pd
import multiprocessing as mp
from typing import List, Dict, Any, Tuple
from collections import defaultdict, deque
import time
import numpy as np


class ResultAggregator:
    """Aggregates and formats execution results"""
    
    def __init__(self):
        self.aggregated_result = None
    
    def format_dataframe_result(self, df: pd.DataFrame, max_rows: int = 20) -> Dict[str, Any]:
        """Format DataFrame result for display"""
        display_df = df.head(max_rows)
        
        result = {
            'type': 'dataframe',
            'shape': {'rows': len(df), 'columns': len(df.columns)},
            'columns': list(df.columns),
            'data': display_df.to_dict('records'),
            'summary': {
                'total_rows': len(df),
                'displayed_rows': len(display_df),
                'truncated': len(df) > max_rows
            }
        }
        
        numeric_cols = df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            result['statistics'] = {}
            for col in numeric_cols:
                result['statistics'][col] = {
                    'sum': float(df[col].sum()),
                    'mean': float(df[col].mean()),
                    'min': float(df[col].min()),
                    'max': float(df[col].max()),
                    'count': int(df[col].count())
                }
        
        return result
    
    def aggregate_and_format(self, execution_results: Dict[str, Any], 
                            original_query: str) -> Dict[str, Any]:
        """Main aggregation function"""
        final_result = execution_results['final_result']
        
        if isinstance(final_result, pd.DataFrame):
            formatted_result = self.format_dataframe_result(final_result)
        elif isinstance(final_result, (int, float)):
            formatted_result = {
                'type': 'scalar',
                'value': final_result,
                'formatted_value': f"{final_result:,.2f}"
            }
        else:
            formatted_result = {'type': 'other', 'value': str(final_result)}
        
        response = {
            'query': original_query,
            'result': formatted_result,
            'execution_metadata': {
                'total_execution_time': round(execution_results['total_execution_time'], 4),
                'num_processes_used': execution_results['num_processes_used'],
                'task_execution_times': {
                    task_id: round(time, 4) 
                    for task_id, time in execution_results['execution_times'].items()
                }
            },
            'status': 'success'
        }
        
        self.aggregated_result = response
        return response
    
    def create_summary(self, response: Dict[str, Any]) -> str:
        """Create human-readable summary"""
        summary = f"Query: {response['query']}\n"
        summary += "=" * 70 + "\n\n"
        
        result = response['result']
        
        if result['type'] == 'dataframe':
            summary += f"Result Type: Table\n"
            summary += f"Total Rows: {result['summary']['total_rows']}\n"
            summary += f"Columns: {', '.join(result['columns'])}\n\n"
            
            df = pd.DataFrame(result['data'])
            summary += "Data Preview:\n" + "-" * 70 + "\n"
            summary += df.to_string(index=False)
            
            if result['summary']['truncated']:
                summary += f"\n... (showing {result['summary']['displayed_rows']} of {result['summary']['total_rows']} rows)"
            
            if 'statistics' in result:
                summary += "\n\nStatistics:\n" + "-" * 70 + "\n"
                for col, stats in result['statistics'].items():
                    summary += f"\n{col}:\n"
                    summary += f"  Sum: {stats['sum']:,.2f}\n"
                    summary += f"  Average: {stats['mean']:,.2f}\n"
                    summary += f"  Min: {stats['min']:,.2f}\n"
                    summary += f"  Max: {stats['max']:,.2f}\n"
        
        elif result['type'] == 'scalar':
            summary += f"Result: {result['formatted_value']}\n"
        
        summary += "\n" + "=" * 70 + "\n"
        summary += f"Execution Time: {response['execution_metadata']['total_execution_time']}s\n"
        summary += f"Processes Used: {response['execution_metadata']['num_processes_used']}\n"
        
        return summary