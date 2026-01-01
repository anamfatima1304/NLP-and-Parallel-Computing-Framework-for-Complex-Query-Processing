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

import pandas as pd
from typing import Dict, Any, List
import json


class ResultAggregator:
    """
    Aggregates results from parallel execution and formats final output.
    """
    
    def __init__(self):
        self.aggregated_result = None
    
    def merge_results(self, partial_results: Dict[str, Any]) -> Any:
        """
        Merge partial results from different workers.
        
        Args:
            partial_results: Dictionary mapping task_id to result
            
        Returns:
            Merged result
        """
        # Get all results
        results = list(partial_results.values())
        
        # If only one result, return it
        if len(results) == 1:
            return results[0]
        
        # If multiple DataFrames, concatenate them
        dataframes = [r for r in results if isinstance(r, pd.DataFrame)]
        
        if dataframes:
            if len(dataframes) == 1:
                return dataframes[0]
            else:
                # Merge all DataFrames
                merged = pd.concat(dataframes, ignore_index=True)
                return merged
        
        return results[0]
    
    def format_dataframe_result(self, df: pd.DataFrame, max_rows: int = 20) -> Dict[str, Any]:
        """
        Format a DataFrame result for display.
        
        Args:
            df: Result DataFrame
            max_rows: Maximum rows to display
            
        Returns:
            Formatted result dictionary
        """
        # Limit rows for display
        display_df = df.head(max_rows)
        
        result = {
            'type': 'dataframe',
            'shape': {
                'rows': len(df),
                'columns': len(df.columns)
            },
            'columns': list(df.columns),
            'data': display_df.to_dict('records'),
            'summary': {
                'total_rows': len(df),
                'displayed_rows': len(display_df),
                'truncated': len(df) > max_rows
            }
        }
        
        # Add statistics for numeric columns
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
    
    def format_scalar_result(self, value: Any) -> Dict[str, Any]:
        """
        Format a scalar result (single value) for display.
        
        Args:
            value: Scalar value
            
        Returns:
            Formatted result dictionary
        """
        return {
            'type': 'scalar',
            'value': value,
            'formatted_value': f"{value:,.2f}" if isinstance(value, (int, float)) else str(value)
        }
    
    def aggregate_and_format(self, execution_results: Dict[str, Any], 
                            original_query: str) -> Dict[str, Any]:
        """
        Main aggregation function that processes execution results
        and creates the final formatted output.
        
        Args:
            execution_results: Results from ParallelExecutor
            original_query: Original natural language query
            
        Returns:
            Formatted final result
        """
        final_result = execution_results['final_result']
        
        # Format based on result type
        if isinstance(final_result, pd.DataFrame):
            formatted_result = self.format_dataframe_result(final_result)
        elif isinstance(final_result, (int, float)):
            formatted_result = self.format_scalar_result(final_result)
        else:
            formatted_result = {
                'type': 'other',
                'value': str(final_result)
            }
        
        # Build complete response
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
        """
        Create a human-readable summary of the results.
        
        Args:
            response: Formatted response dictionary
            
        Returns:
            Summary string
        """
        summary = f"Query: {response['query']}\n"
        summary += "=" * 70 + "\n\n"
        
        result = response['result']
        
        if result['type'] == 'dataframe':
            summary += f"Result Type: Table\n"
            summary += f"Total Rows: {result['summary']['total_rows']}\n"
            summary += f"Columns: {', '.join(result['columns'])}\n\n"
            
            # Show data
            summary += "Data Preview:\n"
            summary += "-" * 70 + "\n"
            
            df = pd.DataFrame(result['data'])
            summary += df.to_string(index=False)
            
            if result['summary']['truncated']:
                summary += f"\n... (showing {result['summary']['displayed_rows']} of {result['summary']['total_rows']} rows)"
            
            # Show statistics if available
            if 'statistics' in result:
                summary += "\n\nStatistics:\n"
                summary += "-" * 70 + "\n"
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
    
    def to_json(self, response: Dict[str, Any]) -> str:
        """
        Convert response to JSON string.
        
        Args:
            response: Formatted response dictionary
            
        Returns:
            JSON string
        """
        return json.dumps(response, indent=2, default=str)


# Example usage
if __name__ == '__main__':
    # Sample execution results
    sample_data = pd.DataFrame({
        'region': ['North', 'South', 'East', 'West'],
        'sales': [125000, 98000, 156000, 142000]
    })
    
    execution_results = {
        'final_result': sample_data,
        'all_results': {'T1': sample_data},
        'execution_times': {'T1': 0.023, 'T2': 0.015, 'T3': 0.012},
        'total_execution_time': 0.050,
        'num_processes_used': 4
    }
    
    aggregator = ResultAggregator()
    response = aggregator.aggregate_and_format(
        execution_results, 
        "Show me total sales by region for 2023"
    )
    
    print(aggregator.create_summary(response))
    print("\n\nJSON Format:")
    print(aggregator.to_json(response))