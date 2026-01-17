from flask import Flask, render_template, request, jsonify
import pandas as pd
import json
from nlp_processor import NLPQueryProcessor
from task_planner import TaskPlanner
from parallel_executor import ParallelExecutor
from aggregator import ResultAggregator
import time

app = Flask(__name__)

# Global data storage
data = None
dataset_info = {}

def load_dataset():
    """Load the dataset and extract metadata"""
    global data, dataset_info
    try:
        data = pd.read_csv(r'data\superstore.csv', encoding='latin1')
        dataset_info = {
            'rows': len(data),
            'columns': len(data.columns),
            'column_names': list(data.columns),
            'memory_usage': f"{data.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB",
            'sample_data': data.head(5).to_dict('records')
        }
        return True
    except Exception as e:
        print(f"Error loading dataset: {e}")
        return False

@app.route('/')
def index():
    """Render the main dashboard"""
    if data is None:
        load_dataset()
    return render_template('index.html', dataset_info=dataset_info)

@app.route('/api/dataset-info')
def get_dataset_info():
    """API endpoint for dataset information"""
    if data is None:
        load_dataset()
    return jsonify(dataset_info)

@app.route('/api/execute-query', methods=['POST'])
def execute_query():
    """Execute the query with specified parameters"""
    try:
        query_data = request.get_json()
        query = query_data.get('query', '')
        num_processors = int(query_data.get('processors', 1))
        
        if not query:
            return jsonify({'error': 'Query is required'}), 400
        
        # Step 1: NLP Processing
        nlp_processor = NLPQueryProcessor()
        query_components = nlp_processor.parse_query(query)
        
        # Step 2: Task Planning
        task_planner = TaskPlanner()
        execution_plan = task_planner.create_execution_plan(query_components)
        
        # Step 3: Parallel Execution
        start_time = time.time()
        executor = ParallelExecutor(data, num_processes=num_processors)
        execution_results = executor.execute_plan(execution_plan)
        execution_time = time.time() - start_time
        
        # Step 4: Result Aggregation
        aggregator = ResultAggregator()
        final_response = aggregator.aggregate_and_format(
            execution_results,
            query_components['original_query']
        )
        
        # IMPROVED MERGING LOGIC - Handles ANY number of tasks
        all_results = execution_results.get('all_results', {})
        final_result = execution_results.get('final_result')
        merged_df = None
        
        # Filter out error results and get only DataFrames
        valid_results = {}
        for task_id, result in all_results.items():
            if isinstance(result, pd.DataFrame):
                valid_results[task_id] = result
            elif isinstance(result, str) and 'Error' not in result:
                # Try to handle non-error string results
                pass
        
        # If we have multiple valid DataFrame results, merge them
        if len(valid_results) > 1:
            dfs_list = []
            
            # Clean each DataFrame
            for task_id, df in valid_results.items():
                df_clean = df.reset_index(drop=True)
                if 'index' in df_clean.columns:
                    df_clean = df_clean.drop(columns=['index'])
                dfs_list.append(df_clean)
            
            if len(dfs_list) > 0:
                # Get the first DataFrame as base
                merged_df = dfs_list[0]
                
                # Identify potential grouping columns from first DataFrame
                # These are typically non-numeric columns or known dimension columns
                known_dimensions = ['Region', 'Category', 'State', 'Segment', 'Ship Mode', 
                                   'Sub-Category', 'City', 'Product Name', 'Customer Name',
                                   'Product ID', 'Customer ID', 'Order ID', 'Year', 'Month']
                
                grouping_cols = []
                for col in merged_df.columns:
                    if col in known_dimensions or merged_df[col].dtype == 'object':
                        grouping_cols.append(col)
                
                # Remove metric columns from grouping (Sales, Profit, Discount, Quantity, etc.)
                metric_keywords = ['Sales', 'Profit', 'Discount', 'Quantity', 'Count', 
                                  'Average', 'Sum', 'Min', 'Max', 'Total']
                grouping_cols = [col for col in grouping_cols 
                               if not any(keyword.lower() in col.lower() for keyword in metric_keywords)]
                
                # Merge all DataFrames
                if grouping_cols:
                    for df in dfs_list[1:]:
                        # Find common grouping columns
                        common_cols = [col for col in grouping_cols if col in df.columns]
                        
                        if common_cols:
                            try:
                                merged_df = pd.merge(merged_df, df, on=common_cols, how='outer')
                            except Exception as e:
                                print(f"Merge error: {e}, concatenating instead")
                                # If merge fails, try concatenating
                                merged_df = pd.concat([merged_df, df], axis=1)
                        else:
                            # No common columns, concatenate side by side
                            merged_df = pd.concat([merged_df, df], axis=1)
                else:
                    # No grouping columns found, concatenate all
                    merged_df = pd.concat(dfs_list, axis=1)
                
                # Remove duplicate columns that might result from merging
                merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]
        
        elif len(valid_results) == 1:
            # Only one result, use it directly
            merged_df = list(valid_results.values())[0]
            merged_df = merged_df.reset_index(drop=True)
            if 'index' in merged_df.columns:
                merged_df = merged_df.drop(columns=['index'])
        
        # Use merged result if available, otherwise use final_result
        if merged_df is not None:
            final_result = merged_df
        elif isinstance(final_result, pd.DataFrame):
            final_result = final_result.reset_index(drop=True)
            if 'index' in final_result.columns:
                final_result = final_result.drop(columns=['index'])
        
        # Format results for frontend
        result_data = None
        result_type = 'unknown'
        result_count = 0
        
        if isinstance(final_result, pd.DataFrame):
            result_data = final_result.to_dict('records')
            result_type = 'dataframe'
            result_count = len(final_result)
        elif final_result is not None:
            result_data = str(final_result)
            result_type = 'text'
            result_count = 0
        else:
            result_data = "No results returned"
            result_type = 'text'
            result_count = 0
        
        response = {
            'success': True,
            'query_components': {
                'filters': query_components.get('filters', []),
                'groupings': query_components.get('groupings', []),
                'aggregations': query_components.get('aggregations', [])
            },
            'execution_plan': {
                'total_tasks': execution_plan.get('total_tasks', 0),
                'total_layers': execution_plan.get('total_layers', 0),
                'max_parallelism': execution_plan.get('max_parallelism', 0)
            },
            'performance': {
                'execution_time': round(execution_time, 4),
                'processors_used': num_processors,
                'tasks_executed': len(all_results)
            },
            'results': {
                'type': result_type,
                'data': result_data,
                'row_count': result_count
            }
        }
        
        return jsonify(response)
        
    except Exception as e:
        import traceback
        print(f"Error in execute_query: {e}")
        print(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    load_dataset()
    app.run(debug=True, port=5000)