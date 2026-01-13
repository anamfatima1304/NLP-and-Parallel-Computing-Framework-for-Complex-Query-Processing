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
        
        # Format results for frontend
        final_result = execution_results['final_result']
        result_data = None
        result_type = 'unknown'
        
        if isinstance(final_result, pd.DataFrame):
            result_data = final_result.to_dict('records')
            result_type = 'dataframe'
        else:
            result_data = str(final_result)
            result_type = 'text'
        
        response = {
            'success': True,
            'query_components': {
                'filters': query_components.get('filters', []),
                'groupings': query_components.get('groupings', []),
                'aggregations': query_components.get('aggregations', [])
            },
            'execution_plan': {
                'total_tasks': execution_plan['total_tasks'],
                'total_layers': execution_plan['total_layers'],
                'max_parallelism': execution_plan['max_parallelism']
            },
            'performance': {
                'execution_time': round(execution_time, 4),
                'processors_used': num_processors,
                'tasks_executed': len(execution_results.get('all_results', {}))
            },
            'results': {
                'type': result_type,
                'data': result_data,
                'row_count': len(final_result) if isinstance(final_result, pd.DataFrame) else None
            }
        }
        
        return jsonify(response)
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    load_dataset()
    app.run(debug=True, port=5000)