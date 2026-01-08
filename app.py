"""
PDC Project - Flask Web Application
====================================
Interactive web interface for NLP query processing with parallel execution.
"""

from flask import Flask, render_template, request, jsonify
import pandas as pd
import numpy as np
import json
from nlp_processor import ParallelNLPQueryProcessor
from parallel_executor import ParallelExecutor
import multiprocessing as mp

app = Flask(__name__)

# Global dataset
DATASET = None
processor = ParallelNLPQueryProcessor()

def initialize_dataset():
    """Initialize the sample dataset."""
    global DATASET
    np.random.seed(42)
    dataset_size = 10000
    
    DATASET = pd.DataFrame({
        'product': np.random.choice(['Laptop', 'Phone', 'Tablet', 'Monitor', 'Keyboard', 'Mouse'], dataset_size),
        'region': np.random.choice(['North', 'South', 'East', 'West', 'Central'], dataset_size),
        'category': np.random.choice(['Electronics', 'Furniture', 'Office Supplies'], dataset_size),
        'segment': np.random.choice(['Consumer', 'Corporate', 'Home Office'], dataset_size),
        'ship_mode': np.random.choice(['Standard', 'Express', 'Same Day', 'Second Class'], dataset_size),
        'state': np.random.choice(['CA', 'TX', 'NY', 'FL', 'IL', 'PA', 'OH', 'WA'], dataset_size),
        'year': np.random.choice([2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023], dataset_size),
        'month': np.random.choice(range(1, 13), dataset_size),
        'sales': np.random.uniform(100, 50000, dataset_size),
        'profit': np.random.uniform(-500, 5000, dataset_size),
        'quantity': np.random.randint(1, 100, dataset_size),
        'discount': np.random.uniform(0, 0.4, dataset_size)
    })

@app.route('/')
def index():
    """Render the main page."""
    max_cpus = mp.cpu_count()
    return render_template('index.html', max_cpus=max_cpus)

@app.route('/api/database-info', methods=['GET'])
def get_database_info():
    """Get database statistics and information."""
    if DATASET is None:
        return jsonify({'error': 'Dataset not initialized'}), 500
    
    # Basic stats
    stats = {
        'total_records': len(DATASET),
        'total_columns': len(DATASET.columns),
        'memory_usage_mb': round(DATASET.memory_usage(deep=True).sum() / 1024**2, 2),
        'columns': []
    }
    
    # Column information
    for col in DATASET.columns:
        col_info = {
            'name': col,
            'dtype': str(DATASET[col].dtype),
            'non_null': int(DATASET[col].count()),
            'unique': int(DATASET[col].nunique())
        }
        
        if DATASET[col].dtype in ['int64', 'float64']:
            col_info['stats'] = {
                'min': round(float(DATASET[col].min()), 2),
                'max': round(float(DATASET[col].max()), 2),
                'mean': round(float(DATASET[col].mean()), 2),
                'median': round(float(DATASET[col].median()), 2),
                'std': round(float(DATASET[col].std()), 2)
            }
        else:
            # Top 5 categories
            top_values = DATASET[col].value_counts().head(5)
            col_info['top_values'] = [
                {'value': str(val), 'count': int(count), 'percentage': round((count/len(DATASET))*100, 1)}
                for val, count in top_values.items()
            ]
        
        stats['columns'].append(col_info)
    
    # Sample data
    stats['sample_data'] = DATASET.head(10).to_dict('records')
    
    return jsonify(stats)

@app.route('/api/process-query', methods=['POST'])
def process_query():
    """Process NLP query and return decomposition."""
    data = request.json
    query = data.get('query', '')
    
    if not query:
        return jsonify({'error': 'No query provided'}), 400
    
    try:
        # Decompose query
        result = processor.decompose_query_parallel(query)
        
        # Format tasks by level
        levels = {}
        for task in result['tasks']:
            level = task.get('level', 0)
            if level not in levels:
                levels[level] = []
            levels[level].append(task)
        
        formatted_result = {
            'original_query': result['original_query'],
            'total_tasks': result['total_tasks'],
            'parallelization_info': result['parallelization_info'],
            'levels': {str(k): v for k, v in levels.items()},
            'tasks': result['tasks']
        }
        
        return jsonify(formatted_result)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/execute-query', methods=['POST'])
def execute_query():
    """Execute query with specified number of processors."""
    data = request.json
    tasks = data.get('tasks', [])
    num_processors = data.get('num_processors', 1)
    
    if not tasks:
        return jsonify({'error': 'No tasks provided'}), 400
    
    try:
        # Execute with parallel executor
        executor = ParallelExecutor(DATASET, num_processes=num_processors)
        exec_results = executor.execute_plan(tasks)
        
        # Format results
        final_result = exec_results['final_result']
        
        result_data = None
        result_shape = None
        
        if isinstance(final_result, pd.DataFrame):
            result_data = final_result.to_dict('records')
            result_shape = {'rows': final_result.shape[0], 'columns': final_result.shape[1]}
        elif isinstance(final_result, dict):
            result_data = final_result
        else:
            result_data = str(final_result)
        
        # Calculate metrics
        sequential_time = sum(exec_results['execution_times'].values())
        parallel_time = exec_results['total_execution_time']
        speedup = sequential_time / parallel_time if parallel_time > 0 else 0
        efficiency = (speedup / num_processors) * 100
        time_saved = sequential_time - parallel_time
        
        # Process utilization
        process_usage = {}
        for task_id, proc_id in exec_results['task_process_map'].items():
            proc_key = str(proc_id)
            if proc_key not in process_usage:
                process_usage[proc_key] = []
            process_usage[proc_key].append(task_id)
        
        formatted_result = {
            'num_processors': num_processors,
            'total_tasks': exec_results['total_tasks'],
            'total_execution_time': round(exec_results['total_execution_time'], 3),
            'level_times': {k: round(v, 3) for k, v in exec_results['level_times'].items()},
            'task_times': {k: round(v, 3) for k, v in exec_results['execution_times'].items()},
            'task_process_map': exec_results['task_process_map'],
            'metrics': {
                'sequential_time': round(sequential_time, 3),
                'parallel_time': round(parallel_time, 3),
                'speedup': round(speedup, 2),
                'efficiency': round(efficiency, 1),
                'time_saved': round(time_saved, 3),
                'time_saved_percentage': round((time_saved/sequential_time)*100, 1) if sequential_time > 0 else 0
            },
            'process_usage': process_usage,
            'result_data': result_data,
            'result_shape': result_shape
        }
        
        return jsonify(formatted_result)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/compare-processors', methods=['POST'])
def compare_processors():
    """Compare execution with multiple processor configurations."""
    data = request.json
    tasks = data.get('tasks', [])
    processor_counts = data.get('processor_counts', [1, 2, 4])
    
    if not tasks:
        return jsonify({'error': 'No tasks provided'}), 400
    
    try:
        comparison_results = []
        
        for proc_count in processor_counts:
            executor = ParallelExecutor(DATASET, num_processes=proc_count)
            exec_results = executor.execute_plan(tasks)
            
            sequential_time = sum(exec_results['execution_times'].values())
            parallel_time = exec_results['total_execution_time']
            speedup = sequential_time / parallel_time if parallel_time > 0 else 0
            efficiency = (speedup / proc_count) * 100
            
            comparison_results.append({
                'processors': proc_count,
                'execution_time': round(parallel_time, 3),
                'speedup': round(speedup, 2),
                'efficiency': round(efficiency, 1),
                'sequential_time': round(sequential_time, 3)
            })
        
        # Calculate relative speedup (compared to single processor)
        baseline_time = comparison_results[0]['execution_time']
        for result in comparison_results:
            result['relative_speedup'] = round(baseline_time / result['execution_time'], 2)
            result['time_saved'] = round(baseline_time - result['execution_time'], 3)
        
        return jsonify({'comparisons': comparison_results})
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("Initializing PDC Flask Application...")
    print("Generating sample dataset...")
    initialize_dataset()
    print(f"Dataset ready: {len(DATASET)} rows")
    print(f"Available CPUs: {mp.cpu_count()}")
    print("\nStarting Flask server...")
    print("Open your browser and navigate to: http://127.0.0.1:5000")
    print("\nPress Ctrl+C to stop the server")
    app.run(debug=True, host='0.0.0.0', port=5000)