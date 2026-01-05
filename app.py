"""
Flask Web Application - WORKING VERSION
========================================
Uses your existing files:
- nlp_processor.py with ParallelNLPQueryProcessor
- parallel_executor.py with ParallelExecutor

NO TaskPlanner, NO aggregator needed.
"""

from flask import Flask, render_template, request, jsonify
import pandas as pd
import os
import json
import traceback
from nlp_processor import ParallelNLPQueryProcessor
from parallel_executor import ParallelExecutor

app = Flask(__name__)

# Global variables
DATA = None
NLP_PROCESSOR = None


def initialize_system():
    """Initialize the system with dataset and components."""
    global DATA, NLP_PROCESSOR
    
    # Path to your Kaggle dataset
    data_file = 'data/superstore.csv'
    
    if not os.path.exists('data'):
        os.makedirs('data')
    
    if not os.path.exists(data_file):
        print("=" * 70)
        print("WARNING: Kaggle dataset not found!")
        print(f"Please place your CSV file at: {data_file}")
        print("=" * 70)
        print("\nCreating sample dataset as fallback...")
        
        # Create sample data
        import numpy as np
        DATA = pd.DataFrame({
            'product': ['Phones', 'Chairs', 'Binders', 'Tables', 'Storage'] * 2000,
            'region': ['North', 'South', 'East', 'West'] * 2500,
            'category': ['Technology', 'Furniture', 'Office Supplies'] * 3333 + ['Technology'],
            'segment': ['Consumer', 'Corporate', 'Home Office'] * 3333 + ['Consumer'],
            'year': [2015, 2016, 2017, 2018] * 2500,
            'month': list(range(1, 13)) * 833 + [1] * 4,
            'sales': np.random.uniform(100, 10000, 10000),
            'profit': np.random.uniform(-500, 3000, 10000),
            'quantity': np.random.randint(1, 50, 10000),
            'discount': np.random.uniform(0, 0.5, 10000),
            'customer': [f'Customer_{i%1000}' for i in range(10000)],
        })
        DATA['revenue'] = DATA['sales']
        if not os.path.exists('data'):
            os.makedirs('data')
        DATA.to_csv('data/sample_data.csv', index=False)
        print(f"✓ Sample dataset created with {len(DATA)} records")
    else:
        print(f"Loading dataset from {data_file}...")
        DATA = pd.read_csv(data_file, encoding='latin1')
        
        # Column name standardization for Kaggle Superstore
        if 'Order Date' in DATA.columns:
            print("Detected Kaggle Superstore format - standardizing columns...")
            
            column_mapping = {
                'Order Date': 'order_date',
                'Ship Date': 'ship_date',
                'Ship Mode': 'ship_mode',
                'Customer ID': 'customer_id',
                'Customer Name': 'customer',
                'Segment': 'segment',
                'Country': 'country',
                'City': 'city',
                'State': 'state',
                'Postal Code': 'postal_code',
                'Region': 'region',
                'Product ID': 'product_id',
                'Category': 'category',
                'Sub-Category': 'product',
                'Product Name': 'product_name',
                'Sales': 'sales',
                'Quantity': 'quantity',
                'Discount': 'discount',
                'Profit': 'profit'
            }
            
            DATA = DATA.rename(columns=column_mapping)
            DATA['order_date'] = pd.to_datetime(DATA['order_date'])
            DATA['year'] = DATA['order_date'].dt.year
            DATA['month'] = DATA['order_date'].dt.month
            DATA['revenue'] = DATA['sales']
            DATA = DATA.dropna(subset=['sales', 'region', 'year'])
            
            if 'Row ID' in DATA.columns:
                DATA = DATA.rename(columns={'Row ID': 'transaction_id'})
            
            print("✓ Data standardization completed")
    
    # Initialize NLP processor
    NLP_PROCESSOR = ParallelNLPQueryProcessor()
    
    # Print dataset summary
    print(f"\n{'='*70}")
    print("DATASET LOADED SUCCESSFULLY")
    print(f"{'='*70}")
    print(f"Total Records: {len(DATA):,}")
    print(f"Date Range: {DATA['year'].min()} - {DATA['year'].max()}")
    print(f"Regions: {DATA['region'].nunique()}")
    print(f"Categories: {DATA['category'].nunique()}")
    print(f"Total Sales: ${DATA['sales'].sum():,.2f}")
    print(f"{'='*70}\n")


def convert_to_json_serializable(obj):
    """Convert DataFrames and other non-serializable objects to JSON format."""
    if isinstance(obj, pd.DataFrame):
        return {
            'type': 'dataframe',
            'data': obj.to_dict('records'),
            'columns': list(obj.columns),
            'row_count': len(obj)
        }
    elif isinstance(obj, pd.core.groupby.DataFrameGroupBy):
        df = obj.first().reset_index()
        return {
            'type': 'grouped_dataframe',
            'data': df.to_dict('records'),
            'columns': list(df.columns),
            'row_count': len(df)
        }
    elif isinstance(obj, dict):
        return {k: convert_to_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_json_serializable(item) for item in obj]
    else:
        return obj


@app.route('/')
def index():
    """Render the main page."""
    return render_template('index.html')


@app.route('/api/process_query', methods=['POST'])
def process_query():
    """
    Process a natural language query through the complete pipeline.
    """
    try:
        # Get request data
        req_data = request.get_json()
        query = req_data.get('query', '')
        num_processes = int(req_data.get('num_processes', 4))
        
        if not query:
            return jsonify({'error': 'Query is required'}), 400
        
        print(f"\n{'='*70}")
        print(f"Processing Query: {query}")
        print(f"Processors: {num_processes}")
        print(f"{'='*70}")
        
        # Step 1: NLP Processing
        decomposed = NLP_PROCESSOR.decompose_query_parallel(query)
        print(f"\n✓ NLP Decomposition: {decomposed['total_tasks']} tasks created")
        
        # Step 2: Parallel Execution
        executor = ParallelExecutor(DATA, num_processes=num_processes)
        execution_results = executor.execute_plan(decomposed['tasks'])
        
        print(f"\n✓ Execution completed in {execution_results['total_execution_time']:.3f}s")
        
        # Step 3: Format response
        response = {
            'status': 'success',
            'query': query,
            
            # NLP Info
            'nlp_decomposition': {
                'total_tasks': decomposed['total_tasks'],
                'tasks': decomposed['tasks'],
                'parallelization_info': decomposed.get('parallelization_info', {})
            },
            
            # Execution Info
            'execution': {
                'num_processes': execution_results['num_processes_used'],
                'total_time': round(execution_results['total_execution_time'], 3),
                'sequential_time': round(sum(execution_results['execution_times'].values()), 3),
                'speedup': round(sum(execution_results['execution_times'].values()) / execution_results['total_execution_time'], 2),
                'level_times': execution_results.get('level_times', {}),
                'task_times': execution_results['execution_times']
            },
            
            # Results
            'results': convert_to_json_serializable(execution_results.get('final_results', {}))
        }
        
        return jsonify(response)
    
    except Exception as e:
        error_trace = traceback.format_exc()
        print(f"\n❌ ERROR: {str(e)}")
        print(error_trace)
        
        return jsonify({
            'status': 'error',
            'error': str(e),
            'traceback': error_trace
        }), 500


@app.route('/api/compare_processors', methods=['POST'])
def compare_processors():
    """Compare performance with different processor counts."""
    try:
        req_data = request.get_json()
        query = req_data.get('query', '')
        
        if not query:
            return jsonify({'error': 'Query is required'}), 400
        
        print(f"\n{'='*70}")
        print(f"PROCESSOR COMPARISON: {query}")
        print(f"{'='*70}")
        
        # Get tasks from NLP
        decomposed = NLP_PROCESSOR.decompose_query_parallel(query)
        tasks = decomposed['tasks']
        
        # Test different processor counts
        import multiprocessing as mp
        processor_counts = [1, 2, 4, mp.cpu_count()]
        comparison = {}
        
        for num_proc in processor_counts:
            print(f"\nTesting with {num_proc} processor(s)...")
            
            executor = ParallelExecutor(DATA, num_processes=num_proc)
            results = executor.execute_plan(tasks)
            
            sequential = sum(results['execution_times'].values())
            parallel = results['total_execution_time']
            
            comparison[str(num_proc)] = {
                'processors': num_proc,
                'total_time': round(parallel, 3),
                'sequential_time': round(sequential, 3),
                'speedup': round(sequential / parallel, 2),
                'efficiency': round((sequential / parallel / num_proc) * 100, 1)
            }
        
        # Add baseline comparison
        baseline = comparison['1']['total_time']
        for key in comparison:
            comparison[key]['speedup_vs_1proc'] = round(baseline / comparison[key]['total_time'], 2)
        
        print(f"\n✓ Comparison completed")
        
        return jsonify({
            'status': 'success',
            'query': query,
            'task_count': len(tasks),
            'comparison': comparison
        })
    
    except Exception as e:
        error_trace = traceback.format_exc()
        print(f"\n❌ ERROR: {str(e)}")
        print(error_trace)
        
        return jsonify({
            'status': 'error',
            'error': str(e),
            'traceback': error_trace
        }), 500


@app.route('/api/example_queries', methods=['GET'])
def get_example_queries():
    """Return example queries."""
    examples = [
        {
            'query': 'Show me total sales and average profit by region and category for year 2017 where sales greater than 5000 and discount less than 0.2',
            'description': 'Complex query with multiple filters, groups, and aggregations (8 tasks)',
            'complexity': 'High'
        },
        {
            'query': 'Show me total sales by region for 2017',
            'description': 'Simple filter, group, and aggregate (3 tasks)',
            'complexity': 'Low'
        },
        {
            'query': 'Find average profit by category where sales greater than 1000 for year 2016',
            'description': 'Multiple filters with grouping (4 tasks)',
            'complexity': 'Medium'
        },
        {
            'query': 'What is the sum of sales and count of quantity by region for years 2015 and 2016',
            'description': 'Multi-year with multiple aggregations (6 tasks)',
            'complexity': 'High'
        },
        {
            'query': 'Show maximum profit by segment where year = 2017',
            'description': 'Simple filter and aggregation (3 tasks)',
            'complexity': 'Low'
        }
    ]
    return jsonify(examples)


@app.route('/api/dataset_info', methods=['GET'])
def get_dataset_info():
    """Return dataset information."""
    try:
        info = {
            'total_records': int(len(DATA)),
            'columns': list(DATA.columns),
            'date_range': {
                'min_year': int(DATA['year'].min()),
                'max_year': int(DATA['year'].max())
            },
            'unique_values': {
                'regions': int(DATA['region'].nunique()),
                'categories': int(DATA['category'].nunique()),
                'products': int(DATA['product'].nunique())
            },
            'financial_summary': {
                'total_sales': float(DATA['sales'].sum()),
                'avg_sales': float(DATA['sales'].mean()),
                'total_quantity': int(DATA['quantity'].sum())
            }
        }
        return jsonify(info)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/system_info', methods=['GET'])
def get_system_info():
    """Return system information."""
    import multiprocessing as mp
    return jsonify({
        'cpu_count': mp.cpu_count(),
        'dataset_size': len(DATA),
        'processor_type': 'ParallelNLPQueryProcessor',
        'executor_type': 'ParallelExecutor',
        'features': [
            'Level-based parallel execution',
            'Independent task processing',
            'Real-time speedup calculation',
            'Multi-processor comparison'
        ]
    })


if __name__ == '__main__':
    # Initialize system
    initialize_system()
    
    # Run Flask app
    print("\n" + "=" * 70)
    print("NLP & Parallel Computing Framework")
    print("=" * 70)
    print("\nStarting Flask server...")
    print("Navigate to: http://localhost:5000")
    print("\nAPI Endpoints:")
    print("  POST /api/process_query - Process a query")
    print("  POST /api/compare_processors - Compare processor counts")
    print("  GET  /api/example_queries - Get example queries")
    print("  GET  /api/dataset_info - Get dataset info")
    print("  GET  /api/system_info - Get system info")
    print("\nPress CTRL+C to stop the server")
    print("=" * 70 + "\n")
    
    app.run(debug=True, host='0.0.0.0', port=5000)