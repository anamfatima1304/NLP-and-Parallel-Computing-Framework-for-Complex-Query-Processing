"""
Flask Web Application
=====================
Main web interface for the NLP & Parallel Query Processing System.

Features:
- Web-based query interface
- Real-time query processing
- Visual results display
- Performance metrics
"""

from flask import Flask, render_template, request, jsonify
import pandas as pd
import os
import json
from nlp_processor import NLPQueryProcessor
from task_planner import TaskPlanner
from parallel_executor import ParallelExecutor
from aggregator import ResultAggregator
from dataset_generator import DatasetGenerator

app = Flask(__name__)

# Global variables
DATA = None
NLP_PROCESSOR = None


def initialize_system():
    """Initialize the system with dataset and components."""
    global DATA, NLP_PROCESSOR
    
    # Check if dataset exists, if not create it
    data_file = 'data/sales_data.csv'
    
    if not os.path.exists('data'):
        os.makedirs('data')
    
    if not os.path.exists(data_file):
        print("Generating sample dataset...")
        generator = DatasetGenerator(num_records=10000)
        df = generator.generate_data()
        generator.save_to_csv(df, data_file)
        DATA = df
    else:
        print(f"Loading dataset from {data_file}...")
        DATA = pd.read_csv(data_file)
    
    # Initialize NLP processor
    NLP_PROCESSOR = NLPQueryProcessor()
    
    print(f"System initialized with {len(DATA)} records")


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
        data = request.get_json()
        query = data.get('query', '')
        num_processes = int(data.get('num_processes', 4))
        
        if not query:
            return jsonify({'error': 'Query is required'}), 400
        
        # Step 1: NLP Processing
        decomposed = NLP_PROCESSOR.decompose_query(query)
        
        # Step 2: Task Planning
        planner = TaskPlanner(decomposed['tasks'])
        execution_plan = planner.create_execution_plan()
        dag_visualization = planner.visualize_dag()
        
        # Step 3: Parallel Execution
        executor = ParallelExecutor(DATA, num_processes=num_processes)
        execution_results = executor.execute_plan(execution_plan)
        
        # Step 4: Result Aggregation
        aggregator = ResultAggregator()
        final_response = aggregator.aggregate_and_format(execution_results, query)
        
        # Add additional info for display
        final_response['decomposition'] = decomposed
        final_response['execution_plan'] = execution_plan
        final_response['dag_visualization'] = dag_visualization
        
        return jsonify(final_response)
    
    except Exception as e:
        return jsonify({
            'error': str(e),
            'status': 'error'
        }), 500


@app.route('/api/example_queries', methods=['GET'])
def get_example_queries():
    """Return example queries for user reference."""
    examples = [
        {
            'query': 'Show me total sales by region for 2023',
            'description': 'Filter by year, group by region, sum sales'
        },
        {
            'query': 'Find all products where sales > 50000 and group by category',
            'description': 'Filter by sales value, group by category'
        },
        {
            'query': 'What is the average revenue by region for year 2022',
            'description': 'Filter by year, group by region, average revenue'
        },
        {
            'query': 'Count total transactions by product for 2023',
            'description': 'Filter by year, group by product, count'
        },
        {
            'query': 'Show maximum profit by region where year = 2024',
            'description': 'Filter by year, group by region, max profit'
        },
        {
            'query': 'Total quantity sold by category in 2023',
            'description': 'Filter by year, group by category, sum quantity'
        }
    ]
    return jsonify(examples)


@app.route('/api/dataset_info', methods=['GET'])
def get_dataset_info():
    """Return information about the loaded dataset."""
    info = {
        'total_records': len(DATA),
        'columns': list(DATA.columns),
        'date_range': {
            'min_year': int(DATA['year'].min()),
            'max_year': int(DATA['year'].max())
        },
        'unique_values': {
            'products': int(DATA['product'].nunique()),
            'regions': int(DATA['region'].nunique()),
            'categories': int(DATA['category'].nunique())
        },
        'financial_summary': {
            'total_revenue': float(DATA['revenue'].sum()),
            'total_profit': float(DATA['profit'].sum()),
            'avg_transaction': float(DATA['revenue'].mean())
        }
    }
    return jsonify(info)


if __name__ == '__main__':
    # Initialize system
    initialize_system()
    
    # Run Flask app
    print("\n" + "=" * 70)
    print("NLP & Parallel Computing Framework for Complex Query Processing")
    print("=" * 70)
    print("\nStarting Flask server...")
    print("Open your browser and navigate to: http://localhost:5000")
    print("\nPress CTRL+C to stop the server")
    print("=" * 70 + "\n")
    
    app.run(debug=True, host='0.0.0.0', port=5000)