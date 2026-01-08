import re
import json
import pandas as pd
import multiprocessing as mp
from typing import List, Dict, Any, Tuple
from collections import defaultdict, deque
import time
import numpy as np
from nlp_processor import NLPQueryProcessor 
from task_planner import TaskPlanner
from parallel_executor import ParallelExecutor
from aggregator import ResultAggregator


if __name__=="__main__":
    query = """Show me total sales and average profit by region and category 
    for year 2017 where sales greater than 5000 and discount less than 0.2"""
    data = pd.read_csv(r'data\superstore.csv', encoding='latin1')

    
    print("🔍 Processing Query:")
    print(f"   \"{query.strip()}\"\n")
    print("="*80 + "\n")
    
    # ============================================================================
    # STEP 1: NLP PROCESSING (Parse Query Components)
    # ============================================================================
    print("STEP 1️ NLP PROCESSING")
    print("-"*80)
    nlp_processor = NLPQueryProcessor()
    query_components = nlp_processor.parse_query(query)
    
    print(f"✓ Query parsed successfully!")
    print(f"   • Filters: {len(query_components['filters'])}")
    for f in query_components['filters']:
        print(f"      - {f['field']} {f['operator']} {f['value']}")
    print(f"   • Groupings: {len(query_components['groupings'])}")
    for g in query_components['groupings']:
        print(f"      - by {g}")
    print(f"   • Aggregations: {len(query_components['aggregations'])}")
    for a in query_components['aggregations']:
        print(f"      - {a['type']}({a['field']})")
    print()
    
    # ============================================================================
    # STEP 2: TASK PLANNING (Build DAG & Execution Plan)
    # ============================================================================
    print("STEP 2 TASK PLANNING")
    print("-"*80)
    task_planner = TaskPlanner()
    execution_plan = task_planner.create_execution_plan(query_components)
    
    print(f"✓ Execution plan created!")
    print(f"   • Total Tasks: {execution_plan['total_tasks']}")
    print(f"   • Execution Layers: {execution_plan['total_layers']}")
    print(f"   • Max Parallel Tasks: {execution_plan['max_parallelism']}")
    print()
    
    # Show visual plan
    print(task_planner.visualize_plan(execution_plan))
    
    # ============================================================================
    # STEP 3: PARALLEL EXECUTION (Execute with different processor counts)
    # ============================================================================
    print("\nSTEP 3  PARALLEL EXECUTION")
    print("-"*80)
    
    # Test with different processor counts
    for num_proc in [1, 2, 4]:
        print(f"\n{'='*80}")
        print(f"🚀 EXECUTING WITH {num_proc} PROCESSOR(S)")
        print(f"{'='*80}")
        
        executor = ParallelExecutor(data, num_processes=num_proc)
        execution_results = executor.execute_plan(execution_plan)
        
        # Show performance report
        executor.print_performance_report(execution_results)
        
        # Show result preview (only for first run)
        if num_proc == 1:
            print(f"📋 RESULT PREVIEW:")
            print("-"*80)
            final_result = execution_results['final_result']
            if isinstance(final_result, pd.DataFrame):
                print(final_result.to_string(index=False))
            else:
                print(final_result)
            print()
    
    # ============================================================================
    # STEP 4: RESULT AGGREGATION (Format Final Output)
    # ============================================================================
    print("\n" + "="*80)
    print("STEP 4 RESULT AGGREGATION")
    print("="*80 + "\n")
    
    aggregator = ResultAggregator()
    final_response = aggregator.aggregate_and_format(
        execution_results, 
        query_components['original_query']
    )
    
    print(aggregator.create_summary(final_response))
    
    # ============================================================================
    # FINAL JSON OUTPUT
    # ============================================================================
    print("\n" + "="*80)
    print("📦 JSON OUTPUT (for API/export)")
    print("="*80)
    print(json.dumps(final_response, indent=2, default=str))
    
    print("\n" + "="*80)
    print("✅ PROCESSING COMPLETE")
    print("="*80)