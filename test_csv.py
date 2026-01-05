from nlp_processor import NLPQueryProcessor

processor = NLPQueryProcessor()

# Test query
query = " Show me the total sales and average profit by region and category for year 2023 where sales greater than 5000 and discount less than 0.2, and also count the number of orders for each segment"

# Process it
result = processor.decompose_query(query)

# Print results
import json
print(json.dumps(result, indent=2))