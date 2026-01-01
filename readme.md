# NLP & Parallel Computing Framework for Complex Query Processing

## 📌 Project Overview

A comprehensive Python-based system that accepts complex natural language queries, automatically decomposes them into executable sub-tasks using NLP techniques, and executes these tasks in parallel using configurable multi-processing capabilities.

### Key Features
- ✅ Natural Language Query Understanding
- ✅ Automatic Query Decomposition
- ✅ DAG-based Task Planning
- ✅ Parallel Task Execution
- ✅ Result Aggregation
- ✅ Web-based User Interface
- ✅ Performance Metrics Tracking

---

## 🏗️ System Architecture

```
User Input (Natural Language)
         ↓
┌─────────────────────────┐
│  NLP Query Processor    │  → Tokenization, Intent Detection, Entity Extraction
└─────────────────────────┘
         ↓
┌─────────────────────────┐
│    Task Planner (DAG)   │  → Build Dependency Graph, Identify Parallel Tasks
└─────────────────────────┘
         ↓
┌─────────────────────────┐
│  Parallel Executor      │  → Multiprocessing Pool, Distributed Execution
└─────────────────────────┘
         ↓
┌─────────────────────────┐
│   Result Aggregator     │  → Merge Results, Format Output
└─────────────────────────┘
         ↓
    Final Response
```

---

## 📂 Project Structure

```
nlp_parallel_query_system/
│
├── nlp_processor.py          # NLP Query Understanding & Decomposition
├── task_planner.py           # DAG Builder & Execution Planner
├── parallel_executor.py      # Parallel Task Execution Engine
├── aggregator.py             # Result Aggregation Logic
├── dataset_generator.py      # Sample Dataset Creator
├── app.py                    # Flask Web Application
│
├── templates/
│   └── index.html           # HTML Template
│
├── static/
│   └── style.css            # CSS Styling
│
├── data/
│   └── sales_data.csv       # Generated Dataset
│
├── requirements.txt          # Python Dependencies
└── README.md                 # This file
```

---

## 🚀 Installation & Setup

### Prerequisites
- Python 3.7 or higher
- pip package manager

### Step 1: Clone or Download the Project

```bash
# Create project directory
mkdir nlp_parallel_query_system
cd nlp_parallel_query_system
```

### Step 2: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 3: Create Required Directories

```bash
mkdir -p data templates static
```

### Step 4: Place All Files

Ensure all Python files are in the root directory:
- `nlp_processor.py`
- `task_planner.py`
- `parallel_executor.py`
- `aggregator.py`
- `dataset_generator.py`
- `app.py`

Place templates and static files in their respective folders:
- `templates/index.html`
- `static/style.css`

---

## 🎯 Running the Application

### Method 1: Run Flask Web Interface (Recommended)

```bash
python app.py
```

Then open your browser and navigate to:
```
http://localhost:5000
```

### Method 2: Run Individual Modules (Testing)

#### Test NLP Processor
```bash
python nlp_processor.py
```

#### Test Task Planner
```bash
python task_planner.py
```

#### Test Parallel Executor
```bash
python parallel_executor.py
```

#### Generate Dataset
```bash
python dataset_generator.py
```

---

## 📝 Example Queries

### Query 1: Simple Aggregation
```
Show me total sales by region for 2023
```

**Decomposition:**
- Task 1: Filter data where year = 2023
- Task 2: Group by region
- Task 3: Sum sales

**Parallel Execution:** 
- Tasks 1-3 execute sequentially (dependency chain)

---

### Query 2: Complex Filter + Group
```
Find all products where sales > 50000 and group by category
```

**Decomposition:**
- Task 1: Filter data where sales > 50000
- Task 2: Group by category
- Task 3: Aggregate results

**Parallel Execution:**
- Task 1 executes first
- Tasks 2-3 depend on Task 1

---

### Query 3: Multi-Year Comparison
```
Compare average revenue between 2022 and 2023
```

**Decomposition:**
- Task 1: Filter for year 2022, calculate avg revenue
- Task 2: Filter for year 2023, calculate avg revenue
- Task 3: Compare results

**Parallel Execution:**
- Tasks 1 and 2 can run in parallel!
- Task 3 waits for both

---

## 🔬 Technical Implementation Details

### 1. NLP Query Processing

**Techniques Used:**
- **Tokenization:** Split query into words
- **Stop Word Removal:** Remove common words (the, a, an, etc.)
- **Intent Detection:** Identify operations (filter, group, aggregate)
- **Entity Extraction:** Extract fields, values, operators
- **Operator Recognition:** Detect AND, OR, comparison operators

**Example:**
```python
Query: "Show me total sales by region for 2023"

Parsed:
{
  "intents": ["filter", "group", "aggregate"],
  "entities": {
    "fields": ["sales", "region"],
    "years": [2023],
    "operators": []
  },
  "tasks": [...]
}
```

---

### 2. Task Planning (DAG)

**Algorithm:** Kahn's Topological Sort

**Process:**
1. Build adjacency list from task dependencies
2. Calculate in-degree for each task
3. Group tasks into execution layers
4. Tasks in same layer can execute in parallel

**Example DAG:**
```
Layer 1: [T1: Filter]
         ↓
Layer 2: [T2: Group]
         ↓
Layer 3: [T3: Aggregate]

Max Parallelism: 1 (sequential in this case)
```

**Parallel Example:**
```
Layer 1: [T1: Filter Year=2022]  [T2: Filter Year=2023]  (Parallel!)
         ↓                        ↓
Layer 2: [T3: Aggregate T1]      [T4: Aggregate T2]      (Parallel!)
         ↓                        ↓
Layer 3: [T5: Compare T3 and T4]

Max Parallelism: 2 tasks
```

---

### 3. Parallel Execution

**Technology:** Python `multiprocessing`

**Process:**
1. Create worker pool with N processes
2. Distribute tasks across workers
3. Execute each layer sequentially
4. Within each layer, execute tasks in parallel
5. Synchronize between layers

**Performance Formula:**
```
Speedup = Sequential Time / Parallel Time
Efficiency = Speedup / Number of Processors
```

**Example:**
```python
# 4 independent tasks, 4 processors
Sequential Time: 4 × 0.1s = 0.4s
Parallel Time: max(0.1s) = 0.1s
Speedup: 4x
Efficiency: 100%
```

---

### 4. Result Aggregation

**Operations:**
- **Merge:** Concatenate DataFrames
- **Aggregate:** Sum, Average, Count, Max, Min
- **Format:** Convert to JSON, HTML, or text

---

## 📊 Performance Metrics

The system tracks:

1. **Total Execution Time:** End-to-end query processing time
2. **Task Execution Times:** Individual task durations
3. **Number of Processes Used:** Actual parallelism achieved
4. **Speedup Factor:** Parallel vs sequential performance
5. **Memory Usage:** RAM consumption per task

### Expected Performance

| Query Complexity | Tasks | Sequential Time | Parallel Time (4 cores) | Speedup |
|-----------------|-------|----------------|------------------------|---------|
| Simple          | 3     | 0.15s          | 0.05s                  | 3x      |
| Medium          | 6     | 0.30s          | 0.10s                  | 3x      |
| Complex         | 10    | 0.50s          | 0.15s                  | 3.3x    |

---

## 🎓 Academic Concepts Demonstrated

### 1. Natural Language Processing
- Text tokenization
- Stop word filtering
- Intent classification
- Named entity recognition

### 2. Algorithm Design
- Directed Acyclic Graphs (DAG)
- Topological sorting
- Task scheduling
- Dependency resolution

### 3. Parallel Computing
- Multiprocessing
- Load balancing
- Synchronization
- Race condition avoidance

### 4. Database Operations
- Query optimization
- Index usage
- Aggregation functions
- Join operations

### 5. Software Engineering
- Modular design
- API development
- Web interface
- Error handling

---

## 🧪 Testing

### Unit Tests

Test individual components:

```bash
# Test NLP Processor
python -c "from nlp_processor import NLPQueryProcessor; 
           p = NLPQueryProcessor(); 
           print(p.decompose_query('Show me sales by region for 2023'))"

# Test Task Planner
python -c "from task_planner import TaskPlanner; 
           tasks = [{'task_id': 'T1', 'operation': 'filter', 'depends_on': []}];
           p = TaskPlanner(tasks);
           print(p.create_execution_plan())"
```

### Integration Tests

Run complete query pipeline:

```bash
python app.py
# Then use web interface to test various queries
```

---

## 🐛 Troubleshooting

### Issue: "Module not found"
**Solution:** Install dependencies
```bash
pip install -r requirements.txt
```

### Issue: "Address already in use"
**Solution:** Change Flask port
```python
# In app.py, change:
app.run(debug=True, host='0.0.0.0', port=5001)  # Use different port
```

### Issue: "Dataset not found"
**Solution:** Dataset is auto-generated on first run. Or manually run:
```bash
python dataset_generator.py
```

### Issue: "Slow performance"
**Solution:** Reduce dataset size or increase process count
```python
# In dataset_generator.py, change:
generator = DatasetGenerator(num_records=5000)  # Reduce from 10000
```

---

## 📚 References

### Academic Papers
- "Query Optimization in Distributed Databases"
- "Natural Language Interfaces for Databases"
- "Parallel Query Processing Techniques"

### Technologies Used
- **Flask:** Web framework
- **Pandas:** Data manipulation
- **Multiprocessing:** Parallel execution
- **Python 3.x:** Core language

---

## 👨‍💻 Author Information

**Project Type:** Final Year Academic Project

**Course:** Parallel and Distributed Computing

**Features:**
- NLP-based query understanding
- Automatic task decomposition
- Parallel execution framework
- Web-based demonstration

---

## 📄 License

This project is created for academic purposes.

---

## 🎯 Future Enhancements

1. **Advanced NLP:** Use transformers (BERT, GPT) for better understanding
2. **Real Hadoop Integration:** Deploy on actual Hadoop cluster
3. **Query Caching:** Store and reuse common query results
4. **ML Optimization:** Learn optimal execution plans
5. **Multi-Table Support:** Join operations across multiple tables
6. **Distributed Storage:** Implement HDFS-like data distribution
7. **Real-time Processing:** Stream processing capabilities
8. **Visualization:** Add graphs and charts for results

---

## 📞 Support

For questions or issues:
1. Check the troubleshooting section
2. Review the code comments
3. Test individual modules separately
4. Check Python and package versions

---

## ✅ Checklist for Submission

- [x] All Python modules implemented
- [x] Flask web interface working
- [x] Dataset generator functional
- [x] Example queries provided
- [x] Documentation complete
- [x] Code well-commented
- [x] Performance metrics tracked
- [x] Error handling implemented

---

**End of Documentation**