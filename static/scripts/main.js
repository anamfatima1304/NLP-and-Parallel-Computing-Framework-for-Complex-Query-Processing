// Global state
let selectedProcessors = 1;

// DOM elements
const queryInput = document.getElementById('query-input');
const executeBtn = document.getElementById('execute-btn');
const loading = document.getElementById('loading');
const processorBtns = document.querySelectorAll('.processor-btn');
const analysisPanel = document.getElementById('analysis-panel');
const performancePanel = document.getElementById('performance-panel');
const resultsPanel = document.getElementById('results-panel');
const errorPanel = document.getElementById('error-panel');

// Initialize processor selection
processorBtns.forEach(btn => {
    btn.addEventListener('click', () => {
        processorBtns.forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        selectedProcessors = parseInt(btn.getAttribute('data-processors'));
    });
});

// Execute query
executeBtn.addEventListener('click', async () => {
    const query = queryInput.value.trim();
    
    if (!query) {
        showError('Please enter a query');
        return;
    }
    
    // Hide previous results
    hideAllPanels();
    
    // Show loading
    loading.classList.remove('hidden');
    executeBtn.disabled = true;
    
    try {
        const response = await fetch('/api/execute-query', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                query: query,
                processors: selectedProcessors
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            displayResults(data);
        } else {
            showError(data.error || 'An error occurred during execution');
        }
        
    } catch (error) {
        showError('Network error: ' + error.message);
    } finally {
        loading.classList.add('hidden');
        executeBtn.disabled = false;
    }
});

// Display results
function displayResults(data) {
    // Show analysis
    displayAnalysis(data.query_components, data.execution_plan);
    
    // Show performance
    displayPerformance(data.performance, data.results);
    
    // Show results
    displayQueryResults(data.results);
    
    // Show panels
    analysisPanel.classList.remove('hidden');
    performancePanel.classList.remove('hidden');
    resultsPanel.classList.remove('hidden');
}

// Display query analysis
function displayAnalysis(components, plan) {
    // Filters
    const filtersList = document.getElementById('filters-list');
    filtersList.innerHTML = '';
    if (components.filters && components.filters.length > 0) {
        components.filters.forEach(filter => {
            const item = document.createElement('div');
            item.className = 'info-list-item';
            item.textContent = `${filter.field} ${filter.operator} ${filter.value}`;
            filtersList.appendChild(item);
        });
    } else {
        filtersList.innerHTML = '<div class="info-list-item">No filters applied</div>';
    }
    
    // Groupings
    const groupingsList = document.getElementById('groupings-list');
    groupingsList.innerHTML = '';
    if (components.groupings && components.groupings.length > 0) {
        components.groupings.forEach(group => {
            const item = document.createElement('div');
            item.className = 'info-list-item';
            item.textContent = group;
            groupingsList.appendChild(item);
        });
    } else {
        groupingsList.innerHTML = '<div class="info-list-item">No grouping</div>';
    }
    
    // Aggregations
    const aggsList = document.getElementById('aggregations-list');
    aggsList.innerHTML = '';
    if (components.aggregations && components.aggregations.length > 0) {
        components.aggregations.forEach(agg => {
            const item = document.createElement('div');
            item.className = 'info-list-item';
            item.textContent = `${agg.type}(${agg.field})`;
            aggsList.appendChild(item);
        });
    } else {
        aggsList.innerHTML = '<div class="info-list-item">No aggregations</div>';
    }
    
    // Execution plan
    document.getElementById('total-tasks').textContent = plan.total_tasks;
    document.getElementById('total-layers').textContent = plan.total_layers;
    document.getElementById('max-parallelism').textContent = plan.max_parallelism;
}

// Display performance metrics
function displayPerformance(performance, results) {
    document.getElementById('exec-time').textContent = `${performance.execution_time}s`;
    document.getElementById('processors-used').textContent = performance.processors_used;
    document.getElementById('tasks-executed').textContent = performance.tasks_executed;
    
    // Show actual row count from results
    const rowCount = results.row_count !== null && results.row_count !== undefined ? results.row_count : 'N/A';
    document.getElementById('result-rows').textContent = rowCount;
}

// Display query results
function displayQueryResults(results) {
    const container = document.getElementById('results-container');
    container.innerHTML = '';
    
    if (results.type === 'dataframe' && results.data) {
        // Create table
        const table = document.createElement('table');
        table.className = 'results-table';
        
        // Create header
        const thead = document.createElement('thead');
        const headerRow = document.createElement('tr');
        const columns = Object.keys(results.data[0]);
        
        columns.forEach(col => {
            const th = document.createElement('th');
            th.textContent = col;
            headerRow.appendChild(th);
        });
        
        thead.appendChild(headerRow);
        table.appendChild(thead);
        
        // Create body
        const tbody = document.createElement('tbody');
        results.data.forEach(row => {
            const tr = document.createElement('tr');
            columns.forEach(col => {
                const td = document.createElement('td');
                td.textContent = row[col];
                tr.appendChild(td);
            });
            tbody.appendChild(tr);
        });
        
        table.appendChild(tbody);
        container.appendChild(table);
        
    } else {
        // Display as text
        const textDiv = document.createElement('div');
        textDiv.className = 'results-text';
        textDiv.textContent = results.data;
        container.appendChild(textDiv);
    }
}

// Show error
function showError(message) {
    hideAllPanels();
    errorPanel.classList.remove('hidden');
    document.getElementById('error-message').textContent = message;
}

// Hide all result panels
function hideAllPanels() {
    analysisPanel.classList.add('hidden');
    performancePanel.classList.add('hidden');
    resultsPanel.classList.add('hidden');
    errorPanel.classList.add('hidden');
}

// Allow Enter key to execute (Ctrl+Enter or Cmd+Enter)
queryInput.addEventListener('keydown', (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        executeBtn.click();
    }
});