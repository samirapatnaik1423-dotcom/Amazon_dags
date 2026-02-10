// Global Variables
let dagCache = [];
let charts = {};
let autoRefreshInterval = null;
let autoRefreshEnabled = false;

// ============================================================================
// Auto-Refresh Functionality
// ============================================================================

function toggleAutoRefresh() {
    const checkbox = document.getElementById('autoRefreshToggle');
    autoRefreshEnabled = checkbox.checked;
    
    if (autoRefreshEnabled) {
        // Start auto-refresh every 30 seconds
        autoRefreshInterval = setInterval(() => {
            console.log('Auto-refreshing dashboard data...');
            const currentView = document.querySelector('.view-section:not([style*="display: none"])');
            if (currentView && currentView.id === 'overview-view') {
                loadOverviewData();
            }
        }, 30000); // 30 seconds
        
        console.log('Auto-refresh enabled (30s interval)');
    } else {
        // Stop auto-refresh
        if (autoRefreshInterval) {
            clearInterval(autoRefreshInterval);
            autoRefreshInterval = null;
        }
        console.log('Auto-refresh disabled');
    }
}

// ============================================================================
// Utility Functions
// ============================================================================

function showLoading(show = true) {
    const overlay = document.getElementById('loadingOverlay');
    if (show) {
        overlay.classList.add('active');
    } else {
        overlay.classList.remove('active');
    }
}

function showView(viewName) {
    // Hide all views
    document.querySelectorAll('.view-section').forEach(view => {
        view.style.display = 'none';
    });
    
    // Show selected view
    const selectedView = document.getElementById(`${viewName}-view`);
    if (selectedView) {
        selectedView.style.display = 'block';
    }
    
    // Update active nav item
    document.querySelectorAll('.nav-item').forEach(item => {
        item.classList.remove('active');
    });
    event.target.classList.add('active');
    
    // Load data for view
    loadViewData(viewName);
}

function switchTab(tabName) {
    const parentContainer = event.target.closest('.tab-container');
    
    // Hide all tab contents in this container
    parentContainer.querySelectorAll('.tab-content').forEach(content => {
        content.classList.remove('active');
    });
    
    // Remove active class from all tab buttons in this container
    parentContainer.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    
    // Show selected tab
    const selectedTab = document.getElementById(tabName);
    if (selectedTab) {
        selectedTab.classList.add('active');
    }
    
    // Add active class to clicked button
    event.target.classList.add('active');
}

function formatDate(dateString) {
    if (!dateString) return '-';
    const date = new Date(dateString);
    return date.toLocaleString('en-US', { 
        month: 'short', 
        day: 'numeric', 
        hour: '2-digit', 
        minute: '2-digit' 
    });
}

function formatDuration(seconds) {
    if (!seconds) return '-';
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = Math.floor(seconds % 60);
    
    if (hours > 0) {
        return `${hours}h ${minutes}m`;
    } else if (minutes > 0) {
        return `${minutes}m ${secs}s`;
    } else {
        return `${secs}s`;
    }
}

function getStatusBadge(status) {
    const statusLower = (status || '').toLowerCase();
    const classMap = {
        'success': 'success',
        'failed': 'failed',
        'running': 'running',
        'pending': 'pending',
        'queued': 'pending',
        'skipped': 'pending'
    };
    const badgeClass = classMap[statusLower] || 'pending';
    return `<span class="badge ${badgeClass}">${status}</span>`;
}

async function fetchAPI(endpoint) {
    try {
        const response = await fetch(`/api${endpoint}`);
        const data = await response.json();
        return data;
    } catch (error) {
        console.error('API Error:', error);
        return { success: false, error: error.message };
    }
}

// ============================================================================
// View Data Loaders
// ============================================================================

async function loadViewData(viewName) {
    switch(viewName) {
        case 'overview':
            await loadOverviewData();
            break;
        case 'dags':
            await loadAllDAGs();
            break;
        case 'monitoring':
            await loadMonitoringData();
            break;
        case 'quality':
            await loadQualityScorecard();
            break;
        case 'logs':
            await populateLogSelects();
            break;
        case 'metadata':
            await loadMetadataSummary();
            break;
        case 'downloads':
            await loadDownloadsData();
            break;
    }
}

// ============================================================================
// Overview Section
// ============================================================================

async function loadOverviewData() {
    showLoading(true);
    
    // Add refreshing animation to button
    const refreshBtn = document.getElementById('refreshBtn');
    if (refreshBtn) {
        refreshBtn.classList.add('refreshing');
        const icon = refreshBtn.querySelector('i');
        if (icon) {
            icon.style.animation = 'spin 1s linear infinite';
        }
    }
    
    try {
        // Load DAGs for stats
        const dagsData = await fetchAPI('/dags');
        
        if (dagsData.success && dagsData.data) {
            dagCache = dagsData.data;
            
            // Update stats
            document.getElementById('totalDAGs').textContent = dagsData.data.length;
            
            // Calculate success rate from recent runs
            let totalRuns = 0;
            let successfulRuns = 0;
            
            dagsData.data.forEach(dag => {
                if (dag.latest_run) {
                    totalRuns++;
                    if (dag.latest_run.state === 'success') {
                        successfulRuns++;
                    }
                }
            });
            
            const successRate = totalRuns > 0 ? Math.round((successfulRuns / totalRuns) * 100) : 0;
            document.getElementById('successRate').textContent = `${successRate}%`;
            
            // Count active runs
            const activeRuns = dagsData.data.filter(dag => 
                dag.latest_run && dag.latest_run.state === 'running'
            ).length;
            document.getElementById('activeRuns').textContent = activeRuns;
        }
        
        // Load quality summary
        const qualityData = await fetchAPI('/quality/summary');
        if (qualityData.success && qualityData.data) {
            // Calculate average score from all tables
            let totalScore = 0;
            let tableCount = 0;
            
            if (Array.isArray(qualityData.data)) {
                qualityData.data.forEach(table => {
                    if (table.overall_score) {
                        totalScore += parseFloat(table.overall_score);
                        tableCount++;
                    }
                });
            } else if (qualityData.data.overall_score) {
                totalScore = parseFloat(qualityData.data.overall_score);
                tableCount = 1;
            }
            
            const avgScore = tableCount > 0 ? Math.round(totalScore / tableCount) : 0;
            document.getElementById('dataQuality').textContent = `${avgScore}%`;
        } else {
            document.getElementById('dataQuality').textContent = 'N/A';
        }
        
        // Render charts
        await renderDAGTimelineChart();
        await renderTaskStatusChart();
        
        // Populate all select dropdowns
        populateDagSelects();
        
    } catch (error) {
        console.error('Error loading overview:', error);
    } finally {
        showLoading(false);
        
        // Remove refreshing animation
        const refreshBtn = document.getElementById('refreshBtn');
        if (refreshBtn) {
            refreshBtn.classList.remove('refreshing');
            const icon = refreshBtn.querySelector('i');
            if (icon) {
                icon.style.animation = '';
            }
        }
    }
}

async function renderDAGTimelineChart() {
    const canvas = document.getElementById('dagTimelineChart');
    const ctx = canvas.getContext('2d');
    
    // Destroy existing chart
    if (charts.timeline) {
        charts.timeline.destroy();
    }
    
    // Generate sample data for last 7 days
    const labels = [];
    const successData = [];
    const failedData = [];
    const runningData = [];
    
    for (let i = 6; i >= 0; i--) {
        const date = new Date();
        date.setDate(date.getDate() - i);
        labels.push(date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }));
        
        // Mock data - in production, fetch actual data
        successData.push(Math.floor(Math.random() * 20) + 10);
        failedData.push(Math.floor(Math.random() * 3));
        runningData.push(Math.floor(Math.random() * 2));
    }
    
    charts.timeline = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Success',
                    data: successData,
                    borderColor: '#48bb78',
                    backgroundColor: 'rgba(72, 187, 120, 0.1)',
                    tension: 0.4,
                    fill: true
                },
                {
                    label: 'Failed',
                    data: failedData,
                    borderColor: '#f56565',
                    backgroundColor: 'rgba(245, 101, 101, 0.1)',
                    tension: 0.4,
                    fill: true
                },
                {
                    label: 'Running',
                    data: runningData,
                    borderColor: '#667eea',
                    backgroundColor: 'rgba(102, 126, 234, 0.1)',
                    tension: 0.4,
                    fill: true
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top',
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        precision: 0
                    }
                }
            }
        }
    });
}

async function renderTaskStatusChart() {
    const canvas = document.getElementById('taskStatusChart');
    const ctx = canvas.getContext('2d');
    
    // Destroy existing chart
    if (charts.taskStatus) {
        charts.taskStatus.destroy();
    }
    
    // Mock data - in production, fetch actual task statistics
    charts.taskStatus = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: ['Success', 'Failed', 'Running', 'Queued'],
            datasets: [{
                data: [245, 12, 8, 15],
                backgroundColor: [
                    '#48bb78',
                    '#f56565',
                    '#667eea',
                    '#ed8936'
                ],
                borderWidth: 0
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'right',
                }
            }
        }
    });
}

// ============================================================================
// DAG Management Section
// ============================================================================

function populateDagSelects() {
    const dagOptions = dagCache.map(dag => 
        `<option value="${dag.dag_id}">${dag.dag_id}</option>`
    ).join('');
    
    document.querySelectorAll('select[id*="dag" i][id*="select" i]').forEach(select => {
        const currentValue = select.value;
        select.innerHTML = '<option value="">Select a DAG...</option>' + dagOptions;
        if (currentValue) {
            select.value = currentValue;
        }
    });
}

async function loadAllDAGs() {
    showLoading(true);
    
    try {
        const data = await fetchAPI('/dags');
        
        if (!data.success || !data.data) {
            document.getElementById('dagListContainer').innerHTML = `
                <div class="empty-state">
                    <i class="fas fa-exclamation-circle"></i>
                    <h3>Failed to load DAGs</h3>
                    <p>${data.error || 'Unknown error'}</p>
                </div>
            `;
            return;
        }
        
        dagCache = data.data;
        populateDagSelects();
        
        let html = '<table class="data-table"><thead><tr>';
        html += '<th>DAG ID</th>';
        html += '<th>Active</th>';
        html += '<th>Paused</th>';
        html += '<th>Latest Run</th>';
        html += '<th>State</th>';
        html += '<th>Last Run Date</th>';
        html += '</tr></thead><tbody>';
        
        data.data.forEach(dag => {
            html += '<tr>';
            html += `<td><strong>${dag.dag_id}</strong></td>`;
            html += `<td>${dag.is_active ? '✓' : '✗'}</td>`;
            html += `<td>${dag.is_paused ? 'Yes' : 'No'}</td>`;
            html += `<td>${dag.latest_run ? dag.latest_run.run_id : '-'}</td>`;
            html += `<td>${dag.latest_run ? getStatusBadge(dag.latest_run.state) : '-'}</td>`;
            html += `<td>${dag.latest_run ? formatDate(dag.latest_run.execution_date) : '-'}</td>`;
            html += '</tr>';
        });
        
        html += '</tbody></table>';
        document.getElementById('dagListContainer').innerHTML = html;
        
    } catch (error) {
        console.error('Error loading DAGs:', error);
    } finally {
        showLoading(false);
    }
}

function filterDAGs() {
    const searchTerm = document.getElementById('dagSearchInput').value.toLowerCase();
    const rows = document.querySelectorAll('#dagListContainer tbody tr');
    
    rows.forEach(row => {
        const text = row.textContent.toLowerCase();
        row.style.display = text.includes(searchTerm) ? '' : 'none';
    });
}

async function loadDAGRuns() {
    const dagId = document.getElementById('dagRunsSelect').value;
    const stateFilter = document.getElementById('dagRunsStateFilter').value;
    
    if (!dagId) {
        document.getElementById('dagRunsContainer').innerHTML = `
            <div class="empty-state">
                <i class="fas fa-info-circle"></i>
                <h3>Select a DAG</h3>
                <p>Choose a DAG from the dropdown to view its runs</p>
            </div>
        `;
        return;
    }
    
    showLoading(true);
    
    try {
        let endpoint = `/dags/${dagId}/runs?page_size=50`;
        if (stateFilter) {
            endpoint += `&state=${stateFilter}`;
        }
        
        const data = await fetchAPI(endpoint);
        
        if (!data.success || !data.data || !data.data.runs) {
            document.getElementById('dagRunsContainer').innerHTML = `
                <div class="empty-state">
                    <i class="fas fa-inbox"></i>
                    <h3>No Runs Found</h3>
                    <p>No runs found for this DAG</p>
                </div>
            `;
            return;
        }
        
        // Build timeline
        let html = '<div class="timeline">';
        
        data.data.runs.forEach(run => {
            const statusClass = run.state.toLowerCase();
            html += `<div class="timeline-item ${statusClass}">`;
            html += '<div class="timeline-content">';
            html += `<h4>${run.run_id}</h4>`;
            html += `<p><strong>State:</strong> ${getStatusBadge(run.state)}</p>`;
            html += `<p><strong>Execution Date:</strong> ${formatDate(run.execution_date)}</p>`;
            html += `<p><strong>Start Date:</strong> ${formatDate(run.start_date)}</p>`;
            html += `<p><strong>End Date:</strong> ${formatDate(run.end_date)}</p>`;
            
            if (run.duration) {
                html += `<p><strong>Duration:</strong> ${formatDuration(run.duration)}</p>`;
            }
            
            html += '</div></div>';
        });
        
        html += '</div>';
        document.getElementById('dagRunsContainer').innerHTML = html;
        
    } catch (error) {
        console.error('Error loading DAG runs:', error);
    } finally {
        showLoading(false);
    }
}

async function loadDAGTasks() {
    const dagId = document.getElementById('dagTasksSelect').value;
    
    if (!dagId) {
        document.getElementById('runTasksSelect').innerHTML = '<option value="">Select a DAG first...</option>';
        return;
    }
    
    showLoading(true);
    
    try {
        const data = await fetchAPI(`/dags/${dagId}/runs?page_size=20`);
        
        if (data.success && data.data && data.data.runs) {
            const runOptions = data.data.runs.map(run => 
                `<option value="${run.run_id}">${run.run_id} (${run.state})</option>`
            ).join('');
            document.getElementById('runTasksSelect').innerHTML = '<option value="">Select a run...</option>' + runOptions;
        }
        
    } catch (error) {
        console.error('Error loading runs:', error);
    } finally {
        showLoading(false);
    }
}

async function loadTasksForRun() {
    const dagId = document.getElementById('dagTasksSelect').value;
    const runId = document.getElementById('runTasksSelect').value;
    
    if (!dagId || !runId) {
        document.getElementById('dagTasksContainer').innerHTML = `
            <div class="empty-state">
                <i class="fas fa-info-circle"></i>
                <h3>Select DAG and Run</h3>
                <p>Choose both a DAG and a run to view tasks</p>
            </div>
        `;
        return;
    }
    
    showLoading(true);
    
    try {
        const data = await fetchAPI(`/dags/${dagId}/runs/${runId}/tasks`);
        
        if (!data.success || !data.data) {
            document.getElementById('dagTasksContainer').innerHTML = `
                <div class="empty-state">
                    <i class="fas fa-inbox"></i>
                    <h3>No Tasks Found</h3>
                    <p>No tasks found for this run</p>
                </div>
            `;
            return;
        }
        
        let html = '<table class="data-table"><thead><tr>';
        html += '<th>Task ID</th>';
        html += '<th>State</th>';
        html += '<th>Start Date</th>';
        html += '<th>End Date</th>';
        html += '<th>Duration</th>';
        html += '<th>Try Number</th>';
        html += '</tr></thead><tbody>';
        
        data.data.forEach(task => {
            html += '<tr>';
            html += `<td><strong>${task.task_id}</strong></td>`;
            html += `<td>${getStatusBadge(task.state)}</td>`;
            html += `<td>${formatDate(task.start_date)}</td>`;
            html += `<td>${formatDate(task.end_date)}</td>`;
            html += `<td>${task.duration ? formatDuration(task.duration) : '-'}</td>`;
            html += `<td>${task.try_number || '-'}</td>`;
            html += '</tr>';
        });
        
        html += '</tbody></table>';
        document.getElementById('dagTasksContainer').innerHTML = html;
        
    } catch (error) {
        console.error('Error loading tasks:', error);
    } finally {
        showLoading(false);
    }
}

// ============================================================================
// Monitoring Section
// ============================================================================

async function loadMonitoringData() {
    showLoading(true);
    
    try {
        // Load metrics
        const metricsData = await fetchAPI('/metadata/metrics');
        await renderMetricsChart(metricsData);
        
        // Load table stats
        const tableData = await fetchAPI('/metadata/tables');
        renderTableStats(tableData);
        
    } catch (error) {
        console.error('Error loading monitoring data:', error);
    } finally {
        showLoading(false);
    }
}

async function renderMetricsChart(data) {
    const canvas = document.getElementById('metricsChart');
    const ctx = canvas.getContext('2d');
    
    // Destroy existing chart
    if (charts.metrics) {
        charts.metrics.destroy();
    }
    
    console.log('[DEBUG] Full metrics response:', data);
    
    // Check if request was successful
    if (!data.success) {
        console.error('Failed to load metrics:', data.error);
        return;
    }
    
    // Flask wraps the FastAPI response: {success: true, data: [array]}
    const metricsArray = Array.isArray(data.data) ? data.data : [];
    
    console.log('[DEBUG] Metrics array:', metricsArray);
    
    if (metricsArray.length === 0) {
        console.log('No metrics data available - check if FastAPI server is running at localhost:8000');
        return;
    }
    
    // Extract metrics
    const labels = metricsArray.map(m => m.table_name || 'Unknown');
    const rowsExtracted = metricsArray.map(m => m.rows_extracted || 0);
    const rowsLoaded = metricsArray.map(m => m.rows_loaded || 0);
    const rowsRejected = metricsArray.map(m => m.rows_rejected || 0);
    
    charts.metrics = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Rows Extracted',
                    data: rowsExtracted,
                    backgroundColor: '#667eea',
                },
                {
                    label: 'Rows Loaded',
                    data: rowsLoaded,
                    backgroundColor: '#48bb78',
                },
                {
                    label: 'Rows Rejected',
                    data: rowsRejected,
                    backgroundColor: '#f56565',
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top',
                },
                title: {
                    display: false
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        precision: 0
                    }
                }
            }
        }
    });
}

function renderTableStats(data) {
    const container = document.getElementById('tableStatsContainer');
    
    if (!data.success || !data.data || data.data.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <i class="fas fa-database"></i>
                <h3>No Table Data Available</h3>
                <p>Table statistics will appear here once data is loaded</p>
            </div>
        `;
        return;
    }
    
    let html = '<table class="data-table"><thead><tr>';
    html += '<th>Table Name</th>';
    html += '<th>Schema</th>';
    html += '<th>Row Count</th>';
    html += '<th>Size</th>';
    html += '<th>Last Updated</th>';
    html += '</tr></thead><tbody>';
    
    data.data.forEach(table => {
        html += '<tr>';
        html += `<td><strong>${table.table_name}</strong></td>`;
        html += `<td>${table.schema_name || 'etl_output'}</td>`;
        html += `<td>${table.row_count?.toLocaleString() || '0'}</td>`;
        html += `<td>${table.size || '-'}</td>`;
        html += `<td>${formatDate(table.last_updated)}</td>`;
        html += '</tr>';
    });
    
    html += '</tbody></table>';
    container.innerHTML = html;
}

// ============================================================================
// Data Quality Section
// ============================================================================

async function loadQualityScorecard() {
    const table = document.getElementById('scorecardTableSelect')?.value || 'all';
    
    showLoading(true);
    
    try {
        const endpoint = table === 'all' ? '/quality/scorecard' : `/quality/scorecard/${table}`;
        const data = await fetchAPI(endpoint);
        
        if (!data.success || !data.data) {
            document.getElementById('qualityScorecardContainer').innerHTML = `
                <div class="empty-state">
                    <i class="fas fa-exclamation-circle"></i>
                    <h3>No Quality Data</h3>
                    <p>${data.error || 'No scorecard data available'}</p>
                </div>
            `;
            return;
        }
        
        // FastAPI returns {scorecards: [...]} for 'all', or single object for specific table
        let scores;
        if (table === 'all' && data.data.scorecards) {
            scores = data.data.scorecards;
        } else if (Array.isArray(data.data)) {
            scores = data.data;
        } else {
            scores = [data.data];
        }
        
        let html = '<table class="data-table"><thead><tr>';
        html += '<th>Table Name</th>';
        html += '<th>Overall Score</th>';
        html += '<th>Completeness</th>';
        html += '<th>Accuracy</th>';
        html += '<th>Consistency</th>';
        html += '<th>Last Check</th>';
        html += '</tr></thead><tbody>';
        
        scores.forEach(score => {
            html += '<tr>';
            html += `<td><strong>${score.table_name}</strong></td>`;
            html += `<td><strong style="color: #667eea;">${score.overall_score || 0}%</strong></td>`;
            html += `<td>${score.completeness_score || 0}%</td>`;
            html += `<td>${score.accuracy_score || 0}%</td>`;
            html += `<td>${score.consistency_score || 0}%</td>`;
            html += `<td>${formatDate(score.generated_at)}</td>`;
            html += '</tr>';
        });
        
        html += '</tbody></table>';
        document.getElementById('qualityScorecardContainer').innerHTML = html;
        
    } catch (error) {
        console.error('Error loading quality scorecard:', error);
    } finally {
        showLoading(false);
    }
}

async function loadValidationResults() {
    const table = document.getElementById('validationTableFilter').value;
    const failedOnly = document.getElementById('validationFailedOnly').checked;
    
    showLoading(true);
    
    try {
        let endpoint = '/quality/validation-results?';
        if (table) endpoint += `table_name=${table}&`;
        if (failedOnly) endpoint += 'failed_only=true';
        
        const data = await fetchAPI(endpoint);
        
        if (!data.success || !data.data) {
            document.getElementById('validationResultsContainer').innerHTML = `
                <div class="empty-state">
                    <i class="fas fa-check-circle"></i>
                    <h3>No Validation Results</h3>
                    <p>No validation results found matching your criteria</p>
                </div>
            `;
            return;
        }
        
        // API returns {summary: {}, results: []}
        const results = data.data.results || [];
        
        if (results.length === 0) {
            document.getElementById('validationResultsContainer').innerHTML = `
                <div class="empty-state">
                    <i class="fas fa-check-circle"></i>
                    <h3>All Validations Passed!</h3>
                    <p>No validation issues found</p>
                </div>
            `;
            return;
        }
        
        let html = '<table class="data-table"><thead><tr>';
        html += '<th>Table</th>';
        html += '<th>Column</th>';
        html += '<th>Validation Rule</th>';
        html += '<th>Status</th>';
        html += '<th>Records Checked</th>';
        html += '<th>Failed Records</th>';
        html += '<th>Check Date</th>';
        html += '</tr></thead><tbody>';
        
        results.forEach(result => {
            const passed = result.failed_records === 0;
            html += '<tr>';
            html += `<td>${result.table_name}</td>`;
            html += `<td>${result.column_name}</td>`;
            html += `<td>${result.validation_rule}</td>`;
            html += `<td>${passed ? getStatusBadge('success') : getStatusBadge('failed')}</td>`;
            html += `<td>${result.records_checked?.toLocaleString() || 0}</td>`;
            html += `<td>${result.failed_records?.toLocaleString() || 0}</td>`;
            html += `<td>${formatDate(result.check_date)}</td>`;
            html += '</tr>';
        });
        
        html += '</tbody></table>';
        document.getElementById('validationResultsContainer').innerHTML = html;
        
    } catch (error) {
        console.error('Error loading validation results:', error);
    } finally {
        showLoading(false);
    }
}

async function loadAnomalies() {
    const table = document.getElementById('anomaliesTableFilter').value;
    const limit = document.getElementById('anomaliesLimit').value;
    
    showLoading(true);
    
    try {
        let endpoint = '/quality/anomalies?';
        if (table) endpoint += `table=${table}&`;
        endpoint += `limit=${limit}`;
        
        const data = await fetchAPI(endpoint);
        
        if (!data.success || !data.data) {
            document.getElementById('anomaliesContainer').innerHTML = `
                <div class="empty-state">
                    <i class="fas fa-shield-alt"></i>
                    <h3>No Anomalies Detected</h3>
                    <p>${data.error || 'No anomalies found - your data looks good!'}</p>
                </div>
            `;
            return;
        }
        
        // API returns {anomalies: []} structure
        const anomalies = data.data.anomalies || [];
        
        if (anomalies.length === 0) {
            document.getElementById('anomaliesContainer').innerHTML = `
                <div class="empty-state">
                    <i class="fas fa-shield-alt"></i>
                    <h3>No Anomalies Detected</h3>
                    <p>Your data looks clean!</p>
                </div>
            `;
            return;
        }
        
        let html = '<table class="data-table"><thead><tr>';
        html += '<th>Table</th>';
        html += '<th>Column</th>';
        html += '<th>Record ID</th>';
        html += '<th>Value</th>';
        html += '<th>Z-Score</th>';
        html += '<th>Detected Date</th>';
        html += '</tr></thead><tbody>';
        
        data.data.forEach(anomaly => {
            html += '<tr>';
            html += `<td>${anomaly.table_name}</td>`;
            html += `<td>${anomaly.column_name}</td>`;
            html += `<td>${anomaly.record_id}</td>`;
            html += `<td>${anomaly.value}</td>`;
            html += `<td><strong style="color: #f56565;">${anomaly.z_score?.toFixed(2) || '-'}</strong></td>`;
            html += `<td>${formatDate(anomaly.detected_date)}</td>`;
            html += '</tr>';
        });
        
        html += '</tbody></table>';
        document.getElementById('anomaliesContainer').innerHTML = html;
        
    } catch (error) {
        console.error('Error loading anomalies:', error);
    } finally {
        showLoading(false);
    }
}

async function loadDataProfiles() {
    const table = document.getElementById('profilesTableSelect').value;
    
    showLoading(true);
    
    try {
        let endpoint = '/quality/profiles';
        if (table) endpoint += `?table_name=${table}`;
        
        const data = await fetchAPI(endpoint);
        
        if (!data.success || !data.data || data.data.length === 0) {
            document.getElementById('dataProfilesContainer').innerHTML = `
                <div class="empty-state">
                    <i class="fas fa-chart-area"></i>
                    <h3>No Profile Data</h3>
                    <p>No profiling data available</p>
                </div>
            `;
            return;
        }
        
        let html = '<table class="data-table"><thead><tr>';
        html += '<th>Table</th>';
        html += '<th>Column</th>';
        html += '<th>Data Type</th>';
        html += '<th>Null Count</th>';
        html += '<th>Distinct Values</th>';
        html += '<th>Min Value</th>';
        html += '<th>Max Value</th>';
        html += '<th>Mean</th>';
        html += '</tr></thead><tbody>';
        
        data.data.forEach(profile => {
            html += '<tr>';
            html += `<td>${profile.table_name}</td>`;
            html += `<td>${profile.column_name}</td>`;
            html += `<td>${profile.data_type}</td>`;
            html += `<td>${profile.null_count?.toLocaleString() || 0}</td>`;
            html += `<td>${profile.distinct_values?.toLocaleString() || 0}</td>`;
            html += `<td>${profile.min_value || '-'}</td>`;
            html += `<td>${profile.max_value || '-'}</td>`;
            html += `<td>${profile.mean ? parseFloat(profile.mean).toFixed(2) : '-'}</td>`;
            html += '</tr>';
        });
        
        html += '</tbody></table>';
        document.getElementById('dataProfilesContainer').innerHTML = html;
        
    } catch (error) {
        console.error('Error loading data profiles:', error);
    } finally {
        showLoading(false);
    }
}

// ============================================================================
// Logs Section
// ============================================================================

async function populateLogSelects() {
    // Populate DAG select
    if (dagCache.length === 0) {
        const data = await fetchAPI('/dags');
        if (data.success && data.data) {
            dagCache = data.data;
        }
    }
    
    populateDagSelects();
}

document.addEventListener('DOMContentLoaded', function() {
    // Set up event listeners for logs
    const logsDagSelect = document.getElementById('logsDagSelect');
    if (logsDagSelect) {
        logsDagSelect.addEventListener('change', async function() {
            const dagId = this.value;
            if (!dagId) return;
            
            const data = await fetchAPI(`/dags/${dagId}/runs?page_size=20`);
            if (data.success && data.data && data.data.runs) {
                const runOptions = data.data.runs.map(run => 
                    `<option value="${run.run_id}">${run.run_id} (${run.state})</option>`
                ).join('');
                document.getElementById('logsRunSelect').innerHTML = '<option value="">Select a run...</option>' + runOptions;
            }
        });
    }
    
    const logsRunSelect = document.getElementById('logsRunSelect');
    if (logsRunSelect) {
        logsRunSelect.addEventListener('change', async function() {
            const dagId = document.getElementById('logsDagSelect').value;
            const runId = this.value;
            if (!dagId || !runId) return;
            
            const data = await fetchAPI(`/dags/${dagId}/runs/${runId}/tasks`);
            if (data.success && data.data) {
                const taskOptions = data.data.map(task => 
                    `<option value="${task.task_id}">${task.task_id} (${task.state})</option>`
                ).join('');
                document.getElementById('logsTaskSelect').innerHTML = '<option value="">Select a task...</option>' + taskOptions;
            }
        });
    }
});

async function loadLogs() {
    const dagId = document.getElementById('logsDagSelect').value;
    const runId = document.getElementById('logsRunSelect').value;
    const taskId = document.getElementById('logsTaskSelect').value;
    
    if (!dagId) {
        document.getElementById('logViewer').innerHTML = `
            <div class="empty-state">
                <i class="fas fa-info-circle"></i>
                <h3>Select DAG</h3>
                <p>Choose a DAG to start viewing logs</p>
            </div>
        `;
        return;
    }
    
    showLoading(true);
    
    try {
        let endpoint = `/logs/${dagId}`;
        if (runId) endpoint += `/${runId}`;
        if (taskId) endpoint += `/${taskId}`;
        
        const data = await fetchAPI(endpoint);
        
        if (!data.success || !data.data) {
            document.getElementById('logViewer').innerHTML = `
                <div class="empty-state">
                    <i class="fas fa-exclamation-circle"></i>
                    <h3>No Logs Found</h3>
                    <p>${data.error || 'No logs available for this selection'}</p>
                </div>
            `;
            return;
        }
        
        // Parse and format logs
        let logs = data.data;
        if (typeof logs === 'string') {
            logs = logs.split('\n');
        } else if (Array.isArray(logs)) {
            // Already an array
        } else {
            logs = [JSON.stringify(logs, null, 2)];
        }
        
        let html = '';
        logs.forEach(line => {
            if (!line.trim()) return;
            
            let levelClass = '';
            if (line.includes('ERROR') || line.includes('CRITICAL')) {
                levelClass = 'level-error';
            } else if (line.includes('WARNING') || line.includes('WARN')) {
                levelClass = 'level-warning';
            } else if (line.includes('INFO')) {
                levelClass = 'level-info';
            }
            
            html += `<div class="log-line ${levelClass}">${line}</div>`;
        });
        
        document.getElementById('logViewer').innerHTML = html || '<div class="empty-state"><p>No logs to display</p></div>';
        
    } catch (error) {
        console.error('Error loading logs:', error);
    } finally {
        showLoading(false);
    }
}

// ============================================================================
// Metadata Section
// ============================================================================

async function loadMetadataSummary() {
    showLoading(true);
    
    try {
        const data = await fetchAPI('/metadata/summary');
        
        if (!data.success || !data.data) {
            document.getElementById('metadataSummaryContainer').innerHTML = `
                <div class="empty-state">
                    <i class="fas fa-exclamation-circle"></i>
                    <h3>No Metadata Available</h3>
                </div>
            `;
            return;
        }
        
        let html = '<div style="background: white; padding: 20px; border-radius: 8px;">';
        html += '<pre style="background: #f7fafc; padding: 20px; border-radius: 8px; overflow-x: auto;">';
        html += JSON.stringify(data.data, null, 2);
        html += '</pre></div>';
        
        document.getElementById('metadataSummaryContainer').innerHTML = html;
        
    } catch (error) {
        console.error('Error loading metadata summary:', error);
    } finally {
        showLoading(false);
    }
}

async function loadTableStats() {
    showLoading(true);
    
    try {
        const data = await fetchAPI('/metadata/tables');
        
        if (!data.success || !data.data) {
            document.getElementById('metadataTablesContainer').innerHTML = `
                <div class="empty-state">
                    <i class="fas fa-table"></i>
                    <h3>No Table Data</h3>
                </div>
            `;
            return;
        }
        
        let html = '<table class="data-table"><thead><tr>';
        html += '<th>Table Name</th>';
        html += '<th>Schema</th>';
        html += '<th>Row Count</th>';
        html += '<th>Size</th>';
        html += '<th>Last Updated</th>';
        html += '</tr></thead><tbody>';
        
        data.data.forEach(table => {
            html += '<tr>';
            html += `<td><strong>${table.table_name}</strong></td>`;
            html += `<td>${table.schema_name || 'public'}</td>`;
            html += `<td>${table.row_count?.toLocaleString() || '-'}</td>`;
            html += `<td>${table.size || '-'}</td>`;
            html += `<td>${formatDate(table.last_updated)}</td>`;
            html += '</tr>';
        });
        
        html += '</tbody></table>';
        document.getElementById('metadataTablesContainer').innerHTML = html;
        
    } catch (error) {
        console.error('Error loading table stats:', error);
    } finally {
        showLoading(false);
    }
}

async function loadMetrics() {
    showLoading(true);
    
    try {
        const data = await fetchAPI('/metadata/metrics');
        
        if (!data.success || !data.data) {
            document.getElementById('metadataMetricsContainer').innerHTML = `
                <div class="empty-state">
                    <i class="fas fa-chart-pie"></i>
                    <h3>No Metrics Available</h3>
                </div>
            `;
            return;
        }
        
        let html = '<table class="data-table"><thead><tr>';
        html += '<th>Table Name</th>';
        html += '<th>Rows Extracted</th>';
        html += '<th>Rows Loaded</th>';
        html += '<th>Rows Rejected</th>';
        html += '<th>Last Run</th>';
        html += '</tr></thead><tbody>';
        
        data.data.forEach(metric => {
            html += '<tr>';
            html += `<td><strong>${metric.table_name || 'Unknown'}</strong></td>`;
            html += `<td>${metric.rows_extracted?.toLocaleString() || 0}</td>`;
            html += `<td>${metric.rows_loaded?.toLocaleString() || 0}</td>`;
            html += `<td style="color: #f56565;"><strong>${metric.rows_rejected?.toLocaleString() || 0}</strong></td>`;
            html += `<td>${formatDate(metric.last_run_date)}</td>`;
            html += '</tr>';
        });
        
        html += '</tbody></table>';
        document.getElementById('metadataMetricsContainer').innerHTML = html;
        
    } catch (error) {
        console.error('Error loading metrics:', error);
    } finally {
        showLoading(false);
    }
}

// ============================================================================
// Data Downloads Section
// ============================================================================

async function loadDownloadsData() {
    showLoading(true);
    try {
        const response = await fetchAPI('/downloads/list');
        
        if (response.success && response.data) {
            const container = document.getElementById('downloadsContainer');
            let html = '';
            
            const categoryIcons = {
                'bronze': 'fa-database',
                'silver': 'fa-filter',
                'gold': 'fa-trophy',
                'processed': 'fa-cog',
                'reports': 'fa-chart-bar'
            };
            
            const categoryColors = {
                'bronze': '#cd7f32',
                'silver': '#c0c0c0',
                'gold': '#ffd700',
                'processed': '#667eea',
                'reports': '#48bb78'
            };
            
            // Define category order: Processed, Reports, Bronze, Silver, Gold
            const categoryOrder = ['processed', 'reports', 'bronze', 'silver', 'gold'];
            
            // Create cards for each category in specified order
            for (const category of categoryOrder) {
                const data = response.data[category];
                if (!data || data.files.length === 0) continue;
                
                const icon = categoryIcons[category] || 'fa-file';
                const color = categoryColors[category] || '#667eea';
                
                html += `
                    <div style="background: white; border-radius: 12px; padding: 25px; margin-bottom: 25px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); border-left: 4px solid ${color};">
                        <div style="display: flex; align-items: center; gap: 15px; margin-bottom: 20px;">
                            <div style="background: ${color}; color: white; width: 50px; height: 50px; border-radius: 10px; display: flex; align-items: center; justify-content: center;">
                                <i class="fas ${icon}" style="font-size: 24px;"></i>
                            </div>
                            <div>
                                <h3 style="margin: 0; color: #1a202c; font-size: 20px;">${data.label}</h3>
                                <p style="margin: 5px 0 0 0; color: #718096; font-size: 14px;">${data.files.length} file(s) available</p>
                            </div>
                            <button onclick="downloadAllCategory('${category}')" style="margin-left: auto; padding: 10px 20px; background: ${color}; color: white; border: none; border-radius: 6px; cursor: pointer; font-size: 14px; display: flex; align-items: center; gap: 8px;">
                                <i class="fas fa-download"></i> Download All
                            </button>
                        </div>
                        
                        <div style="overflow-x: auto;">
                            <table style="width: 100%; border-collapse: collapse;">
                                <thead>
                                    <tr style="background: #f7fafc;">
                                        <th style="padding: 12px; text-align: left; border-bottom: 2px solid #e2e8f0; font-weight: 600; color: #2d3748;">Filename</th>
                                        <th style="padding: 12px; text-align: left; border-bottom: 2px solid #e2e8f0; font-weight: 600; color: #2d3748;">Size</th>
                                        <th style="padding: 12px; text-align: left; border-bottom: 2px solid #e2e8f0; font-weight: 600; color: #2d3748;">Last Modified</th>
                                        <th style="padding: 12px; text-align: center; border-bottom: 2px solid #e2e8f0; font-weight: 600; color: #2d3748;">Actions</th>
                                    </tr>
                                </thead>
                                <tbody>
                `;
                
                data.files.forEach(file => {
                    const modDate = new Date(file.modified * 1000).toLocaleString();
                    html += `
                        <tr style="border-bottom: 1px solid #e2e8f0;">
                            <td style="padding: 12px;">
                                <div style="display: flex; align-items: center; gap: 8px;">
                                    <i class="fas fa-file-csv" style="color: #48bb78;"></i>
                                    <span style="font-weight: 500;">${file.name}</span>
                                </div>
                            </td>
                            <td style="padding: 12px; color: #718096;">${file.size_mb} MB</td>
                            <td style="padding: 12px; color: #718096; font-size: 13px;">${modDate}</td>
                            <td style="padding: 12px; text-align: center;">
                                <button onclick="previewFile('${category}', '${file.name}')" style="padding: 6px 12px; background: #667eea; color: white; border: none; border-radius: 4px; cursor: pointer; margin-right: 8px; font-size: 13px;">
                                    <i class="fas fa-eye"></i> Preview
                                </button>
                                <button onclick="downloadFile('${category}', '${file.name}')" style="padding: 6px 12px; background: #48bb78; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 13px;">
                                    <i class="fas fa-download"></i> Download
                                </button>
                            </td>
                        </tr>
                    `;
                });
                
                html += `
                                </tbody>
                            </table>
                        </div>
                    </div>
                `;
            }
            
            if (html === '') {
                html = `
                    <div style="text-align: center; padding: 60px; color: #718096;">
                        <i class="fas fa-folder-open" style="font-size: 48px; margin-bottom: 15px; opacity: 0.5;"></i>
                        <p style="font-size: 18px; margin: 0;">No files available for download</p>
                        <p style="font-size: 14px; margin-top: 10px;">Run the ETL pipeline to generate data files</p>
                    </div>
                `;
            }
            
            container.innerHTML = html;
        }
    } catch (error) {
        console.error('Error loading downloads:', error);
        document.getElementById('downloadsContainer').innerHTML = `
            <div style="text-align: center; padding: 40px; color: #e53e3e;">
                <i class="fas fa-exclamation-triangle" style="font-size: 32px; margin-bottom: 15px;"></i>
                <p>Failed to load downloads</p>
            </div>
        `;
    } finally {
        showLoading(false);
    }
}

async function previewFile(category, filename) {
    showLoading(true);
    try {
        console.log(`Previewing file: ${category}/${filename}`);
        const response = await fetchAPI(`/downloads/preview/${category}/${filename}`);
        
        console.log('Preview response:', response);
        
        if (response.success && response.data) {
            const data = response.data;
            const modal = document.getElementById('previewModal');
            const title = document.getElementById('previewTitle');
            const content = document.getElementById('previewContent');
            
            if (!modal || !title || !content) {
                console.error('Modal elements not found');
                alert('Preview modal not found in page');
                return;
            }
            
            title.textContent = `Preview: ${filename}`;
            
            let html = `
                <div style="margin-bottom: 20px; padding: 15px; background: #f7fafc; border-radius: 8px;">
                    <strong>Total Rows:</strong> ${data.total_rows.toLocaleString()} | 
                    <strong>Total Columns:</strong> ${data.total_columns} | 
                    <strong>Showing:</strong> First 50 rows
                </div>
                
                <div style="overflow-x: auto; max-height: 500px; overflow-y: auto;">
                    <table style="width: 100%; border-collapse: collapse; font-size: 13px;">
                        <thead style="position: sticky; top: 0; background: #2d3748; color: white;">
                            <tr>
            `;
            
            data.columns.forEach(col => {
                html += `<th style="padding: 12px; text-align: left; white-space: nowrap;">${col}</th>`;
            });
            
            html += `
                            </tr>
                        </thead>
                        <tbody>
            `;
            
            data.rows.forEach((row, idx) => {
                html += `<tr style="border-bottom: 1px solid #e2e8f0; ${idx % 2 === 0 ? 'background: #f7fafc;' : ''}">`;
                data.columns.forEach(col => {
                    const value = row[col] !== null && row[col] !== undefined ? row[col] : '';
                    html += `<td style="padding: 10px; white-space: nowrap;">${value}</td>`;
                });
                html += `</tr>`;
            });
            
            html += `
                        </tbody>
                    </table>
                </div>
            `;
            
            content.innerHTML = html;
            modal.style.display = 'block';
        } else {
            alert(`Failed to preview file: ${response.error || 'Unknown error'}`);
        }
    } catch (error) {
        console.error('Error previewing file:', error);
        alert(`Failed to preview file: ${error.message}`);
    } finally {
        showLoading(false);
    }
}

function closePreview() {
    const modal = document.getElementById('previewModal');
    if (modal) {
        modal.style.display = 'none';
    }
}

// Close modal when clicking outside the content
document.addEventListener('click', function(event) {
    const modal = document.getElementById('previewModal');
    if (modal && event.target === modal) {
        closePreview();
    }
});

function downloadFile(category, filename) {
    window.location.href = `/api/downloads/file/${category}/${filename}`;
}

async function downloadAllCategory(category) {
    const confirmed = confirm(`Download all files from ${category.toUpperCase()} layer?`);
    if (confirmed) {
        try {
            const response = await fetchAPI('/downloads/list');
            if (response.success && response.data[category]) {
                response.data[category].files.forEach(file => {
                    setTimeout(() => {
                        downloadFile(category, file.name);
                    }, 500);
                });
            }
        } catch (error) {
            console.error('Error downloading all files:', error);
        }
    }
}

// ============================================================================
// Initialize Dashboard
// ============================================================================

document.addEventListener('DOMContentLoaded', function() {
    // Load overview data on startup
    loadOverviewData();
});
