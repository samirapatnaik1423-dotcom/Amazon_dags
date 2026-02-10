# ══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 7: Quick API Test using curl
# ══════════════════════════════════════════════════════════════════════

Write-Host "`n╔══════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║       SPRINT 7 API Testing Guide                        ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Instructions
Write-Host "📋 To test the REST API, follow these steps:" -ForegroundColor Yellow
Write-Host ""
Write-Host "1️⃣  Start the API Server:" -ForegroundColor Green
Write-Host "   cd d:\sam\Projects\Infosys\Airflow" -ForegroundColor White
Write-Host '   $env:POSTGRES_HOST="localhost"; $env:POSTGRES_PORT="5434"' -ForegroundColor White  
Write-Host '   $env:POSTGRES_DB="airflow"; $env:POSTGRES_USER="airflow"' -ForegroundColor White
Write-Host '   $env:POSTGRES_PASSWORD="airflow"; $env:ETL_SCHEMA="etl_output"' -ForegroundColor White
Write-Host '   $env:API_KEY_ENABLED="true"; $env:API_KEYS="dev-key-12345"' -ForegroundColor White
Write-Host '   uvicorn scripts.api.main:app --reload --host 0.0.0.0 --port 8000' -ForegroundColor White
Write-Host ""

Write-Host "2️⃣  Open a NEW terminal and run these test commands:" -ForegroundColor Green
Write-Host ""

# Test commands
$tests = @(
    @{
        Name = "Health Check (Public)"
        Command = 'curl http://localhost:8000/health'
    },
    @{
        Name = "Root Endpoint (Public)"
        Command = 'curl http://localhost:8000/'
    },
    @{
        Name = "List DAGs (Authenticated)"
        Command = 'curl -H "X-API-Key: dev-key-12345" http://localhost:8000/api/v1/dags'
    },
    @{
        Name = "Get DAG Status"
        Command = 'curl -H "X-API-Key: dev-key-12345" http://localhost:8000/api/v1/dags/etl_customers/status'
    },
    @{
        Name = "Metadata Summary"
        Command = 'curl -H "X-API-Key: dev-key-12345" http://localhost:8000/api/v1/metadata/summary'
    },
    @{
        Name = "Test Authentication (Should Fail - 403)"
        Command = 'curl http://localhost:8000/api/v1/dags'
    }
)

foreach ($test in $tests) {
    Write-Host "  📝 $($test.Name):" -ForegroundColor Cyan
    Write-Host "     $($test.Command)" -ForegroundColor White
    Write-Host ""
}

Write-Host "3️⃣  Access Interactive API Documentation:" -ForegroundColor Green
Write-Host "   🌐 Swagger UI: http://localhost:8000/docs" -ForegroundColor White
Write-Host "   🌐 ReDoc:      http://localhost:8000/redoc" -ForegroundColor White
Write-Host ""

Write-Host "4️⃣  Using PowerShell (Invoke-RestMethod):" -ForegroundColor Green
Write-Host ""
Write-Host "   # Health Check" -ForegroundColor Yellow
Write-Host '   Invoke-RestMethod -Uri "http://localhost:8000/health"' -ForegroundColor White
Write-Host ""
Write-Host "   # List DAGs (with authentication)" -ForegroundColor Yellow  
Write-Host '   $headers = @{"X-API-Key" = "dev-key-12345"}' -ForegroundColor White
Write-Host '   Invoke-RestMethod -Uri "http://localhost:8000/api/v1/dags" -Headers $headers' -ForegroundColor White
Write-Host ""

Write-Host "══════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

# Show current status
Write-Host "📊 Current API Server Status:" -ForegroundColor Yellow
try {
    $response = Invoke-WebRequest -Uri "http://localhost:8000/health" -TimeoutSec 2 -ErrorAction Stop
    Write-Host "✅ API Server is RUNNING on port 8000" -ForegroundColor Green
    Write-Host "   Status: $($response.StatusCode)" -ForegroundColor Green
} catch {
    Write-Host "❌ API Server is NOT running" -ForegroundColor Red
    Write-Host "   Please start the server using the commands above" -ForegroundColor Yellow
}

Write-Host ""
