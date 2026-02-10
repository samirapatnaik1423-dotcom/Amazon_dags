# Start Airflow ETL Pipeline on Windows
# Run this script from PowerShell in the Docker/ directory

Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║   🚀 Starting Multi-Source ETL Pipeline in Docker          ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan

# Step 1: Verify Docker is running
Write-Host "`n📦 Checking Docker..." -ForegroundColor Yellow
try {
    $dockerVersion = docker --version
    Write-Host "✅ Docker found: $dockerVersion" -ForegroundColor Green
} catch {
    Write-Host "❌ Docker not found. Please install Docker Desktop." -ForegroundColor Red
    exit 1
}

# Step 2: Verify docker-compose is available
Write-Host "`n🐳 Checking docker-compose..." -ForegroundColor Yellow
try {
    $composeVersion = docker-compose --version
    Write-Host "✅ docker-compose found: $composeVersion" -ForegroundColor Green
} catch {
    Write-Host "❌ docker-compose not found." -ForegroundColor Red
    exit 1
}

# Step 3: Check if docker-compose.yaml exists
Write-Host "`n📄 Checking docker-compose.yaml..." -ForegroundColor Yellow
if (Test-Path "docker-compose.yaml") {
    Write-Host "✅ docker-compose.yaml found" -ForegroundColor Green
} else {
    Write-Host "❌ docker-compose.yaml not found in current directory" -ForegroundColor Red
    exit 1
}

# Step 4: Validate docker-compose.yaml
Write-Host "`n🔍 Validating docker-compose configuration..." -ForegroundColor Yellow
try {
    docker-compose config --quiet
    Write-Host "✅ Configuration is valid" -ForegroundColor Green
} catch {
    Write-Host "❌ Configuration error: $_" -ForegroundColor Red
    exit 1
}

# Step 5: Start services
Write-Host "`n🚀 Starting services..." -ForegroundColor Yellow
docker-compose up -d

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Failed to start services" -ForegroundColor Red
    exit 1
}

Write-Host "✅ Services started" -ForegroundColor Green

# Step 6: Wait for services to be ready
Write-Host "`n⏳ Waiting for services to be ready (30 seconds)..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

# Step 7: Check service status
Write-Host "`n📊 Checking service status..." -ForegroundColor Yellow
docker-compose ps

# Step 8: Wait more for Airflow to initialize
Write-Host "`n⏳ Waiting for Airflow webserver to be ready..." -ForegroundColor Yellow
$maxAttempts = 30
$attempt = 0

while ($attempt -lt $maxAttempts) {
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8080/health" -UseBasicParsing -TimeoutSec 2
        if ($response.StatusCode -eq 200) {
            Write-Host "✅ Airflow is ready!" -ForegroundColor Green
            break
        }
    } catch {
        # Service not ready yet
    }
    $attempt++
    Write-Host "." -NoNewline
    Start-Sleep -Seconds 1
}

if ($attempt -eq $maxAttempts) {
    Write-Host "`n⚠️ Airflow took longer than expected, but services should be running" -ForegroundColor Yellow
}

# Step 9: Display connection information
Write-Host "`n╔════════════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║               ✅ Pipeline is Running!                        ║" -ForegroundColor Green
Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Green

Write-Host "`n🌐 Access Airflow Web UI:" -ForegroundColor Cyan
Write-Host "   URL:      http://localhost:8080" -ForegroundColor White
Write-Host "   Username: admin" -ForegroundColor White
Write-Host "   Password: admin" -ForegroundColor White

Write-Host "`n📊 Services:" -ForegroundColor Cyan
Write-Host "   PostgreSQL:        localhost:5432" -ForegroundColor White
Write-Host "   Redis:             localhost:6379" -ForegroundColor White
Write-Host "   Airflow Webserver: http://localhost:8080" -ForegroundColor White

Write-Host "`n📝 Useful Commands:" -ForegroundColor Cyan
Write-Host "   View logs:         docker-compose logs -f" -ForegroundColor White
Write-Host "   Stop services:     docker-compose stop" -ForegroundColor White
Write-Host "   Restart services:  docker-compose restart" -ForegroundColor White
Write-Host "   Full reset:        docker-compose down -v" -ForegroundColor White

Write-Host "`n🎯 Next Steps:" -ForegroundColor Cyan
Write-Host "   1. Open http://localhost:8080 in your browser" -ForegroundColor White
Write-Host "   2. Login with admin / admin" -ForegroundColor White
Write-Host "   3. Find 'unified_etl_dag' and trigger it" -ForegroundColor White
Write-Host "   4. Watch the DAG execute in real-time" -ForegroundColor White
Write-Host "   5. Check logs to monitor progress" -ForegroundColor White

Write-Host "`n" -ForegroundColor Green
