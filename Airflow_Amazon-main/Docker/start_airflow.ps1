# Airflow Docker Startup Script
# This script helps initialize and start Airflow in Docker

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Airflow ETL Pipeline - Docker Setup" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# Check if Docker is running
Write-Host "Checking Docker status..." -ForegroundColor Yellow
try {
    $dockerVersion = docker --version 2>&1
    Write-Host "✓ Docker is installed: $dockerVersion" -ForegroundColor Green
    
    # Check if Docker daemon is running
    docker ps > $null 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "✗ Docker is not running! Please start Docker Desktop." -ForegroundColor Red
        Write-Host "  After starting Docker Desktop, run this script again." -ForegroundColor Yellow
        exit 1
    }
    Write-Host "✓ Docker daemon is running" -ForegroundColor Green
} catch {
    Write-Host "✗ Docker is not installed!" -ForegroundColor Red
    Write-Host "  Please install Docker Desktop from: https://www.docker.com/products/docker-desktop" -ForegroundColor Yellow
    exit 1
}

# Check current directory
$currentDir = Get-Location
if ($currentDir.Path -notlike "*Docker*") {
    Write-Host "`nNavigating to Docker directory..." -ForegroundColor Yellow
    if (Test-Path "Docker") {
        Set-Location Docker
        Write-Host "✓ Changed to Docker directory" -ForegroundColor Green
    } else {
        Write-Host "✗ Docker directory not found!" -ForegroundColor Red
        Write-Host "  Please run this script from the project root directory." -ForegroundColor Yellow
        exit 1
    }
}

# Check if .env file exists
if (!(Test-Path ".env")) {
    Write-Host "`n✗ .env file not found!" -ForegroundColor Red
    Write-Host "  Creating default .env file..." -ForegroundColor Yellow
    
    $envContent = @"
# UID/GID used to chown files created by init_dirs
AIRFLOW_UID=50000
AIRFLOW_GID=50000

# Postgres credentials
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Fernet key (for encryption)
AIRFLOW__CORE__FERNET_KEY=lZIYHPYioP9lMFymp6vDwi_SArOuwh2gD3R8rD8FB0o=
"@
    
    $envContent | Out-File -FilePath ".env" -Encoding UTF8
    Write-Host "✓ Created .env file with default values" -ForegroundColor Green
}

# Menu
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "What would you like to do?" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

Write-Host "1. Initialize Airflow (first time setup)" -ForegroundColor White
Write-Host "2. Start Airflow services" -ForegroundColor White
Write-Host "3. Stop Airflow services" -ForegroundColor White
Write-Host "4. View service status" -ForegroundColor White
Write-Host "5. View logs" -ForegroundColor White
Write-Host "6. Restart services" -ForegroundColor White
Write-Host "7. Clean shutdown (remove all data)" -ForegroundColor White
Write-Host "8. Test DAG" -ForegroundColor White
Write-Host "9. Exit" -ForegroundColor White

$choice = Read-Host "`nEnter your choice (1-9)"

switch ($choice) {
    "1" {
        Write-Host "`n========================================" -ForegroundColor Cyan
        Write-Host "Initializing Airflow" -ForegroundColor Cyan
        Write-Host "========================================`n" -ForegroundColor Cyan
        
        Write-Host "Creating directories and initializing database..." -ForegroundColor Yellow
        Write-Host "This may take a few minutes on first run...`n" -ForegroundColor Yellow
        
        # Run init_dirs first
        docker-compose up init_dirs
        
        Write-Host "`n✓ Initialization complete!" -ForegroundColor Green
        Write-Host "`nNext step: Choose option 2 to start Airflow services" -ForegroundColor Yellow
    }
    
    "2" {
        Write-Host "`n========================================" -ForegroundColor Cyan
        Write-Host "Starting Airflow Services" -ForegroundColor Cyan
        Write-Host "========================================`n" -ForegroundColor Cyan
        
        Write-Host "Starting all services in background..." -ForegroundColor Yellow
        docker-compose up -d
        
        Write-Host "`n✓ Services started!" -ForegroundColor Green
        Write-Host "`nAirflow UI will be available at: http://localhost:8080" -ForegroundColor Cyan
        Write-Host "Username: airflow" -ForegroundColor Gray
        Write-Host "Password: airflow" -ForegroundColor Gray
        Write-Host "`nWait ~30 seconds for services to be ready..." -ForegroundColor Yellow
        
        Start-Sleep -Seconds 5
        Write-Host "`nChecking service status..." -ForegroundColor Yellow
        docker-compose ps
    }
    
    "3" {
        Write-Host "`n========================================" -ForegroundColor Cyan
        Write-Host "Stopping Airflow Services" -ForegroundColor Cyan
        Write-Host "========================================`n" -ForegroundColor Cyan
        
        Write-Host "Stopping all services..." -ForegroundColor Yellow
        docker-compose down
        
        Write-Host "`n✓ All services stopped!" -ForegroundColor Green
    }
    
    "4" {
        Write-Host "`n========================================" -ForegroundColor Cyan
        Write-Host "Service Status" -ForegroundColor Cyan
        Write-Host "========================================`n" -ForegroundColor Cyan
        
        docker-compose ps
        
        Write-Host "`n" -ForegroundColor Cyan
        Write-Host "Running containers:" -ForegroundColor Yellow
        docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    }
    
    "5" {
        Write-Host "`n========================================" -ForegroundColor Cyan
        Write-Host "View Logs" -ForegroundColor Cyan
        Write-Host "========================================`n" -ForegroundColor Cyan
        
        Write-Host "Which service logs would you like to view?" -ForegroundColor Yellow
        Write-Host "1. Webserver" -ForegroundColor White
        Write-Host "2. Scheduler" -ForegroundColor White
        Write-Host "3. All services" -ForegroundColor White
        
        $logChoice = Read-Host "`nEnter choice (1-3)"
        
        Write-Host "`nPress Ctrl+C to stop viewing logs`n" -ForegroundColor Yellow
        Start-Sleep -Seconds 2
        
        switch ($logChoice) {
            "1" { docker-compose logs -f webserver }
            "2" { docker-compose logs -f scheduler }
            "3" { docker-compose logs -f }
            default { Write-Host "Invalid choice" -ForegroundColor Red }
        }
    }
    
    "6" {
        Write-Host "`n========================================" -ForegroundColor Cyan
        Write-Host "Restart Services" -ForegroundColor Cyan
        Write-Host "========================================`n" -ForegroundColor Cyan
        
        Write-Host "Restarting all services..." -ForegroundColor Yellow
        docker-compose restart
        
        Write-Host "`n✓ Services restarted!" -ForegroundColor Green
    }
    
    "7" {
        Write-Host "`n========================================" -ForegroundColor Cyan
        Write-Host "Clean Shutdown" -ForegroundColor Cyan
        Write-Host "========================================`n" -ForegroundColor Cyan
        
        Write-Host "⚠ WARNING: This will remove all containers and volumes!" -ForegroundColor Red
        Write-Host "  Your DAGs and scripts in the project folder will be safe." -ForegroundColor Yellow
        Write-Host "  But all Airflow metadata and database will be deleted." -ForegroundColor Yellow
        
        $confirm = Read-Host "`nAre you sure? (yes/no)"
        
        if ($confirm -eq "yes") {
            Write-Host "`nRemoving all containers and volumes..." -ForegroundColor Yellow
            docker-compose down -v
            Write-Host "`n✓ Clean shutdown complete!" -ForegroundColor Green
            Write-Host "  Run option 1 to initialize again." -ForegroundColor Yellow
        } else {
            Write-Host "`nCancelled." -ForegroundColor Yellow
        }
    }
    
    "8" {
        Write-Host "`n========================================" -ForegroundColor Cyan
        Write-Host "Test DAG" -ForegroundColor Cyan
        Write-Host "========================================`n" -ForegroundColor Cyan
        
        Write-Host "Testing customer_etl DAG..." -ForegroundColor Yellow
        Write-Host "This will run the DAG without scheduling it`n" -ForegroundColor Gray
        
        docker-compose exec webserver airflow dags test customer_etl 2025-01-01
        
        Write-Host "`n✓ DAG test complete!" -ForegroundColor Green
        Write-Host "  Check the output above for any errors." -ForegroundColor Yellow
        Write-Host "  Processed data should be in: ..\data\processed\cleaned_data.csv" -ForegroundColor Cyan
    }
    
    "9" {
        Write-Host "`nExiting..." -ForegroundColor Yellow
        exit 0
    }
    
    default {
        Write-Host "`n✗ Invalid choice!" -ForegroundColor Red
    }
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Useful Commands:" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Start:        docker-compose up -d" -ForegroundColor Gray
Write-Host "Stop:         docker-compose down" -ForegroundColor Gray
Write-Host "Logs:         docker-compose logs -f" -ForegroundColor Gray
Write-Host "Status:       docker-compose ps" -ForegroundColor Gray
Write-Host "Web UI:       http://localhost:8080" -ForegroundColor Gray
Write-Host "========================================`n" -ForegroundColor Cyan
