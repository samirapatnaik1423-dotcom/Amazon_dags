#!/bin/bash
# Docker Compose Startup Script for Multi-Source ETL Pipeline
# Usage: bash start_airflow.sh

set -e

echo "=========================================="
echo "  Multi-Source ETL Pipeline - Docker"
echo "=========================================="
echo ""

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "📂 Project Root: $PROJECT_ROOT"
echo "📂 Docker Directory: $SCRIPT_DIR"
echo ""

# Check if .env exists
if [ ! -f "$SCRIPT_DIR/.env" ]; then
    echo "❌ Error: .env file not found in $SCRIPT_DIR"
    echo "Please create .env with required variables"
    exit 1
fi

echo "✅ .env file found"
echo ""

# Load environment variables
set -a
source "$SCRIPT_DIR/.env"
set +a

# Check Docker and Docker Compose
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed or not in PATH"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    if ! docker compose version &> /dev/null; then
        echo "❌ Docker Compose is not installed or not in PATH"
        exit 1
    fi
    DOCKER_COMPOSE="docker compose"
else
    DOCKER_COMPOSE="docker-compose"
fi

echo "✅ Docker: $(docker --version)"
echo "✅ Docker Compose: $($DOCKER_COMPOSE --version)"
echo ""

# Change to Docker directory
cd "$SCRIPT_DIR"

# Check for running containers
echo "Checking for existing containers..."
if [ "$($DOCKER_COMPOSE ps -q | wc -l)" -gt 0 ]; then
    echo "⚠️  Containers already running"
    echo ""
    echo "Options:"
    echo "  1. Continue (restart existing containers)"
    echo "  2. Stop and remove (clean restart)"
    echo "  3. Exit"
    read -p "Select option (1-3): " option
    
    case $option in
        1)
            echo "Restarting containers..."
            $DOCKER_COMPOSE restart
            ;;
        2)
            echo "Stopping and removing containers..."
            $DOCKER_COMPOSE down
            echo "Starting fresh..."
            $DOCKER_COMPOSE up -d
            ;;
        3)
            echo "Exiting..."
            exit 0
            ;;
        *)
            echo "Invalid option"
            exit 1
            ;;
    esac
else
    echo "✅ No containers running"
    echo ""
    echo "🚀 Starting services..."
    $DOCKER_COMPOSE up -d
fi

echo ""
echo "=========================================="
echo "  Waiting for services to be healthy..."
echo "=========================================="
echo ""

# Wait for PostgreSQL
echo "Waiting for PostgreSQL..."
for i in {1..30}; do
    if $DOCKER_COMPOSE exec -T postgres pg_isready -U $POSTGRES_USER &> /dev/null; then
        echo "✅ PostgreSQL is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ PostgreSQL failed to start"
        exit 1
    fi
    echo "  Attempt $i/30..."
    sleep 2
done

# Wait for Airflow webserver
echo "Waiting for Airflow Webserver..."
for i in {1..30}; do
    if $DOCKER_COMPOSE exec -T webserver curl -f http://localhost:8080/health &> /dev/null; then
        echo "✅ Airflow Webserver is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ Airflow Webserver failed to start"
        exit 1
    fi
    echo "  Attempt $i/30..."
    sleep 2
done

echo ""
echo "=========================================="
echo "  ✅ All Services Started Successfully!"
echo "=========================================="
echo ""

# Display service information
echo "📊 Service Status:"
$DOCKER_COMPOSE ps
echo ""

echo "🌐 Access Information:"
echo "  Airflow UI:      http://localhost:8080"
echo "  Username:        admin"
echo "  Password:        admin"
echo ""
echo "  PostgreSQL:      localhost:5432"
echo "  Database:        $POSTGRES_DB"
echo "  User:            $POSTGRES_USER"
echo "  Password:        $POSTGRES_PASSWORD"
echo ""

echo "📝 Useful Commands:"
echo "  View logs:       $DOCKER_COMPOSE logs -f"
echo "  Stop services:   $DOCKER_COMPOSE stop"
echo "  Stop & remove:   $DOCKER_COMPOSE down -v"
echo "  Shell access:    $DOCKER_COMPOSE exec webserver bash"
echo "  DB connection:   $DOCKER_COMPOSE exec postgres psql -U $POSTGRES_USER -d $POSTGRES_DB"
echo ""

echo "🚀 Next Steps:"
echo "  1. Open http://localhost:8080 in your browser"
echo "  2. Login with admin/admin"
echo "  3. Trigger 'unified_etl_dag' from DAGs page"
echo "  4. Monitor execution in Airflow UI"
echo ""

echo "=========================================="
echo "  🎉 Setup Complete!"
echo "=========================================="
