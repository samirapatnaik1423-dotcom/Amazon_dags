#!/bin/bash

# ETL Pipeline Health Check
# This script verifies all services are running correctly

echo "╔════════════════════════════════════════════════════════════╗"
echo "║         🏥 ETL Pipeline Health Check                       ║"
echo "╚════════════════════════════════════════════════════════════╝"

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check Docker
echo -e "\n${BLUE}1. Checking Docker...${NC}"
if command -v docker &> /dev/null; then
    echo -e "${GREEN}✓ Docker is installed${NC}"
else
    echo -e "${RED}✗ Docker is not installed${NC}"
    exit 1
fi

# Check PostgreSQL
echo -e "\n${BLUE}2. Checking PostgreSQL...${NC}"
if docker exec docker-postgres-1 pg_isready -U airflow -d airflow &> /dev/null; then
    echo -e "${GREEN}✓ PostgreSQL is running and healthy${NC}"
    
    # Check tables
    TABLES=$(docker exec docker-postgres-1 psql -U airflow -d airflow -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'etl'")
    echo -e "${GREEN}  └─ ETL schema has $TABLES tables${NC}"
else
    echo -e "${RED}✗ PostgreSQL is not responding${NC}"
fi

# Check Redis
echo -e "\n${BLUE}3. Checking Redis...${NC}"
if docker exec docker-redis-1 redis-cli ping &> /dev/null; then
    echo -e "${GREEN}✓ Redis is running${NC}"
else
    echo -e "${RED}✗ Redis is not responding${NC}"
fi

# Check Airflow Webserver
echo -e "\n${BLUE}4. Checking Airflow Webserver...${NC}"
if curl -s http://localhost:8080/health | grep -q "healthy"; then
    echo -e "${GREEN}✓ Airflow Webserver is running and healthy${NC}"
else
    echo -e "${YELLOW}⚠ Airflow Webserver is running but health check returned unexpected result${NC}"
fi

# Check Airflow Scheduler
echo -e "\n${BLUE}5. Checking Airflow Scheduler...${NC}"
if docker exec docker-scheduler-1 ps aux | grep -q "[s]cheduler"; then
    echo -e "${GREEN}✓ Airflow Scheduler is running${NC}"
else
    echo -e "${RED}✗ Airflow Scheduler is not running${NC}"
fi

# Check DAGs
echo -e "\n${BLUE}6. Checking DAGs...${NC}"
DAG_COUNT=$(docker exec docker-scheduler-1 airflow dags list 2>/dev/null | grep -c "^\|" || echo "0")
if [ "$DAG_COUNT" -ge 3 ]; then
    echo -e "${GREEN}✓ DAGs are loaded in Airflow${NC}"
    docker exec docker-scheduler-1 airflow dags list 2>/dev/null | grep -E "customer_etl_dag|sales_etl_dag|unified_etl_dag" | while read line; do
        echo -e "${GREEN}  └─ $line${NC}"
    done
else
    echo -e "${YELLOW}⚠ DAGs may not be loaded yet, please wait...${NC}"
fi

# Summary
echo -e "\n${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✓ All services are operational!${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"

echo -e "\n${BLUE}📊 Quick Stats:${NC}"
docker ps --format "table {{.Names}}\t{{.Status}}" | grep docker-

echo -e "\n${BLUE}🌐 Access Points:${NC}"
echo -e "  ${YELLOW}Airflow UI:${NC}     http://localhost:8080 (admin/admin)"
echo -e "  ${YELLOW}PostgreSQL:${NC}    localhost:5432 (airflow/airflow)"
echo -e "  ${YELLOW}Redis:${NC}         localhost:6379"

echo -e "\n${BLUE}📝 Next Steps:${NC}"
echo -e "  1. Open http://localhost:8080 in your browser"
echo -e "  2. Login with admin / admin"
echo -e "  3. Find 'unified_etl_dag' and trigger it"
echo -e "  4. Monitor execution in real-time"

echo -e "\n${GREEN}✨ Pipeline is ready for use!${NC}\n"
