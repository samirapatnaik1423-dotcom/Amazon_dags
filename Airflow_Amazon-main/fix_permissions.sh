#!/bin/bash
# Script to fix permissions in Airflow Docker container

echo "=== Fixing Airflow Directory Permissions ==="

# Fix permissions for data directories
echo "Fixing /opt/airflow/data permissions..."
chmod -R 777 /opt/airflow/data/processed 2>/dev/null || echo "  ⚠ Could not modify /opt/airflow/data/processed (may need root)"
chmod -R 777 /opt/airflow/data/raw 2>/dev/null || echo "  ⚠ Could not modify /opt/airflow/data/raw (may need root)"
chmod -R 777 /opt/airflow/data/staging 2>/dev/null || echo "  ⚠ Could not modify /opt/airflow/data/staging (may need root)"

# Remove existing locked files
echo "Removing locked Excel files..."
rm -f /opt/airflow/data/processed/amazon_cleaned_data.xlsx 2>/dev/null && echo "  ✓ Removed amazon_cleaned_data.xlsx" || echo "  ℹ File not found or already removed"
rm -f /opt/airflow/data/processed/cleaned_data.xlsx 2>/dev/null && echo "  ✓ Removed cleaned_data.xlsx" || echo "  ℹ File not found or already removed"

echo ""
echo "=== Permission Fix Complete ==="
echo "Tip: Run this inside the Docker container with:"
echo "  docker exec -it <container-name> bash /opt/airflow/fix_permissions.sh"
