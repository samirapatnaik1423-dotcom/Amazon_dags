"""
Phase 1 Test Runner
Comprehensive testing of all Phase 1 ingestion DAGs
"""

import os
import sys
import time
import requests
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
AIRFLOW_URL = "http://localhost:8080"
API_URL = "http://localhost:8000"
AIRFLOW_AUTH = ("airflow", "airflow")

PHASE1_DAGS = [
    "etl_json_ingestion",
    "etl_sql_ingestion",
    "etl_api_ingestion",
    "etl_file_watcher"
]


def check_airflow_available():
    """Check if Airflow webserver is running"""
    try:
        response = requests.get(f"{AIRFLOW_URL}/health", timeout=5)
        if response.status_code == 200:
            logger.info("✅ Airflow webserver is running")
            return True
        else:
            logger.error(f"❌ Airflow returned status {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Airflow not accessible: {e}")
        return False


def check_api_available():
    """Check if FastAPI is running"""
    try:
        response = requests.get(f"{API_URL}/health", timeout=5)
        if response.status_code == 200:
            logger.info("✅ FastAPI service is running")
            return True
        else:
            logger.warning(f"⚠️ FastAPI returned status {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        logger.warning(f"⚠️ FastAPI not accessible: {e}")
        return False


def get_dag_status(dag_id):
    """Get DAG status from Airflow"""
    try:
        response = requests.get(
            f"{AIRFLOW_URL}/api/v1/dags/{dag_id}",
            auth=AIRFLOW_AUTH,
            timeout=10
        )
        
        if response.status_code == 200:
            dag_info = response.json()
            return {
                "exists": True,
                "is_paused": dag_info.get("is_paused", True),
                "is_active": dag_info.get("is_active", False),
                "file_path": dag_info.get("fileloc", "Unknown")
            }
        else:
            return {"exists": False}
    except Exception as e:
        logger.error(f"Error checking DAG {dag_id}: {e}")
        return {"exists": False, "error": str(e)}


def trigger_dag(dag_id, conf=None):
    """Trigger a DAG run"""
    try:
        payload = {}
        if conf:
            payload["conf"] = conf
        
        response = requests.post(
            f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns",
            auth=AIRFLOW_AUTH,
            json=payload,
            timeout=10
        )
        
        if response.status_code in [200, 201]:
            dag_run = response.json()
            logger.info(f"✅ Triggered {dag_id} - Run ID: {dag_run.get('dag_run_id')}")
            return True, dag_run.get('dag_run_id')
        else:
            logger.error(f"❌ Failed to trigger {dag_id}: {response.text}")
            return False, None
    except Exception as e:
        logger.error(f"❌ Error triggering {dag_id}: {e}")
        return False, None


def check_dag_run_status(dag_id, dag_run_id, timeout=300):
    """Monitor DAG run until completion or timeout"""
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            response = requests.get(
                f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}",
                auth=AIRFLOW_AUTH,
                timeout=10
            )
            
            if response.status_code == 200:
                dag_run = response.json()
                state = dag_run.get("state")
                
                if state == "success":
                    logger.info(f"✅ {dag_id} completed successfully")
                    return True
                elif state == "failed":
                    logger.error(f"❌ {dag_id} failed")
                    return False
                elif state in ["running", "queued"]:
                    logger.info(f"🔄 {dag_id} is {state}...")
                    time.sleep(5)
                else:
                    logger.warning(f"⚠️ {dag_id} has state: {state}")
                    time.sleep(5)
            else:
                logger.error(f"Failed to get status: {response.text}")
                return False
        except Exception as e:
            logger.error(f"Error checking status: {e}")
            time.sleep(5)
    
    logger.warning(f"⏱️ Timeout waiting for {dag_id}")
    return False


def test_api_endpoints():
    """Test Phase 1 API endpoints"""
    logger.info("\n🧪 Testing API Endpoints...")
    
    endpoints = [
        ("GET", "/api/v1/ingest/file-watcher/status"),
        ("GET", "/api/v1/ingest/history?limit=10"),
    ]
    
    results = []
    
    for method, endpoint in endpoints:
        try:
            if method == "GET":
                response = requests.get(f"{API_URL}{endpoint}", timeout=10)
            
            if response.status_code == 200:
                logger.info(f"✅ {method} {endpoint} - OK")
                results.append(True)
            else:
                logger.warning(f"⚠️ {method} {endpoint} - Status {response.status_code}")
                results.append(False)
        except Exception as e:
            logger.error(f"❌ {method} {endpoint} - Error: {e}")
            results.append(False)
    
    return all(results)


def run_phase1_tests():
    """Run comprehensive Phase 1 tests"""
    
    print("\n" + "="*70)
    print("PHASE 1 - MULTI-FORMAT INGESTION TEST SUITE")
    print("="*70 + "\n")
    
    # Step 1: Check services
    logger.info("🔍 Step 1: Checking services availability...")
    
    airflow_ok = check_airflow_available()
    api_ok = check_api_available()
    
    if not airflow_ok:
        logger.error("❌ Airflow is not available. Exiting.")
        return False
    
    # Step 2: Verify DAGs exist
    logger.info("\n🔍 Step 2: Verifying Phase 1 DAGs...")
    
    dags_status = {}
    for dag_id in PHASE1_DAGS:
        status = get_dag_status(dag_id)
        dags_status[dag_id] = status
        
        if status.get("exists"):
            logger.info(f"✅ {dag_id} - Found")
            if status.get("is_paused"):
                logger.warning(f"   ⚠️ DAG is paused - unpause it in Airflow UI")
        else:
            logger.error(f"❌ {dag_id} - NOT FOUND")
    
    existing_dags = [dag for dag, status in dags_status.items() if status.get("exists")]
    
    if not existing_dags:
        logger.error("❌ No Phase 1 DAGs found. Check DAG files.")
        return False
    
    # Step 3: Test API endpoints
    if api_ok:
        logger.info("\n🔍 Step 3: Testing API endpoints...")
        api_test_result = test_api_endpoints()
    else:
        logger.warning("\n⚠️ Skipping API tests (service not available)")
    
    # Step 4: Trigger DAGs
    logger.info("\n🔍 Step 4: Triggering Phase 1 DAGs...")
    
    trigger_results = {}
    
    for dag_id in existing_dags:
        status = dags_status[dag_id]
        
        if status.get("is_paused"):
            logger.warning(f"⏸️ Skipping {dag_id} (paused)")
            trigger_results[dag_id] = "skipped"
            continue
        
        logger.info(f"\n🚀 Triggering {dag_id}...")
        success, run_id = trigger_dag(dag_id)
        
        if success:
            trigger_results[dag_id] = "triggered"
            logger.info(f"   Waiting for completion...")
            
            # Monitor execution (with timeout)
            completion_status = check_dag_run_status(dag_id, run_id, timeout=180)
            
            if completion_status:
                trigger_results[dag_id] = "success"
            else:
                trigger_results[dag_id] = "failed"
        else:
            trigger_results[dag_id] = "trigger_failed"
    
    # Step 5: Results summary
    print("\n" + "="*70)
    print("TEST RESULTS SUMMARY")
    print("="*70 + "\n")
    
    logger.info("📊 Services:")
    logger.info(f"   Airflow: {'✅ OK' if airflow_ok else '❌ DOWN'}")
    logger.info(f"   FastAPI: {'✅ OK' if api_ok else '⚠️ DOWN'}")
    
    logger.info("\n📊 DAGs Status:")
    for dag_id, status in dags_status.items():
        if status.get("exists"):
            result = trigger_results.get(dag_id, "not_triggered")
            
            if result == "success":
                icon = "✅"
            elif result == "skipped":
                icon = "⏸️"
            elif result == "failed":
                icon = "❌"
            else:
                icon = "⚠️"
            
            logger.info(f"   {icon} {dag_id}: {result}")
        else:
            logger.info(f"   ❌ {dag_id}: NOT FOUND")
    
    # Final verdict
    success_count = sum(1 for r in trigger_results.values() if r == "success")
    total_count = len(existing_dags)
    
    print("\n" + "="*70)
    if success_count == total_count:
        print("✅ ALL TESTS PASSED")
    elif success_count > 0:
        print(f"⚠️ PARTIAL SUCCESS - {success_count}/{total_count} DAGs passed")
    else:
        print("❌ ALL TESTS FAILED")
    print("="*70 + "\n")
    
    return success_count > 0


if __name__ == "__main__":
    success = run_phase1_tests()
    sys.exit(0 if success else 1)
