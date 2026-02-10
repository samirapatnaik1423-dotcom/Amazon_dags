# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 7: API Test Script
# Tasks: T0033-T0037
# ═══════════════════════════════════════════════════════════════════════

"""
test_api.py - Simple API Test Script

Test the REST API endpoints to verify Sprint 7 implementation.

Usage:
    python scripts/api/test_api.py
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import httpx
from typing import Dict, Any


API_BASE_URL = "http://localhost:8000"
API_KEY = "dev-key-12345"

def test_health():
    """Test health check endpoint"""
    print("\n" + "="*60)
    print("TEST: Health Check")
    print("="*60)
    
    response = httpx.get(f"{API_BASE_URL}/health")
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
    return response.status_code == 200


def test_root():
    """Test root endpoint"""
    print("\n" + "="*60)
    print("TEST: Root Endpoint")
    print("="*60)
    
    response = httpx.get(f"{API_BASE_URL}/")
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
    return response.status_code == 200


def test_list_dags():
    """Test list DAGs endpoint (T0034)"""
    print("\n" + "="*60)
    print("TEST: List DAGs (T0034)")
    print("="*60)
    
    headers = {"X-API-Key": API_KEY}
    response = httpx.get(f"{API_BASE_URL}/api/v1/dags", headers=headers)
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        dags = response.json()
        print(f"Total DAGs: {len(dags)}")
        if dags:
            print(f"First DAG: {dags[0]['dag_id']}")
        return True
    else:
        print(f"Error: {response.text}")
        return False


def test_metadata_summary():
    """Test metadata summary endpoint (T0035)"""
    print("\n" + "="*60)
    print("TEST: Metadata Summary (T0035)")
    print("="*60)
    
    headers = {"X-API-Key": API_KEY}
    response = httpx.get(f"{API_BASE_URL}/api/v1/metadata/summary", headers=headers)
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        metadata = response.json()
        print(f"Pipeline: {metadata['pipeline_name']}")
        print(f"Total DAGs: {metadata['total_dags']}")
        print(f"Tables: {len(metadata['tables'])}")
        return True
    else:
        print(f"Error: {response.text}")
        return False


def test_dag_status():
    """Test DAG status endpoint (T0034)"""
    print("\n" + "="*60)
    print("TEST: DAG Status (T0034)")
    print("="*60)
    
    headers = {"X-API-Key": API_KEY}
    dag_id = "etl_customers"  # Known DAG from project
    
    response = httpx.get(f"{API_BASE_URL}/api/v1/dags/{dag_id}/status", headers=headers)
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        status = response.json()
        print(f"DAG ID: {status['dag_info']['dag_id']}")
        print(f"Is Paused: {status['dag_info']['is_paused']}")
        print(f"Total Runs: {status['total_runs']}")
        print(f"Success Count: {status['success_count']}")
        return True
    else:
        print(f"Error: {response.text}")
        return False


def test_pagination():
    """Test pagination (T0037)"""
    print("\n" + "="*60)
    print("TEST: Pagination (T0037)")
    print("="*60)
    
    headers = {"X-API-Key": API_KEY}
    dag_id = "etl_customers"
    
    response = httpx.get(
        f"{API_BASE_URL}/api/v1/dags/{dag_id}/runs",
        headers=headers,
        params={"page": 1, "page_size": 10}
    )
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"Page: {data['page']}")
        print(f"Page Size: {data['page_size']}")
        print(f"Total: {data['total']}")
        print(f"Has Next: {data['has_next']}")
        return True
    else:
        print(f"Error: {response.text}")
        return False


def test_authentication():
    """Test API key authentication"""
    print("\n" + "="*60)
    print("TEST: Authentication")
    print("="*60)
    
    # Test without API key (should fail)
    response = httpx.get(f"{API_BASE_URL}/api/v1/dags")
    print(f"Without API Key - Status Code: {response.status_code}")
    auth_required = response.status_code == 401
    
    # Test with invalid API key (should fail)
    headers = {"X-API-Key": "invalid-key"}
    response = httpx.get(f"{API_BASE_URL}/api/v1/dags", headers=headers)
    print(f"Invalid API Key - Status Code: {response.status_code}")
    invalid_rejected = response.status_code == 403
    
    # Test with valid API key (should succeed)
    headers = {"X-API-Key": API_KEY}
    response = httpx.get(f"{API_BASE_URL}/api/v1/dags", headers=headers)
    print(f"Valid API Key - Status Code: {response.status_code}")
    valid_accepted = response.status_code == 200
    
    return auth_required and invalid_rejected and valid_accepted


def main():
    """Run all tests"""
    print("╔" + "="*58 + "╗")
    print("║" + " "*10 + "TEAM 1 - SPRINT 7 API TESTS" + " "*20 + "║")
    print("╚" + "="*58 + "╝")
    
    print(f"\nAPI Base URL: {API_BASE_URL}")
    print(f"API Key: {API_KEY}")
    
    # Check if server is running
    try:
        httpx.get(f"{API_BASE_URL}/health", timeout=2.0)
    except Exception as e:
        print("\n❌ ERROR: API server is not running!")
        print(f"   Please start the server first:")
        print(f"   uvicorn scripts.api.main:app --reload --host 0.0.0.0 --port 8000")
        return
    
    # Run tests
    tests = [
        ("Health Check", test_health),
        ("Root Endpoint", test_root),
        ("Authentication", test_authentication),
        ("List DAGs (T0034)", test_list_dags),
        ("DAG Status (T0034)", test_dag_status),
        ("Metadata Summary (T0035)", test_metadata_summary),
        ("Pagination (T0037)", test_pagination),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ Test failed with exception: {e}")
            results.append((test_name, False))
    
    # Print summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {test_name}")
    
    print("\n" + "="*60)
    print(f"Results: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
    print("="*60)
    
    if passed == total:
        print("\n🎉 All tests passed! Sprint 7 implementation is complete!")
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Please review the errors above.")


if __name__ == "__main__":
    main()
