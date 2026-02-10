"""Test Data Quality Dashboard endpoints"""
import requests
import json

API_BASE = "http://localhost:8000"
HEADERS = {"X-API-Key": "dev-key-12345"}

def test_endpoint(name, url):
    """Test an endpoint and report results"""
    print(f"\n{'='*80}")
    print(f"Testing: {name}")
    print(f"URL: {url}")
    print('='*80)
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=5)
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"Response keys: {list(data.keys())}")
            
            # Check structure
            if 'scorecards' in data:
                print(f"  - Scorecards count: {len(data.get('scorecards', []))}")
                if data.get('scorecards'):
                    print(f"  - First scorecard keys: {list(data['scorecards'][0].keys())}")
            
            if 'summary' in data:
                print(f"  - Summary: {data['summary']}")
            
            if 'results' in data:
                print(f"  - Results count: {len(data.get('results', []))}")
            
            if 'anomalies' in data:
                print(f"  - Anomalies count: {len(data.get('anomalies', []))}")
            
            print(f"✅ SUCCESS")
            return True, data
        else:
            print(f"❌ FAILED: Status {response.status_code}")
            print(f"Response: {response.text[:200]}")
            return False, None
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False, None

# Test all quality endpoints
print("\n" + "="*80)
print("DATA QUALITY DASHBOARD ENDPOINT ANALYSIS")
print("="*80)

results = {}

# 1. Quality Summary
results['summary'] = test_endpoint(
    "Quality Summary",
    f"{API_BASE}/quality/summary"
)

# 2. Quality Scorecard (all tables)
results['scorecard_all'] = test_endpoint(
    "Quality Scorecard (All Tables)",
    f"{API_BASE}/quality/scorecard"
)

# 3. Quality Scorecard (specific table)
results['scorecard_specific'] = test_endpoint(
    "Quality Scorecard (Customers Table)",
    f"{API_BASE}/quality/scorecard/customers"
)

# 4. Validation Results
results['validation'] = test_endpoint(
    "Validation Results",
    f"{API_BASE}/quality/validation-results"
)

# 5. Anomalies
results['anomalies'] = test_endpoint(
    "Anomalies",
    f"{API_BASE}/quality/anomalies?limit=10"
)

# 6. Data Profiles
results['profiles'] = test_endpoint(
    "Data Profiles",
    f"{API_BASE}/quality/profiles"
)

# Summary
print("\n" + "="*80)
print("SUMMARY OF TESTS")
print("="*80)

for name, (success, data) in results.items():
    status = "✅ PASS" if success else "❌ FAIL"
    print(f"{status} - {name}")

print("\n" + "="*80)
print("FLASK PROXY COMPATIBILITY CHECK")
print("="*80)
print("\nChecking if Flask wrapper adds the expected structure...")

# Test through Flask proxy
flask_url = "http://localhost:5000/api/quality/scorecard"
print(f"Testing Flask proxy: {flask_url}")
try:
    response = requests.get(flask_url, timeout=5)
    if response.status_code == 200:
        flask_data = response.json()
        print(f"Flask response keys: {list(flask_data.keys())}")
        print(f"Has 'success' key: {'success' in flask_data}")
        print(f"Has 'data' key: {'data' in flask_data}")
        
        if 'data' in flask_data:
            print(f"Data type: {type(flask_data['data'])}")
            if isinstance(flask_data['data'], dict):
                print(f"Data keys: {list(flask_data['data'].keys())}")
except Exception as e:
    print(f"Error testing Flask: {e}")

print("\n" + "="*80)
print("RECOMMENDATIONS")
print("="*80)

# Check scorecard data structure
success, scorecard_data = results.get('scorecard_all', (False, None))
if success and scorecard_data:
    if 'scorecards' in scorecard_data:
        print("✅ Scorecard endpoint returns 'scorecards' array")
        print("   JavaScript expects: data.data as array")
        print("   Need to access: data.data.scorecards")
    else:
        print("⚠️  Scorecard structure mismatch")
