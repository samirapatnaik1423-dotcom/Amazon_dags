# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 7: Simple API Test Script (PowerShell)
# Tasks: T0033-T0037
# ═══════════════════════════════════════════════════════════════════════

$API_BASE_URL = "http://localhost:8000"
$API_KEY = "dev-key-12345"
$headers = @{
    "X-API-Key" = $API_KEY
    "Content-Type" = "application/json"
}

Write-Host "`n╔==========================================================╗" -ForegroundColor Cyan
Write-Host "║          TEAM 1 - SPRINT 7 API TESTS (PowerShell)      ║" -ForegroundColor Cyan
Write-Host "╚==========================================================╝" -ForegroundColor Cyan
Write-Host ""
Write-Host "API Base URL: $API_BASE_URL" -ForegroundColor Yellow
Write-Host "API Key: $API_KEY" -ForegroundColor Yellow
Write-Host ""

# Test 1: Health Check (No Auth Required)
Write-Host "`n============================================================" -ForegroundColor Green
Write-Host "TEST 1: Health Check Endpoint (Public)" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
try {
    $response = Invoke-RestMethod -Uri "$API_BASE_URL/health" -Method Get
    Write-Host "✅ SUCCESS" -ForegroundColor Green
    Write-Host "Response:" -ForegroundColor Cyan
    $response | ConvertTo-Json -Depth 3
} catch {
    Write-Host "❌ FAILED: $_" -ForegroundColor Red
}

# Test 2: Root Endpoint
Write-Host "`n============================================================" -ForegroundColor Green
Write-Host "TEST 2: Root Endpoint (Public)" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
try {
    $response = Invoke-RestMethod -Uri "$API_BASE_URL/" -Method Get
    Write-Host "✅ SUCCESS" -ForegroundColor Green
    Write-Host "Response:" -ForegroundColor Cyan
    $response | ConvertTo-Json -Depth 3
} catch {
    Write-Host "❌ FAILED: $_" -ForegroundColor Red
}

# Test 3: DAGs List (With Auth)
Write-Host "`n============================================================" -ForegroundColor Green
Write-Host "TEST 3: List DAGs (Authenticated)" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
try {
    $response = Invoke-RestMethod -Uri "$API_BASE_URL/api/v1/dags" -Method Get -Headers $headers
    Write-Host "✅ SUCCESS" -ForegroundColor Green
    Write-Host "Total DAGs: $($response.total)" -ForegroundColor Cyan
    Write-Host "DAGs Retrieved: $($response.items.Count)" -ForegroundColor Cyan
    if ($response.items.Count -gt 0) {
        Write-Host "`nFirst DAG:" -ForegroundColor Yellow
        $response.items[0] | ConvertTo-Json -Depth 2
    }
} catch {
    Write-Host "❌ FAILED: $_" -ForegroundColor Red
}

# Test 4: Metadata Summary (With Auth)
Write-Host "`n============================================================" -ForegroundColor Green
Write-Host "TEST 4: Metadata Summary (Authenticated)" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
try {
    $response = Invoke-RestMethod -Uri "$API_BASE_URL/api/v1/metadata/summary" -Method Get -Headers $headers
    Write-Host "✅ SUCCESS" -ForegroundColor Green
    Write-Host "Response:" -ForegroundColor Cyan
    $response | ConvertTo-Json -Depth 3
} catch {
    Write-Host "❌ FAILED: $_" -ForegroundColor Red
}

# Test 5: Authentication Test (Should Fail without API Key)
Write-Host "`n============================================================" -ForegroundColor Green
Write-Host "TEST 5: Authentication Test (No API Key - Should Fail)" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
try {
    $response = Invoke-RestMethod -Uri "$API_BASE_URL/api/v1/dags" -Method Get
    Write-Host "❌ UNEXPECTED: Request succeeded without API key!" -ForegroundColor Red
} catch {
    if ($_.Exception.Response.StatusCode -eq 403) {
        Write-Host "✅ SUCCESS: Authentication properly enforced (403 Forbidden)" -ForegroundColor Green
    } else {
        Write-Host "❌ FAILED: Unexpected error: $_" -ForegroundColor Red
    }
}

# Test 6: Test with wrong API Key
Write-Host "`n============================================================" -ForegroundColor Green
Write-Host "TEST 6: Wrong API Key Test (Should Fail)" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
try {
    $wrong_headers = @{
        "X-API-Key" = "wrong-key"
        "Content-Type" = "application/json"
    }
    $response = Invoke-RestMethod -Uri "$API_BASE_URL/api/v1/dags" -Method Get -Headers $wrong_headers
    Write-Host "❌ UNEXPECTED: Request succeeded with wrong API key!" -ForegroundColor Red
} catch {
    if ($_.Exception.Response.StatusCode -eq 403) {
        Write-Host "✅ SUCCESS: Invalid API key rejected (403 Forbidden)" -ForegroundColor Green
    } else {
        Write-Host "❌ FAILED: Unexpected error: $_" -ForegroundColor Red
    }
}

Write-Host "`n╔==========================================================╗" -ForegroundColor Cyan
Write-Host "║                   TESTS COMPLETE                        ║" -ForegroundColor Cyan
Write-Host "╚==========================================================╝" -ForegroundColor Cyan
Write-Host ""
Write-Host "📚 For more tests, visit: http://localhost:8000/docs" -ForegroundColor Yellow
Write-Host ""
