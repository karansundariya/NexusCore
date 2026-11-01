# =============================================================================
# test_e2e.ps1 — NexusCore End-to-End Test Suite for Windows PowerShell
# =============================================================================
$ErrorActionPreference = "Continue"

$GATEWAY_URL = if ($env:GATEWAY_URL) { $env:GATEWAY_URL } else { "http://localhost:8080" }
$API_KEY = "nexuscore-dev-key-12345"
$HEADERS = @{
    "Authorization" = "Bearer $API_KEY"
    "Content-Type"  = "application/json"
}

Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "          NexusCore E2E Test Suite (PowerShell)       " -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "Target Gateway: $GATEWAY_URL"
Write-Host "Timestamp:      $(Get-Date)"
Write-Host ""

# Pre-flight Check
try {
    $health = Invoke-RestMethod -Uri "$GATEWAY_URL/v1/health" -Method Get -TimeoutSec 5 -ErrorAction Stop
    Write-Host "[OK] Gateway is reachable at $GATEWAY_URL" -ForegroundColor Green
} catch {
    Write-Host "[ERROR] Gateway is not reachable at $GATEWAY_URL." -ForegroundColor Red
    Write-Host "Please ensure Gateway is running: cd gateway-service && mvn spring-boot:run" -ForegroundColor Yellow
    exit 1
}

$passed = 0
$failed = 0

# TEST 1: Cold Cache Miss
Write-Host ""
Write-Host "------------------------------------------------------" -ForegroundColor DarkGray
Write-Host "TEST 1: Cold Cache Miss" -ForegroundColor Yellow
Write-Host "------------------------------------------------------" -ForegroundColor DarkGray
$uniquePrompt = "How do I cancel my subscription e2e test $(Get-Random)"
$body = @{
    model = "gpt-4o"
    messages = @(
        @{ role = "user"; content = $uniquePrompt }
    )
} | ConvertTo-Json

$sw = [System.Diagnostics.Stopwatch]::StartNew()
try {
    $resp = Invoke-WebRequest -Uri "$GATEWAY_URL/v1/chat/completions" -Method Post -Headers $HEADERS -Body $body -TimeoutSec 60 -UseBasicParsing
    $sw.Stop()
    $statusCode = $resp.StatusCode
    $cacheStatus = $resp.Headers["X-Cache-Status"]
    if (-not $cacheStatus) {
        $json = $resp.Content | ConvertFrom-Json
        $cacheStatus = $json.x_cache_status
    }
    Write-Host "  HTTP Status:  $statusCode" -ForegroundColor White
    Write-Host "  Latency:      $($sw.ElapsedMilliseconds) ms" -ForegroundColor White
    Write-Host "  Cache Status: $cacheStatus" -ForegroundColor White

    if ($statusCode -eq 200 -and $cacheStatus -eq "MISS") {
        Write-Host "  [PASS] Test 1: Cold Cache Miss" -ForegroundColor Green
        $passed++
    } else {
        Write-Host "  [FAIL] Test 1: Expected 200 & MISS, got $statusCode & $cacheStatus" -ForegroundColor Red
        $failed++
    }
} catch {
    $sw.Stop()
    Write-Host "  [FAIL] Test 1 request failed: $_" -ForegroundColor Red
    $failed++
}

Write-Host "Waiting 2s for background cache population..." -ForegroundColor Gray
Start-Sleep -Seconds 2

# TEST 2: Semantic Cache Hit
Write-Host ""
Write-Host "------------------------------------------------------" -ForegroundColor DarkGray
Write-Host "TEST 2: Semantic Cache Hit" -ForegroundColor Yellow
Write-Host "------------------------------------------------------" -ForegroundColor DarkGray
$sw = [System.Diagnostics.Stopwatch]::StartNew()
try {
    $resp2 = Invoke-WebRequest -Uri "$GATEWAY_URL/v1/chat/completions" -Method Post -Headers $HEADERS -Body $body -TimeoutSec 60 -UseBasicParsing
    $sw.Stop()
    $statusCode2 = $resp2.StatusCode
    $cacheStatus2 = $resp2.Headers["X-Cache-Status"]
    if (-not $cacheStatus2) {
        $json2 = $resp2.Content | ConvertFrom-Json
        $cacheStatus2 = $json2.x_cache_status
    }
    Write-Host "  HTTP Status:  $statusCode2" -ForegroundColor White
    Write-Host "  Latency:      $($sw.ElapsedMilliseconds) ms (<50ms expected)" -ForegroundColor White
    Write-Host "  Cache Status: $cacheStatus2" -ForegroundColor White

    if ($statusCode2 -eq 200 -and $cacheStatus2 -eq "SEMANTIC_HIT") {
        Write-Host "  [PASS] Test 2: Semantic Cache Hit" -ForegroundColor Green
        $passed++
    } else {
        Write-Host "  [FAIL] Test 2: Expected 200 & SEMANTIC_HIT, got $statusCode2 & $cacheStatus2" -ForegroundColor Red
        $failed++
    }
} catch {
    $sw.Stop()
    Write-Host "  [FAIL] Test 2 request failed: $_" -ForegroundColor Red
    $failed++
}

# TEST 3: Rate Limiting
Write-Host ""
Write-Host "------------------------------------------------------" -ForegroundColor DarkGray
Write-Host "TEST 3: Distributed Rate Limiting (60 req/min limit)" -ForegroundColor Yellow
Write-Host "------------------------------------------------------" -ForegroundColor DarkGray
Write-Host "Firing rapid burst of 70 requests..." -ForegroundColor Gray

$succCount = 0
$rateLimitCount = 0

for ($i = 1; $i -le 70; $i++) {
    $burstBody = @{
        model = "gpt-4o"
        messages = @(
            @{ role = "user"; content = "Burst request $i" }
        )
    } | ConvertTo-Json

    try {
        $r = Invoke-WebRequest -Uri "$GATEWAY_URL/v1/chat/completions" -Method Post -Headers $HEADERS -Body $burstBody -TimeoutSec 10 -UseBasicParsing -ErrorAction Stop
        if ($r.StatusCode -eq 200) { $succCount++ }
    } catch {
        $code = 0
        if ($_.Exception -and $_.Exception.Response) {
            $code = [int]$_.Exception.Response.StatusCode
        }
        if ($code -eq 429) {
            $rateLimitCount++
        }
    }
}

Write-Host "  Successful (200):    $succCount" -ForegroundColor White
Write-Host "  Rate Limited (429):  $rateLimitCount" -ForegroundColor White

if ($rateLimitCount -gt 0) {
    Write-Host "  [PASS] Test 3: Rate Limiting enforced successfully (HTTP 429 received)" -ForegroundColor Green
    $passed++
} else {
    Write-Host "  [FAIL] Test 3: No HTTP 429 received in 70 requests" -ForegroundColor Red
    $failed++
}

Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "Summary: $passed Passed, $failed Failed" -ForegroundColor $(if ($failed -eq 0) { "Green" } else { "Red" })
Write-Host "======================================================" -ForegroundColor Cyan
