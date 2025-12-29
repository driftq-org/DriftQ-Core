$ErrorActionPreference = "Stop"

# ---- Config ----
$BaseUrl = $env:DRIFTQ_URL
if (-not $BaseUrl) { $BaseUrl = "http://localhost:8080" }
$BaseUrl = $BaseUrl.TrimEnd('/')

# Unique topic per run to avoid old backlog
$Topic      = "test-topic-" + (Get-Date -Format "yyyyMMdd-HHmmss")
$Partitions = 1

$Group    = "demo-group"
$Owner    = "demo-client"
$LeaseMs  = 5000
$TenantId = "demo"
$IdemKey  = "abc123"

function Get-HttpErrorBody($err) {
  try {
    $resp = $err.Exception.Response
    if ($null -eq $resp) { return $null }
    $stream = $resp.GetResponseStream()
    if ($null -eq $stream) { return $null }
    $reader = New-Object System.IO.StreamReader($stream)
    return $reader.ReadToEnd()
  } catch {
    return $null
  }
}

function Post-Json($url, $obj) {
  $json = $obj | ConvertTo-Json -Depth 30 -Compress
  try {
    return Invoke-RestMethod -Method Post -Uri $url -ContentType "application/json" -Body $json
  } catch {
    $body = $_.ErrorDetails.Message
    if (-not $body) { $body = Get-HttpErrorBody $_ }
    if ($body) { Write-Host "HTTP error body:`n$body" -ForegroundColor Red }
    throw
  }
}

function Post-JsonNoContent($url, $obj) {
  $json = $obj | ConvertTo-Json -Depth 30 -Compress
  try {
    Invoke-RestMethod -Method Post -Uri $url -ContentType "application/json" -Body $json | Out-Null
  } catch {
    $body = $_.ErrorDetails.Message
    if (-not $body) { $body = Get-HttpErrorBody $_ }
    if ($body) { Write-Host "HTTP error body:`n$body" -ForegroundColor Red }
    throw
  }
}

function Parse-RetryAfterMs([string]$body) {
  $fallback = 500
  if ([string]::IsNullOrWhiteSpace($body)) { return $fallback }
  try {
    $e = $body | ConvertFrom-Json
    if ($e.retry_after_ms) { return [int]$e.retry_after_ms }
  } catch {}
  return $fallback
}

function Post-JsonWithRetry($url, $obj, $maxAttempts = 20) {
  for ($i=1; $i -le $maxAttempts; $i++) {
    try {
      return Post-Json $url $obj
    } catch {
      $body = $_.ErrorDetails.Message
      if (-not $body) { $body = Get-HttpErrorBody $_ }

      if ($body -and $body -match "RESOURCE_EXHAUSTED") {
        $retryMs = Parse-RetryAfterMs $body
        Write-Host "Producer overloaded (attempt $i/$maxAttempts). Sleeping ${retryMs}ms then retrying..." -ForegroundColor Yellow
        Start-Sleep -Milliseconds $retryMs
        continue
      }
      throw
    }
  }
  throw "Request failed after $maxAttempts attempts (still overloaded)."
}

function Consume-OneNdjsonLine($url, $timeoutSeconds = 15) {
  # curl streams forever; Select-Object -First 1 closes it after the first line.
  # --max-time prevents hanging if no messages arrive.
  $line = (curl.exe -sN --max-time $timeoutSeconds $url | Select-Object -First 1)
  return $line
}

function ConsumeAndAck-One() {
  Write-Host "`nConsuming ONE message..."
  $consumeUrl = "$BaseUrl/v1/consume?topic=$Topic&group=$Group&owner=$Owner&lease_ms=$LeaseMs"

  $line = Consume-OneNdjsonLine $consumeUrl 15
  if (-not $line) { throw "No message received (consume timed out)." }

  Write-Host $line
  $msg = $line | ConvertFrom-Json

  if ($null -eq $msg.partition -or $null -eq $msg.offset) {
    throw "Consume output missing partition/offset. Got: $line"
  }

  Write-Host "Acking partition=$($msg.partition) offset=$($msg.offset)..."
  Post-JsonNoContent "$BaseUrl/v1/ack" @{
    topic     = $Topic
    group     = $Group
    owner     = $Owner
    partition = [int]$msg.partition
    offset    = [int]$msg.offset
  }
  Write-Host "Acked."
}

# -------------------- Demo --------------------

Write-Host "Using DriftQ at: $BaseUrl"
Write-Host "Topic: $Topic"

Write-Host "`n1) Creating topic..."
Post-Json "$BaseUrl/v1/topics" @{ name = $Topic; partitions = $Partitions } | ConvertTo-Json -Compress | Write-Host

Write-Host "`n2) Produce normal message..."
Post-JsonWithRetry "$BaseUrl/v1/produce" @{
  topic    = $Topic
  value    = "Hello DriftQ!"
  envelope = @{ tenant_id = $TenantId }
} | ConvertTo-Json -Compress | Write-Host

Write-Host "`n3) Consume+Ack one..."
ConsumeAndAck-One

Write-Host "`n4) Producer idempotency demo (same key twice)..."
$idemBody = @{
  topic    = $Topic
  value    = "Hello again (idempotent)"
  envelope = @{
    tenant_id       = $TenantId
    idempotency_key = $IdemKey
  }
}

Post-JsonWithRetry "$BaseUrl/v1/produce" $idemBody | ConvertTo-Json -Compress | Write-Host
Post-JsonWithRetry "$BaseUrl/v1/produce" $idemBody | ConvertTo-Json -Compress | Write-Host

Write-Host "`n5) Consume+Ack one..."
ConsumeAndAck-One

Write-Host "`nDemo complete."
