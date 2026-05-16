# check_aftermarket_log.ps1 — KiwoomAftermarketSync 실행 결과를 Telegram으로 전송
# 작업 스케줄러에서 실행: 2026-05-13 16:10 (1회성)

$scriptDir = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$logFile   = Join-Path $scriptDir "logs\aftermarket_sync.log"
$envFile   = Join-Path $scriptDir ".env"

# .env 로드
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
            [System.Environment]::SetEnvironmentVariable($Matches[1].Trim(), $Matches[2].Trim(), 'Process')
        }
    }
}

$token  = $env:TELEGRAM_TOKEN
$chatId = $env:TELEGRAM_CHAT_ID

function Send-TelegramMessage($text) {
    $url  = "https://api.telegram.org/bot$token/sendMessage"
    $body = @{ chat_id = $chatId; text = $text; parse_mode = "HTML" } | ConvertTo-Json -Compress
    try {
        Invoke-RestMethod -Uri $url -Method Post -Body $body -ContentType "application/json" | Out-Null
    } catch {
        Write-Warning "Telegram 전송 실패: $_"
    }
}

# 로그 파일 확인
if (-not (Test-Path $logFile)) {
    $msg = "⚠️ <b>KiwoomAftermarketSync</b>`n로그 파일 없음: $logFile`n`n"

    # 작업 스케줄러 상태 조회
    try {
        $taskInfo = schtasks /Query /TN KiwoomAftermarketSync /FO LIST 2>&1
        $msg += "<b>Task Scheduler 상태:</b>`n<pre>$($taskInfo -join "`n")</pre>"
    } catch {
        $msg += "작업 스케줄러 조회 실패: $_"
    }

    Send-TelegramMessage $msg
    exit 0
}

$content = Get-Content $logFile -Encoding UTF8 -ErrorAction SilentlyContinue
if (-not $content -or $content.Count -eq 0) {
    $msg = "⚠️ <b>KiwoomAftermarketSync</b>`n로그 파일이 비어있습니다.`n`n"
    try {
        $taskInfo = schtasks /Query /TN KiwoomAftermarketSync /FO LIST 2>&1
        $msg += "<b>Task Scheduler 상태:</b>`n<pre>$($taskInfo -join "`n")</pre>"
    } catch {
        $msg += "작업 스케줄러 조회 실패: $_"
    }
    Send-TelegramMessage $msg
    exit 0
}

# 마지막 50줄 발췌 (Telegram 4096자 제한 고려)
$tail = ($content | Select-Object -Last 50) -join "`n"
if ($tail.Length -gt 3500) {
    $tail = $tail.Substring($tail.Length - 3500)
}

$lastModified = (Get-Item $logFile).LastWriteTime.ToString("yyyy-MM-dd HH:mm:ss")
$lineCount    = $content.Count

$msg = "📋 <b>KiwoomAftermarketSync 로그</b>`n최종 수정: $lastModified / 총 $lineCount 줄`n`n<pre>$tail</pre>"
Send-TelegramMessage $msg
