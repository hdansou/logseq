# Save as QuickCapture.ps1
$text = Get-Clipboard
if ($text) {
    Start-Process "logseq://x-callback-url/quickCapture?content=$([System.Uri]::EscapeDataString($text))&page=quick%20dash%20capture"
    Write-Host "✓ Captured to Logseq" -ForegroundColor Green
} else {
    Write-Host "✗ Clipboard is empty" -ForegroundColor Red
}