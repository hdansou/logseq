<#
.SYNOPSIS
    Sends content to Logseq using the Quick Capture URL scheme.

.DESCRIPTION
    This function allows you to quickly capture content to Logseq from PowerShell.
    You can capture text, URLs, clipboard content, and specify which page to save to.
    
    The function supports:
    - Direct text input or pipeline input
    - Clipboard content capture
    - Custom page targeting (including journal pages)
    - URL and title metadata
    - Append mode (add to end of page)

.PARAMETER Content
    The text content to capture. Can be provided directly or via pipeline.
    Optional if using -UseClipboard.

.PARAMETER Page
    The name of the page to capture to. 
    Special values:
    - "today" = today's journal page
    - Default (if omitted) = "quick capture" page
    Examples: "quick dash capture", "inbox", "meeting notes"

.PARAMETER Url
    Optional URL to attach to the capture (e.g., source article link).

.PARAMETER Title
    Optional title for the URL (works with -Url parameter).

.PARAMETER Append
    If specified, adds content to the end of the page.
    Default behavior is to add to the beginning.

.PARAMETER UseClipboard
    Capture content from clipboard instead of -Content parameter.

.EXAMPLE
    Invoke-LogseqQuickCapture -Content "Remember to review this"
    
    Captures text to the default "quick capture" page.

.EXAMPLE
    Invoke-LogseqQuickCapture -Content "Dashboard item" -Page "quick dash capture"
    
    Captures text to a specific page named "quick dash capture".

.EXAMPLE
    Invoke-LogseqQuickCapture -Content "Today's note" -Page "today"
    
    Captures text to today's journal page.

.EXAMPLE
    Invoke-LogseqQuickCapture -UseClipboard
    
    Captures whatever is currently in your clipboard.

.EXAMPLE
    "Important note" | Invoke-LogseqQuickCapture -Page "inbox"
    
    Uses pipeline input to capture text to the "inbox" page.

.EXAMPLE
    Invoke-LogseqQuickCapture -Url "https://example.com" -Title "Example Site" -Content "Interesting article"
    
    Captures a URL with title and additional notes.

.NOTES
    Author: GitHub Copilot
    Requires: Logseq desktop application installed and running
    The logseq:// URL scheme must be registered on your system.
#>
function Invoke-LogseqQuickCapture {
    [CmdletBinding()]
    param(
        [Parameter(ValueFromPipeline = $true, Position = 0)]
        [string]$Content,
        
        [Parameter(Position = 1)]
        [string]$Page,
        
        [string]$Url,
        [string]$Title,
        [switch]$Append,   
        [switch]$UseClipboard
    )

    # Get content from clipboard if requested
    if ($UseClipboard) {
        $Content = Get-Clipboard -Raw
        if ([string]::IsNullOrWhiteSpace($Content)) {
            Write-Error "Clipboard is empty"
            return
        }
    }
    
    # Build the URL parameters
    $params = [System.Collections.ArrayList]@()
    
    if (![string]::IsNullOrWhiteSpace($Content)) {
        $encodedContent = [System.Uri]::EscapeDataString($Content)
        [void]$params.Add("content=$encodedContent")
    }
    
    if (![string]::IsNullOrWhiteSpace($Url)) {
        $encodedUrl = [System.Uri]::EscapeDataString($Url)
        [void]$params.Add("url=$encodedUrl")
    }
    
    if (![string]::IsNullOrWhiteSpace($Title)) {
        $encodedTitle = [System.Uri]::EscapeDataString($Title)
        [void]$params.Add("title=$encodedTitle")
    }
    
    if (![string]::IsNullOrWhiteSpace($Page)) {
        $encodedPage = [System.Uri]::EscapeDataString($Page)
        [void]$params.Add("page=$encodedPage")
    }
    
    if ($Append) {
        [void]$params.Add("append=true")
    }
    
    # Construct the full URL
    $queryString = $params -join "&"
    $logseqUrl = "logseq://x-callback-url/quickCapture?$queryString"
    
    # Output the URL for debugging
    Write-Verbose "Opening URL: $logseqUrl"
    
    # Open the URL (this will launch/activate Logseq)
    Start-Process $logseqUrl
    
    Write-Host "✓ Sent to Logseq: $($Content.Substring(0, [Math]::Min(50, $Content.Length)))..." -ForegroundColor Green
}

# ============================================
# USAGE EXAMPLES
# ============================================

<#
# Beginner Examples
# -----------------

# 1. Simplest usage - capture a quick note
Invoke-LogseqQuickCapture -Content "Remember to review this"

# 2. Capture to your "quick dash capture" page
Invoke-LogseqQuickCapture -Content "Dashboard item" -Page "quick dash capture"

# 3. Capture to today's journal
Invoke-LogseqQuickCapture -Content "Today's task" -Page "TODAY"

# 4. Capture what's in your clipboard
Invoke-LogseqQuickCapture -UseClipboard -Page "TODAY"


# Intermediate Examples
# ---------------------

# 5. Capture from command output
Get-Date | Invoke-LogseqQuickCapture -Page "quick dash capture"

# 6. Capture a URL with description
Invoke-LogseqQuickCapture -Url "https://github.com" -Title "GitHub" -Content "Check this repo"

# 7. Add to the bottom of a page
Invoke-LogseqQuickCapture -Content "Add to bottom" -Page "quick dash capture" -Append


# Advanced Examples
# -----------------

# 8. Multi-line capture using here-string
@"
- Task 1
- Task 2
- Task 3
"@ | Invoke-LogseqQuickCapture -Page "quick dash capture"

# 9. Capture meeting notes with timestamp
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm"
Invoke-LogseqQuickCapture -Content "Meeting notes - $timestamp" -Page "meetings"

# 10. Quick alias for daily use
Set-Alias -Name lq -Value Invoke-LogseqQuickCapture
lq -Content "Quick note using alias" -Page "today"

#>
