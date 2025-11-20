#Logseq API
function Invoke-LogseqAPI {
    param (
        [Parameter(Mandatory = $true)]
        [string]$Method,
        
        [Parameter(Mandatory = $false)]
        [array]$Args = @()
    )

    $Token = $env:LogseqToken
    if (-not $Token) {
        throw "LogseqToken environment variable is not set"
    }

    $url = "http://127.0.0.1:12315/api"
    $headers = @{
        "Authorization" = "Bearer $Token"
        "Content-Type"  = "application/json"
    }
    $body = @{
        method = $Method
        args   = $Args
    } | ConvertTo-Json -Depth 10

    Invoke-RestMethod -Uri $url -Method Post -Headers $headers -Body $body
}

function Invoke-LogseqInsertBlock {
    param (
        [string]$Page,
        [string]$Content
    )

    Invoke-LogseqAPI -Method "logseq.Editor.insertBlock" -Args @($Page, $Content, @{ isPageBlock = $true })
}
function Invoke-LogseqSimpleQuery {
    param ()

    #args   = "(and [[ARC Enabled Services]] (tags [[Product Specification Document]] ))"
    #args   = @("[:find ?id :in $ ?name :where [?id :block/name ?name]]", "Test")
    Invoke-LogseqAPI -Method "logseq.db.datascriptQuery" -Args @("[:find (pull ?p [*]) :where [?p :db/ident :logseq.kv/schema-version]]")
}
function Get-LogseqSelectedBlocks {
    param ()

    Invoke-LogseqAPI -Method "logseq.Editor.GetSelectedBlocks"
}
function Get-LogseqCurrentBlock {
    param ()

    Invoke-LogseqAPI -Method "logseq.Editor.getCurrentBlock"
}
function Get-LogseqCurrentPage {
    param ()

    Invoke-LogseqAPI -Method "logseq.Editor.getCurrentPage"
}
function Get-LogseqBlock {
    param (
        [string]$BlockUUID
    )

    Invoke-LogseqAPI -Method "logseq.Editor.getBlock" -Args @($BlockUUID)
}
function Invoke-LogseqSearch {
    param ()
    
    Invoke-LogseqAPI -Method "logseq.api.search" -Args @("Test Page")
}
function Invoke-LogseqTextQuery {
    param ()
    
    Invoke-LogseqAPI -Method "logseq.api.q" -Args @("(task TODO)")
}
$env:LogseqToken = "7E071879-293C-4AF7-90F8-0A644DAEB815"

Invoke-LogseqInsertBlock -Page "Test page metro" -Content "This is a new block"
Invoke-LogseqSimpleQuery
Get-LogseqSelectedBlocks
Get-LogseqCurrentBlock
Get-LogseqCurrentPage
Get-LogseqBlock -BlockUUID "68fcd746-b47e-4e05-b762-1454076a77d0"
Invoke-LogseqSearch
Invoke-LogseqTextQuery