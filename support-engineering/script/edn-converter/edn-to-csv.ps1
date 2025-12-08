
    function ConvertFrom-EDN {
        <#
        .SYNOPSIS
            Converts EDN (Extensible Data Notation) string to JSON or CSV format.
        
        .DESCRIPTION
            This function takes an EDN string and converts it to either JSON or CSV format
            using the jet command-line tool.
        
        .PARAMETER EdnString
            The EDN string to convert.
        
        .PARAMETER OutputFormat
            The desired output format. Valid values are 'json' or 'csv'. Default is 'json'.
        
        .PARAMETER OutputPath
            Optional file path to save the output. If not specified, output is written to stdout.
        
        .EXAMPLE
            ConvertFrom-EDN -EdnString $myEdn -OutputFormat json
        
        .EXAMPLE
            ConvertFrom-EDN -EdnString $myEdn -OutputFormat csv -OutputPath "output.csv"
        #>
        [CmdletBinding()]
        param(
            [Parameter(Mandatory = $true, ValueFromPipeline = $true)]
            [string]$EdnString,
            
            [Parameter(Mandatory = $false)]
            [ValidateSet('json', 'csv')]
            [string]$OutputFormat = 'json',
            
            [Parameter(Mandatory = $false)]
            [string]$OutputPath
        )
        
        process {
            try {
                # Convert EDN to JSON first
                $jsonOutput = $EdnString | jet --to json
                
                if ($LASTEXITCODE -ne 0) {
                    throw "jet command failed with exit code $LASTEXITCODE"
                }
                
                if ($OutputFormat -eq 'json') {
                    $result = $jsonOutput
                }
                elseif ($OutputFormat -eq 'csv') {
                    # Convert JSON to CSV
                    $jsonObject = $jsonOutput | ConvertFrom-Json
                    $result = $jsonObject | ConvertTo-Csv -NoTypeInformation
                }
                
                # Output to file or stdout
                if ($OutputPath) {
                    $result | Out-File -FilePath $OutputPath -Encoding utf8
                    Write-Host "Output saved to: $OutputPath"
                }
                else {
                    return $result
                }
            }
            catch {
                Write-Error "Error converting EDN: $_"
                throw
            }
        }
    }

# Export the function if this script is dot-sourced
Export-ModuleMember -Function ConvertFrom-EDN