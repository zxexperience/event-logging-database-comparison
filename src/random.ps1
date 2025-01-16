# Function to generate a random string of specified length
function Get-RandomString($length) {
    $chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    $string = -join ((65..90) + (97..122) | Get-Random -Count $length | ForEach-Object { [char]$_ })
    return $string
}

# Function to generate a random timestamp
function Get-RandomTimestamp() {
    $start = Get-Date "2025-01-01"
    $end = Get-Date "2025-12-31"
    $range = $end - $start
    $randomTimeSpan = New-TimeSpan -Seconds (Get-Random -Minimum 0 -Maximum $range.TotalSeconds)
    $randomDate = $start + $randomTimeSpan
    return $randomDate.ToString("yyyy-MM-dd HH:mm:ss")
}

# Generate data
$data = @()
for ($i = 1; $i -le 100000; $i++) {
    $entry = @{
        timestamp = Get-RandomTimestamp
        message = Get-RandomString -length (Get-Random -Minimum 20 -Maximum 150)
        severity_ID = Get-Random -Minimum 1 -Maximum 3
        event_type_ID = Get-Random -Minimum 1 -Maximum 6
        source_ID = Get-Random -Minimum 1 -Maximum 150
    }
    $data += $entry
}

# Convert data to JSON and save to file
$json = $data | ConvertTo-Json -Depth 3
$json | Out-File -FilePath "data_100000.json" -Encoding utf8

Write-Output "JSON file 'data.json' has been created."
