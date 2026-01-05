# PowerShell script to fix method calls

# Get all Java files in command directory
$files = Get-ChildItem -Path "src\main\java\com\mini\redis\command" -Filter "*.java" -Recurse

foreach ($file in $files) {
    $content = Get-Content $file.FullName -Raw

    # Replace .elements() with .getElements()
    $content = $content -replace '\.elements\(\)', '.getElements()'

    # Replace .value() with .getStringValue() for BulkString
    $content = $content -replace '\)\.value\(\)', ').getStringValue()'

    Set-Content -Path $file.FullName -Value $content -NoNewline
}

Write-Host "Fixed method calls in all command files"