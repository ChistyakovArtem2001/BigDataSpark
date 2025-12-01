Param(
    [string]$OutputDir = ".\spark-jars"
)

if (-not (Test-Path $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir | Out-Null
}

$artifacts = @(
    @{
        Name = "postgresql-42.7.1.jar"
        Url  = "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar"
    },
    @{
        Name = "clickhouse-jdbc-0.6.0-all.jar"
        Url  = "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.0/clickhouse-jdbc-0.6.0-all.jar"
    }
)

foreach ($artifact in $artifacts) {
    $dest = Join-Path $OutputDir $artifact.Name
    if (Test-Path $dest) {
        Write-Host "$($artifact.Name) уже скачан"
        continue
    }
    Write-Host "Скачивание $($artifact.Name)..."
    Invoke-WebRequest -Uri $artifact.Url -OutFile $dest
}

Write-Host "Готово - $OutputDir"

