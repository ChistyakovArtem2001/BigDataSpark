param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("transform", "reports", "both")]
    [string]$Job = "both"
)


Write-Host "Запуск Spark джобов"


if ($Job -eq "transform" -or $Job -eq "both") {
    Write-Host "`n1. Запуск трансформации данных в модель звезда..."
    Write-Host "..."
    docker-compose exec spark-master bash -c "export JAVA_HOME=/usr/lib/jvm/default-java && export SPARK_HOME=/opt/spark && export PYSPARK_PYTHON=python3 && export PYSPARK_DRIVER_PYTHON=python3 && python /opt/spark/apps/01_transform_to_star_schema.py"
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Ошибка при выполнении трансформации"
        exit 1
    }
}

if ($Job -eq "reports" -or $Job -eq "both") {
    Write-Host "`n2. Запуск создания отчетов в ClickHouse..."
    Write-Host "..."
    docker-compose exec spark-master bash -c "export JAVA_HOME=/usr/lib/jvm/default-java && export SPARK_HOME=/opt/spark && export PYSPARK_PYTHON=python3 && export PYSPARK_DRIVER_PYTHON=python3 && python /opt/spark/apps/02_create_reports_clickhouse.py"
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Ошибка при создании отчетов"
        exit 1
    }
}

Write-Host "`n=========================================="
Write-Host "Готово"
Write-Host "=========================================="

