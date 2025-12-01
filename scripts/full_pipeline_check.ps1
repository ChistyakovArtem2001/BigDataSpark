param(
    [switch]$RecreateClickHouseTables
)

Write-Host "======================" -ForegroundColor Cyan
Write-Host "Полная проверка ЛР2" -ForegroundColor Cyan
Write-Host "======================" -ForegroundColor Cyan
Write-Host ""

function Invoke-Step {
    param(
        [string]$Title,
        [ScriptBlock]$Action
    )
    Write-Host "---- $Title ----" -ForegroundColor Yellow
    try {
        & $Action
        if ($LASTEXITCODE -ne $null -and $LASTEXITCODE -ne 0) {
            throw "Команда завершилась с кодом $LASTEXITCODE"
        }
        Write-Host "OK: $Title" -ForegroundColor Green
        Write-Host ""
        return $true
    }
    catch {
        Write-Host "Ошибка в шаге: $Title" -ForegroundColor Red
        Write-Host $_.Exception.Message -ForegroundColor Red
        Write-Host ""
        return $false
    }
}

$results = @{}

$results["docker"] = [bool](Invoke-Step "Запуск контейнеров (docker-compose up -d)" {
    docker-compose up -d | Out-Null
    docker-compose ps
})

$results["jdbc"] = [bool](Invoke-Step "Проверка JDBC драйверов (download_jdbc_jars.ps1)" {
    powershell -NoLogo -NoProfile -ExecutionPolicy Bypass -File ".\scripts\download_jdbc_jars.ps1"
})

$results["data_loader"] = [bool](Invoke-Step "Загрузка данных в PostgreSQL (data_loader)" {
    docker-compose exec data_loader sh -c "pip install --no-cache-dir pandas sqlalchemy psycopg2-binary >/tmp/pip.log 2>&1 && python /scripts/load_data.py && python /scripts/run_ddl.py && python /scripts/run_dml.py"
})

$results["clickhouse_schema"] = [bool](Invoke-Step "Создание таблиц в ClickHouse (pet_shop_analytics)" {
    if ($RecreateClickHouseTables) {
        Write-Host "Пересоздание таблиц по скрипту create_clickhouse_tables.ps1..." -ForegroundColor DarkYellow
    }
    powershell -NoLogo -NoProfile -ExecutionPolicy Bypass -File ".\scripts\create_clickhouse_tables.ps1"
})

$results["spark_transform"] = [bool](Invoke-Step "Spark: трансформация в модель звезда (Job=transform)" {
    powershell -NoLogo -NoProfile -ExecutionPolicy Bypass -File ".\scripts\run_spark_jobs.ps1" -Job transform
})

$results["spark_reports"] = [bool](Invoke-Step "Spark: создание отчетов в ClickHouse (Job=reports)" {
    powershell -NoLogo -NoProfile -ExecutionPolicy Bypass -File ".\scripts\run_spark_jobs.ps1" -Job reports
})

$script:pgSummary = $null
$results["check_postgres"] = [bool](Invoke-Step "Проверка данных в PostgreSQL (fact + dimensions)" {
    $script:pgSummary = docker-compose exec postgres psql -U chistyakovartem214 -d pet_shop_dw -t -A -F"," -c "SELECT 'fact_sales' AS table_name, COUNT(*) AS records FROM fact_sales UNION ALL SELECT 'dimension_customers', COUNT(*) FROM dimension_customers UNION ALL SELECT 'dimension_products', COUNT(*) FROM dimension_products UNION ALL SELECT 'dimension_stores', COUNT(*) FROM dimension_stores;"
    $script:pgSummary -split '\s+' | ForEach-Object { if ($_ -ne '') { Write-Host $_ } }
})

$script:chSummary = $null
$results["check_clickhouse"] = [bool](Invoke-Step "Проверка витрин в ClickHouse (pet_shop_analytics)" {
    $script:chSummary = docker-compose exec clickhouse clickhouse-client --query "SELECT 'report_products_sales' AS table_name, COUNT(*) AS records FROM pet_shop_analytics.report_products_sales UNION ALL SELECT 'report_customers_sales', COUNT(*) FROM pet_shop_analytics.report_customers_sales UNION ALL SELECT 'report_time_sales', COUNT(*) FROM pet_shop_analytics.report_time_sales UNION ALL SELECT 'report_stores_sales', COUNT(*) FROM pet_shop_analytics.report_stores_sales UNION ALL SELECT 'report_suppliers_sales', COUNT(*) FROM pet_shop_analytics.report_suppliers_sales UNION ALL SELECT 'report_product_quality', COUNT(*) FROM pet_shop_analytics.report_product_quality;" --format CSV

    $script:chSummary -split "`n" | ForEach-Object { if ($_ -ne '') { Write-Host $_ } }

    Write-Host ""
    Write-Host "TOP-5 записей каждой витрины (ClickHouse):" -ForegroundColor Yellow

    function Show-TopRows {
        param(
            [string]$title,
            [string]$query
        )
        Write-Host ""
        Write-Host $title -ForegroundColor DarkCyan
        $result = docker-compose exec clickhouse clickhouse-client --format CSVWithNames --query $query
        $result -split "`n" | ForEach-Object { if ($_ -ne '') { Write-Host $_ } }
    }

    Show-TopRows -title "report_products_sales (TOP 5 строк)" -query "SELECT * FROM pet_shop_analytics.report_products_sales LIMIT 5"
    Show-TopRows -title "report_customers_sales (TOP 5 строк)" -query "SELECT * FROM pet_shop_analytics.report_customers_sales LIMIT 5"
    Show-TopRows -title "report_time_sales (TOP 5 строк)" -query "SELECT * FROM pet_shop_analytics.report_time_sales LIMIT 5"
    Show-TopRows -title "report_stores_sales (TOP 5 строк)" -query "SELECT * FROM pet_shop_analytics.report_stores_sales LIMIT 5"
    Show-TopRows -title "report_suppliers_sales (TOP 5 строк)" -query "SELECT * FROM pet_shop_analytics.report_suppliers_sales LIMIT 5"
    Show-TopRows -title "report_product_quality (TOP 5 строк)" -query "SELECT * FROM pet_shop_analytics.report_product_quality LIMIT 5"
})

Write-Host ""
Write-Host "==================================" -ForegroundColor Cyan
Write-Host "ИТОГОВЫЙ ОТЧЕТ ПО ЛР2 (требования)" -ForegroundColor Cyan
Write-Host "==================================" -ForegroundColor Cyan

function Show-Flag {
    param([bool]$ok)
    if ($ok) { return "[OK] " } else { return "[FAIL] " }
}

Write-Host (Show-Flag $results["docker"])          "Docker + инфраструктура (PostgreSQL, Spark, ClickHouse)"
Write-Host (Show-Flag $results["jdbc"])            "JDBC драйверы (PostgreSQL + ClickHouse)"
Write-Host (Show-Flag $results["data_loader"])     "Загрузка исходных данных в PostgreSQL (staging -> mock_data)"
Write-Host (Show-Flag $results["clickhouse_schema"]) "Схема ClickHouse (pet_shop_analytics, 6 таблиц витрин)"
Write-Host (Show-Flag $results["spark_transform"]) "Spark ETL: PostgreSQL staging -> модель звезда в PostgreSQL"
Write-Host (Show-Flag $results["spark_reports"])   "Spark ETL: модель звезда PostgreSQL -> отчеты в ClickHouse"
Write-Host (Show-Flag $results["check_postgres"])  "Проверка объемов данных в fact/dim таблицах PostgreSQL"
Write-Host (Show-Flag $results["check_clickhouse"]) "Проверка объемов данных в витринах ClickHouse"

Write-Host ""
Write-Host "Краткая сводка по PostgreSQL (table,records):" -ForegroundColor Yellow
if ($script:pgSummary) {
    $script:pgSummary -split '\s+' | ForEach-Object { if ($_ -ne '') { Write-Host $_ } }
} else {
    Write-Host "нет данных (шаг проверки не выполнен)" -ForegroundColor DarkYellow
}

Write-Host ""
Write-Host "Краткая сводка по ClickHouse (table,records):" -ForegroundColor Yellow
if ($script:chSummary) {
    $script:chSummary -split "`n" | ForEach-Object { if ($_ -ne '') { Write-Host $_ } }
} else {
    Write-Host "нет данных (шаг проверки не выполнен)" -ForegroundColor DarkYellow
}

Write-Host ""
Write-Host "Готово" -ForegroundColor Cyan



