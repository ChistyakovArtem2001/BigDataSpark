# Лабораторная работа №2 - BigDataSpark

## Описание

Анализ больших данных - лабораторная работа №2 - ETL реализованный с помощью Spark

В ЛР2 реализован ETL-пайплайн с помощью Apache Spark для трансформации данных из исходной модели в модель данных звезда в PostgreSQL + создание аналитических отчетов в ClickHouse.


## Установка и запуск


Для быстрой проверки всех витрин и соответствия требованиям ЛР, можно выполнить скрипт `scripts/full_pipeline_check.ps1` — он автоматически делает SQL‑проверки и выводит краткий отчет

### 1. Автоматический полный прогон + проверка требований ЛР

Рекомендуемый способ - скрипт `scripts/full_pipeline_check.ps1`. Он:
- поднимает и проверяет инфраструктуру (PostgreSQL, Spark, ClickHouse),
- убеждается, что JDBC-драйверы скачаны,
- загружает исходные данные в PostgreSQL,
- создает схему и таблицы витрин в ClickHouse,
- запускает оба Spark приложения (трансформация и создание витрин),
- выполняет проверки в PostgreSQL и ClickHouse,
- выводит итоговый отчет по требованиям ЛР

Запуск:

```powershell
powershell -NoLogo -NoProfile -ExecutionPolicy Bypass -File .\scripts\full_pipeline_check.ps1
```

Результатом скрипта будет:
- список шагов с пометкой `[OK]` / `[FAIL]`:
  - Docker + инфраструктура (PostgreSQL, Spark, ClickHouse)
  - JDBC драйверы (PostgreSQL + ClickHouse)
  - Загрузка исходных данных в PostgreSQL (staging → mock_data)
  - Схема ClickHouse (`pet_shop_analytics`, 6 таблиц витрин)
  - Spark ETL: PostgreSQL staging → модель звезда в PostgreSQL
  - Spark ETL: модель звезда PostgreSQL → отчеты в ClickHouse
  - Проверка объемов данных в fact/dim таблицах PostgreSQL
  - Проверка объемов данных в витринах ClickHouse
- краткая сводка по количеству записей:
  - в PostgreSQL (`fact_sales`, `dimension_customers`, `dimension_products`, `dimension_stores`),
  - в ClickHouse (все 6 витрин),
- **TOP‑5 строк по каждой витрине** в ClickHouse


### 2. Ручной запуск

Запуске всех сервисов :

```powershell
docker-compose up -d
```

Запустятся:
- **PostgreSQL**
- **ClickHouse**
- **Spark Master**
- **Spark Worker**


### 3. Загрузка исходных данных в PostgreSQL

Исходные данные загружаются в PostgreSQL через контейнер `data_loader`.
Делается автоматом, но можно руками -

```powershell
docker-compose exec data_loader python /scripts/load_data.py
docker-compose exec data_loader python /scripts/run_ddl.py
docker-compose exec data_loader python /scripts/run_dml.py
```

### 4. Создание таблиц в ClickHouse

Создаем таблицы в ClickHouse:

```powershell
.\scripts\create_clickhouse_tables.ps1
```

### 5. Скачивание JDBC-драйверов

```powershell
.\scripts\download_jdbc_jars.ps1
```

Ручное создание пользователя (если потребуется (но вообще он автоматом регается)):

```powershell
docker-compose exec clickhouse clickhouse-client --query="CREATE USER IF NOT EXISTS spark_etl IDENTIFIED WITH plaintext_password BY 'spark_pass' HOST ANY;"
docker-compose exec clickhouse clickhouse-client --query="GRANT ALL PRIVILEGES ON pet_shop_analytics.* TO spark_etl;"
docker-compose exec clickhouse clickhouse-client --query="GRANT SHOW TABLES, CREATE TABLE, DROP TABLE ON pet_shop_analytics.* TO spark_etl;"
```


### 7. Ручной запуск Spark джобов


- **Трансформация в модель звезда:**
```powershell
.\scripts\run_spark_jobs.ps1 -Job transform
```

- **Создание отчетов в ClickHouse:**
```powershell
.\scripts\run_spark_jobs.ps1 -Job reports
```

- **Оба джоба подряд:**
```powershell
.\scripts\run_spark_jobs.ps1 -Job both
```

## Созданные отчеты в ClickHouse

После выполнения второго джоба в ClickHouse будут созданы следующие таблицы:

1. **report_products_sales** - Витрина продаж по продуктам
   - Топ-10 самых продаваемых продуктов
   - Общая выручка по категориям продуктов
   - Средний рейтинг и количество отзывов

2. **report_customers_sales** - Витрина продаж по клиентам
   - Топ-10 клиентов с наибольшей общей суммой покупок
   - Распределение клиентов по странам
   - Средний чек для каждого клиента

3. **report_time_sales** - Витрина продаж по времени
   - Месячные и годовые тренды продаж
   - Сравнение выручки за разные периоды
   - Средний размер заказа по месяцам

4. **report_stores_sales** - Витрина продаж по магазинам
   - Топ-5 магазинов с наибольшей выручкой
   - Распределение продаж по городам и странам
   - Средний чек для каждого магазина

5. **report_suppliers_sales** - Витрина продаж по поставщикам
   - Топ-5 поставщиков с наибольшей выручкой
   - Средняя цена товаров от каждого поставщика
   - Распределение продаж по странам поставщиков

6. **report_product_quality** - Витрина качества продукции
   - Продукты с наивысшим и наименьшим рейтингом
   - Корреляция между рейтингом и объемом продаж
   - Продукты с наибольшим количеством отзывов

## Проверка результатов

### Проверка данных в PostgreSQL

Подключение к PostgreSQL через DBeaver или psql:

```powershell
docker-compose exec postgres psql -U chistyakovartem214 -d pet_shop_dw
```

Проверка количества записей в таблицах:

```sql
SELECT 'fact_sales' AS table_name, COUNT(*) AS records FROM fact_sales
UNION ALL
SELECT 'dimension_customers', COUNT(*) FROM dimension_customers
UNION ALL
SELECT 'dimension_products', COUNT(*) FROM dimension_products
UNION ALL
SELECT 'dimension_stores', COUNT(*) FROM dimension_stores;
```

### Проверка данных в ClickHouse

Подключение:

```powershell
docker-compose exec clickhouse clickhouse-client
```

Далее выберите БД `pet_shop_analytics`:

```sql
USE pet_shop_analytics;

SHOW TABLES;

SELECT COUNT(*) FROM report_products_sales;
SELECT COUNT(*) FROM report_customers_sales;
SELECT COUNT(*) FROM report_time_sales;
SELECT COUNT(*) FROM report_stores_sales;
SELECT COUNT(*) FROM report_suppliers_sales;
SELECT COUNT(*) FROM report_product_quality;
```

Пример запроса для просмотра данных:

```sql
SELECT * FROM pet_shop_analytics.report_products_sales
ORDER BY total_revenue DESC
LIMIT 10;
```


