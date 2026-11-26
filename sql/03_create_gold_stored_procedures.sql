-- 1. Procedura ładująca tabele wymiarów warstwy Gold
CREATE OR ALTER PROCEDURE dbo.usp_load_gold_dimensions
AS
BEGIN
    DROP TABLE IF EXISTS dbo.dim_date;
    DROP TABLE IF EXISTS dbo.dim_gdp_yearly;
    DROP TABLE IF EXISTS dbo.dim_air_quality_sensors;

    CREATE TABLE dbo.dim_date AS
    WITH all_dates AS (
        SELECT DISTINCT pickup_date AS full_date
        FROM [Lakehouse_Silver].[dbo].[nyc_taxi_silver]
        WHERE pickup_date IS NOT NULL
        UNION
        SELECT DISTINCT rate_date AS full_date
        FROM [Lakehouse_Silver].[dbo].[fx_usd_eur_silver]
        WHERE rate_date IS NOT NULL
    )
    SELECT
        YEAR(full_date) * 10000 + MONTH(full_date) * 100 + DAY(full_date) AS date_key,
        full_date,
        YEAR(full_date) AS year,
        MONTH(full_date) AS month,
        CAST(DATENAME(MONTH, full_date) AS VARCHAR(20)) AS month_name,
        DAY(full_date) AS day,
        DATEPART(WEEKDAY, full_date) AS day_of_week,
        CAST(DATENAME(WEEKDAY, full_date) AS VARCHAR(20)) AS day_name,
        CASE WHEN DATEPART(WEEKDAY, full_date) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend
    FROM all_dates;

    CREATE TABLE dbo.dim_gdp_yearly AS
    SELECT
        year AS gdp_year,
        country_iso3,
        country_name,
        indicator_id,
        indicator_name,
        gdp_usd
    FROM [Lakehouse_Silver].[dbo].[gdp_silver];

    CREATE TABLE dbo.dim_air_quality_sensors AS
    SELECT
        sensor_id,
        location_id,
        location_name,
        country_code,
        country_name,
        latitude,
        longitude,
        parameter_name,
        parameter_display_name,
        parameter_units,
        provider_name,
        is_monitor
    FROM [Lakehouse_Silver].[dbo].[openaq_silver];
END;
GO

-- 2. Procedura ładująca główną tabelę faktów warstwy Gold
CREATE OR ALTER PROCEDURE dbo.usp_load_gold_fact_taxi
AS
BEGIN
    DROP TABLE IF EXISTS dbo.fact_taxi_trips;

    CREATE TABLE dbo.fact_taxi_trips AS
    WITH fx_grouped AS (
        SELECT
            d.full_date,
            fx.exchange_rate,
            COUNT(fx.exchange_rate) OVER (
                ORDER BY d.full_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS fx_grp
        FROM dbo.dim_date d
        LEFT JOIN [Lakehouse_Silver].[dbo].[fx_usd_eur_silver] fx
            ON d.full_date = fx.rate_date
    ),
    fx_filled AS (
        SELECT
            full_date,
            MAX(exchange_rate) OVER (PARTITION BY fx_grp) AS usd_per_eur_rate
        FROM fx_grouped
    )
    SELECT
        YEAR(t.pickup_date) * 10000 + MONTH(t.pickup_date) * 100 + DAY(t.pickup_date) AS date_key,
        t.pickup_year AS gdp_year,
        t.tpep_pickup_datetime AS pickup_datetime,
        t.tpep_dropoff_datetime AS dropoff_datetime,
        t.pickup_hour,
        t.vendorid AS vendor_id,
        t.pulocationid AS pu_location_id,
        t.dolocationid AS do_location_id,
        t.payment_type,
        t.passenger_count,
        t.trip_distance AS trip_distance_miles,
        t.trip_duration_minutes,
        t.fare_amount AS fare_amount_usd,
        t.tip_amount AS tip_amount_usd,
        t.total_amount AS total_amount_usd,
        fx.usd_per_eur_rate,
        ROUND(t.total_amount / NULLIF(fx.usd_per_eur_rate, 0), 2) AS total_amount_eur
    FROM [Lakehouse_Silver].[dbo].[nyc_taxi_silver] t
    LEFT JOIN fx_filled fx
        ON t.pickup_date = fx.full_date;
END;