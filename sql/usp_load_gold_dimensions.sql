-- 1. Procedura ładująca tabele wymiarów warstwy Gold
CREATE   PROCEDURE dbo.usp_load_gold_dimensions
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