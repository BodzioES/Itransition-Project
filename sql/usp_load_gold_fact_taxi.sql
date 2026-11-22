-- 2. Procedura ładująca główną tabelę faktów warstwy Gold
CREATE   PROCEDURE dbo.usp_load_gold_fact_taxi
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

GO