CREATE OR ALTER PROCEDURE dbo.usp_load_gold_air_quality
AS
BEGIN
    DROP TABLE IF EXISTS dbo.fact_air_quality_daily;

    CREATE TABLE dbo.fact_air_quality_daily AS
    SELECT
        m.date_key,
        m.measurement_date,
        MAX(CASE WHEN m.parameter_name = 'pm25' THEN m.daily_avg_value END) AS pm25_avg_ugm3,
        MAX(CASE WHEN m.parameter_name = 'pm25' THEN m.daily_min_value END) AS pm25_min_ugm3,
        MAX(CASE WHEN m.parameter_name = 'pm25' THEN m.daily_max_value END) AS pm25_max_ugm3,
        MAX(CASE WHEN m.parameter_name = 'pm25' THEN m.active_sensors_count END) AS pm25_sensors_count,
        MAX(CASE WHEN m.parameter_name = 'no2' THEN m.daily_avg_value END) AS no2_avg_ppm,
        MAX(CASE WHEN m.parameter_name = 'no2' THEN m.daily_min_value END) AS no2_min_ppm,
        MAX(CASE WHEN m.parameter_name = 'no2' THEN m.daily_max_value END) AS no2_max_ppm,
        MAX(CASE WHEN m.parameter_name = 'no2' THEN m.active_sensors_count END) AS no2_sensors_count
    FROM [Lakehouse_Silver].[dbo].[openaq_measurements_silver] m
    GROUP BY
        m.date_key,
        m.measurement_date;
END;
GO

EXEC dbo.usp_load_gold_air_quality;
GO

SELECT TOP 10 * FROM dbo.fact_air_quality_daily ORDER BY measurement_date DESC;