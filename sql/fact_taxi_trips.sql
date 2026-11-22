CREATE TABLE [dbo].[fact_taxi_trips] (
    [date_key]              INT           NULL,
    [gdp_year]              INT           NULL,
    [pickup_datetime]       DATETIME2 (6) NULL,
    [dropoff_datetime]      DATETIME2 (6) NULL,
    [pickup_hour]           INT           NULL,
    [vendor_id]             INT           NULL,
    [pu_location_id]        INT           NULL,
    [do_location_id]        INT           NULL,
    [payment_type]          BIGINT        NULL,
    [passenger_count]       BIGINT        NULL,
    [trip_distance_miles]   FLOAT (53)    NULL,
    [trip_duration_minutes] FLOAT (53)    NULL,
    [fare_amount_usd]       FLOAT (53)    NULL,
    [tip_amount_usd]        FLOAT (53)    NULL,
    [total_amount_usd]      FLOAT (53)    NULL,
    [usd_per_eur_rate]      FLOAT (53)    NULL,
    [total_amount_eur]      FLOAT (53)    NULL
);


GO