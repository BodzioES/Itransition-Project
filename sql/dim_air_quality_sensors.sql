CREATE TABLE [dbo].[dim_air_quality_sensors] (
    [sensor_id]              BIGINT         NULL,
    [location_id]            BIGINT         NULL,
    [location_name]          VARCHAR (8000) NULL,
    [country_code]           VARCHAR (8000) NULL,
    [country_name]           VARCHAR (8000) NULL,
    [latitude]               FLOAT (53)     NULL,
    [longitude]              FLOAT (53)     NULL,
    [parameter_name]         VARCHAR (8000) NULL,
    [parameter_display_name] VARCHAR (8000) NULL,
    [parameter_units]        VARCHAR (8000) NULL,
    [provider_name]          VARCHAR (8000) NULL,
    [is_monitor]             BIT            NULL
);


GO