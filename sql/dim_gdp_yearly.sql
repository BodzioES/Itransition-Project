CREATE TABLE [dbo].[dim_gdp_yearly] (
    [gdp_year]       INT            NULL,
    [country_iso3]   VARCHAR (8000) NULL,
    [country_name]   VARCHAR (8000) NULL,
    [indicator_id]   VARCHAR (8000) NULL,
    [indicator_name] VARCHAR (8000) NULL,
    [gdp_usd]        FLOAT (53)     NULL
);


GO