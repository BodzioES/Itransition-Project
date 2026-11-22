CREATE TABLE [dbo].[dim_date] (
    [date_key]    INT          NULL,
    [full_date]   DATE         NULL,
    [year]        INT          NULL,
    [month]       INT          NULL,
    [month_name]  VARCHAR (20) NULL,
    [day]         INT          NULL,
    [day_of_week] INT          NULL,
    [day_name]    VARCHAR (20) NULL,
    [is_weekend]  INT          NOT NULL
);


GO
