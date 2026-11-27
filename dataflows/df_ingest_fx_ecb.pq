// Dataflow Gen2: df_ingest_fx_ecb
// Query:        fx_usd_eur_dataflow
// Destination:  Lakehouse_Bronze / dbo / fx_usd_eur_bronze_dataflow (update method: Replace)
// Source:       ECB daily USD/EUR reference rate (CSV), anonymous access
let
    Source = Csv.Document(
        Web.Contents("https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata"),
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    SelectedColumns = Table.SelectColumns(
        PromotedHeaders,
        {"TIME_PERIOD", "CURRENCY", "CURRENCY_DENOM", "FREQ", "OBS_VALUE", "OBS_STATUS"}
    ),
    TypedColumns = Table.TransformColumnTypes(
        SelectedColumns,
        {
            {"TIME_PERIOD", type date},
            {"OBS_VALUE", type number},
            {"CURRENCY", type text},
            {"CURRENCY_DENOM", type text},
            {"FREQ", type text},
            {"OBS_STATUS", type text}
        },
        "en-US"
    ),
    RenamedColumns = Table.RenameColumns(
        TypedColumns,
        {
            {"TIME_PERIOD", "rate_date"},
            {"OBS_VALUE", "exchange_rate"},
            {"CURRENCY", "currency"},
            {"CURRENCY_DENOM", "currency_denom"},
            {"FREQ", "frequency"},
            {"OBS_STATUS", "obs_status"}
        }
    ),
    FilteredRows = Table.SelectRows(
        RenamedColumns,
        each [rate_date] <> null and [exchange_rate] <> null and [exchange_rate] > 0
    ),
    AddedIngestedAt = Table.AddColumn(
        FilteredRows,
        "ingested_at_utc",
        each DateTimeZone.RemoveZone(DateTimeZone.UtcNow()),
        type datetime
    )
in
    AddedIngestedAt
