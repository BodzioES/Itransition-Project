# Data Dictionary — UrbanAnalyticsWarehouse (Gold layer)

Complete column-level reference for the eight Gold tables defined in `sql/01`–`sql/05`. Types are
the physical types produced by the `CREATE TABLE … AS SELECT` statements in the Fabric Warehouse:

* Spark `int` → `int`, Spark `bigint` → `bigint`, Spark `double` → `float`.
* Spark `date` → `date`, Spark `timestamp` → `datetime2(6)`.
* Spark `string` → `varchar(8000)` (Fabric's default width for Spark strings); `month_name` and
  `day_name` are explicitly cast to `varchar(20)`.
* Spark `boolean` → `bit`.

> **Keys are logical, not enforced.** Every Gold table is built with `CREATE TABLE … AS SELECT`, so
> the warehouse declares **no physical PRIMARY KEY / FOREIGN KEY constraints**. The keys below are
> the ones used by the loading procedures and by the Direct Lake semantic model; uniqueness is
> guaranteed upstream by the `dropDuplicates()` calls in Silver and verified by the quality gate.

Back to the [project overview](../README.md) · [governance](governance.md) ·
[architecture decisions](architecture_decisions.md)

---

## `dim_date` — conformed calendar dimension

| Column | Type | Key | Business meaning |
|--------|------|-----|------------------|
| `date_key` | `int` | **PK** | Surrogate date key in `YYYYMMDD` form (e.g. `20240229`); join key for every fact table. |
| `full_date` | `date` | | Calendar date. Union of taxi pickup dates, ECB rate dates and OpenAQ measurement dates. |
| `year` | `int` | | Calendar year; links trips to annual GDP context. |
| `month` | `int` | | Calendar month 1–12. |
| `month_name` | `varchar(20)` | | English month name for report labels. |
| `day` | `int` | | Day of month 1–31. |
| `day_of_week` | `int` | | Day of week as returned by `DATEPART(WEEKDAY, …)`. |
| `day_name` | `varchar(20)` | | English weekday name. |
| `is_weekend` | `int` | | `1` for Saturday/Sunday, `0` otherwise. |

## `dim_zone` — NYC TLC taxi zones

| Column | Type | Key | Business meaning |
|--------|------|-----|------------------|
| `location_id` | `int` | **PK** | Official TLC `LocationID` (1–265); joins to `pu_location_id` / `do_location_id`. |
| `borough` | `varchar(8000)` | | Borough (Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR, Unknown). |
| `zone_name` | `varchar(8000)` | | Zone name, e.g. `Upper East Side North`. |
| `service_zone` | `varchar(8000)` | | Service classification (`Yellow Zone`, `Boro Zone`, `Airports`, …). |

## `dim_fx_daily` — daily USD/EUR reference rate

| Column | Type | Key | Business meaning |
|--------|------|-----|------------------|
| `date_key` | `int` | **PK**, FK → `dim_date` | Date key (`YYYYMMDD`). |
| `rate_date` | `date` | | Calendar date of the observation. |
| `usd_per_eur_rate` | `float` | | ECB reference rate: USD per 1 EUR. Weekend/holiday gaps are forward-filled, so every date in `dim_date` has a usable rate. |

## `dim_gdp_yearly` — US GDP by year

| Column | Type | Key | Business meaning |
|--------|------|-----|------------------|
| `gdp_year` | `int` | **PK** | Observation year; joins to `fact_taxi_trips.gdp_year` and `fact_taxi_daily.gdp_year`. |
| `country_iso3` | `varchar(8000)` | | ISO-3166 alpha-3 code (`USA`); 3 characters of meaning in an 8000-wide column inherited from Spark. |
| `country_name` | `varchar(8000)` | | Country name as returned by the World Bank. |
| `indicator_id` | `varchar(8000)` | | World Bank indicator code (`NY.GDP.MKTP.CD`). |
| `indicator_name` | `varchar(8000)` | | Human-readable indicator description. |
| `gdp_usd` | `float` | | Annual US GDP in current US dollars. |

## `dim_air_quality_sensors` — OpenAQ stations and sensors

| Column | Type | Key | Business meaning |
|--------|------|-----|------------------|
| `sensor_id` | `bigint` | **PK** | OpenAQ sensor identifier; one row per physical sensor. |
| `location_id` | `bigint` | | OpenAQ station identifier (one station can host several sensors). |
| `location_name` | `varchar(8000)` | | Station name (school, park, …). |
| `country_code` | `varchar(8000)` | | ISO country code of the station (`US`). |
| `country_name` | `varchar(8000)` | | Country name. |
| `latitude` | `float` | | Station latitude (WGS84). |
| `longitude` | `float` | | Station longitude (WGS84). |
| `parameter_name` | `varchar(8000)` | | Machine-readable parameter (`pm25`, `no2`, `pm10`, `o3`, …). |
| `parameter_display_name` | `varchar(8000)` | | Display label used on the sensor-inventory chart (`PM2.5`, `NO2`, …). |
| `parameter_units` | `varchar(8000)` | | Unit of measure (`µg/m³`, `ppm`, …). |
| `provider_name` | `varchar(8000)` | | Organisation providing the measurements. |
| `is_monitor` | `bit` | | `1` for reference-grade monitors, `0` for low-cost sensors — a reliability proxy. |

## `fact_taxi_trips` — trip-level fact

| Column | Type | Key | Business meaning |
|--------|------|-----|------------------|
| `date_key` | `int` | FK → `dim_date` | Pickup date key (`YYYYMMDD`). |
| `gdp_year` | `int` | FK → `dim_gdp_yearly` | Pickup year, used to attach annual GDP context. |
| `pickup_datetime` | `datetime2(6)` | | Meter-activated pickup timestamp. |
| `dropoff_datetime` | `datetime2(6)` | | Meter-disabled drop-off timestamp. |
| `pickup_hour` | `int` | | Hour of day 0–23; drives the hourly revenue profile. |
| `vendor_id` | `int` | | TLC TPEP provider code. |
| `pu_location_id` | `int` | FK → `dim_zone` | Pickup taxi zone. |
| `do_location_id` | `int` | FK → `dim_zone` | Drop-off taxi zone. |
| `payment_type` | `bigint` | | Payment method code (1 = credit card, 2 = cash, …). |
| `passenger_count` | `bigint` | | Number of passengers reported by the driver. |
| `trip_distance_miles` | `float` | | Trip distance in miles; zero-distance trips are retained for waiting/cancellation charges. |
| `trip_duration_minutes` | `float` | | Drop-off minus pickup in minutes; filtered to `0 < duration < 1440`. |
| `fare_amount_usd` | `float` | | Metered fare in USD. |
| `tip_amount_usd` | `float` | | Tip in USD (reliably captured for card payments only). |
| `total_amount_usd` | `float` | | Total charged amount in USD, including taxes and surcharges. |
| `usd_per_eur_rate` | `float` | | ECB rate applied to the trip (denormalised from `dim_fx_daily`). |
| `total_amount_eur` | `float` | | `total_amount_usd / usd_per_eur_rate`, rounded to 2 decimals. |

## `fact_taxi_daily` — daily taxi aggregates

| Column | Type | Key | Business meaning |
|--------|------|-----|------------------|
| `date_key` | `int` | **PK** (with `gdp_year`), FK → `dim_date` | Aggregation date. |
| `gdp_year` | `int` | **PK** (with `date_key`), FK → `dim_gdp_yearly` | Year of the aggregation date. |
| `total_trips` | `int` | | Number of trips completed that day — `COUNT(*)`, so the warehouse type is `int`, not `bigint`. |
| `total_passengers` | `bigint` | | Sum of `passenger_count`. |
| `total_distance_miles` | `float` | | Sum of trip distances. |
| `avg_distance_miles` | `float` | | Average trip distance (3.45 mi across Q1 2024). |
| `avg_duration_minutes` | `float` | | Average trip duration. |
| `total_fare_usd` | `float` | | Sum of metered fares. |
| `avg_fare_usd` | `float` | | Average fare per trip (18.97 USD across Q1 2024). |
| `total_tip_usd` | `float` | | Sum of tips. |
| `total_revenue_usd` | `float` | | Sum of total charged amounts. |
| `usd_per_eur_rate` | `float` | | ECB rate for that day. |
| `total_revenue_eur` | `float` | | Daily revenue converted to EUR. |

## `fact_air_quality_daily` — daily air-quality fact

| Column | Type | Key | Business meaning |
|--------|------|-----|------------------|
| `date_key` | `int` | **PK**, FK → `dim_date` | Measurement date key (`YYYYMMDD`). |
| `measurement_date` | `date` | | Calendar date of the daily aggregate. |
| `pm25_avg_ugm3` | `float` | | City-wide average PM2.5 concentration, µg/m³. |
| `pm25_min_ugm3` | `float` | | Lowest station-level PM2.5 average of the day. |
| `pm25_max_ugm3` | `float` | | Highest station-level PM2.5 average of the day. |
| `pm25_sensors_count` | `int` | | Number of PM2.5 sensors that reported that day (coverage indicator). |
| `no2_avg_ppm` | `float` | | City-wide average NO2 concentration, ppm. |
| `no2_min_ppm` | `float` | | Lowest station-level NO2 average of the day. |
| `no2_max_ppm` | `float` | | Highest station-level NO2 average of the day. |
| `no2_sensors_count` | `int` | | Number of NO2 sensors that reported that day. |

---

## Relationships

```
dim_date ──1:N──▶ fact_taxi_trips        dim_zone ──1:N──▶ fact_taxi_trips (pickup / drop-off)
   │                                     dim_fx_daily ──1:N──▶ fact_taxi_trips (rate attribute)
   ├──1:N──▶ fact_taxi_daily
   └──1:N──▶ fact_air_quality_daily      dim_gdp_yearly ──1:N──▶ fact_taxi_daily
                                         dim_air_quality_sensors ──(descriptive)──▶ fact_air_quality_daily
```

`fact_air_quality_daily` has no sensor key: it is aggregated per day and parameter, so the sensor
dimension is used for context and coverage analysis rather than as a direct join in the fact table.

## Sources of each table

| Gold table | Loaded by | Reads from |
|------------|-----------|------------|
| `dim_date` | `usp_load_gold_dimensions` | `nyc_taxi_silver`, `fx_usd_eur_silver`, `openaq_measurements_silver` |
| `dim_zone` | `usp_load_gold_dimensions` | `nyc_taxi_zones_silver` |
| `dim_fx_daily` | `usp_load_gold_dimensions` | `dim_date` + `fx_usd_eur_silver` (forward fill) |
| `dim_gdp_yearly` | `usp_load_gold_dimensions` | `gdp_silver` |
| `dim_air_quality_sensors` | `usp_load_gold_dimensions` | `openaq_silver` |
| `fact_taxi_trips` | `usp_load_gold_fact_taxi` | `nyc_taxi_silver` + `dim_fx_daily` |
| `fact_taxi_daily` | `usp_load_gold_fact_taxi` | `fact_taxi_trips` |
| `fact_air_quality_daily` | `usp_load_gold_air_quality` | `openaq_measurements_silver` |
