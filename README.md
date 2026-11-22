# Urban Analytics Platform — Microsoft Fabric End-to-End Data Engineering Project

An end-to-end data engineering and analytics platform built on **Microsoft Fabric**, integrating urban mobility records (**NYC Taxi**), environmental monitoring data (**OpenAQ**), and macroeconomic indicators (**World Bank GDP** & **ECB Foreign Exchange rates**) into a unified lakehouse and data warehouse architecture.

---

## 1. Architecture & Data Flow (Medallion Architecture)

The platform follows a three-tier **Medallion Architecture** orchestrated end-to-end by the master pipeline **`pl_master_e2e`**:

* **Bronze Layer (`Lakehouse_Bronze`):** Automated ingestion of raw heterogeneous datasets—NYC TLC monthly Parquet files and REST API payloads from OpenAQ, World Bank, and the European Central Bank—orchestrated by `pl_master_ingest_bronze` and PySpark ingestion notebooks.
* **Silver Layer (`Lakehouse_Silver`):** Schema standardization, deduplication, data type normalization, and daily USD-to-EUR currency conversion executed via `pl_master_transform_silver`.
* **Gold Layer (`UrbanAnalyticsWarehouse`):** Dimensional **Star Schema** powering the `UrbanAnalytics_SemanticModel` and the interactive Power BI report `UrbanAnalytics_Dashboard`.

### Execution & Resource Optimization
All Bronze-layer ingestion notebooks share a single Spark session attached to the default `Lakehouse_Silver` and write Delta tables directly to physical **ABFS paths** inside `Lakehouse_Bronze`. This architectural choice eliminates multi-session startup overhead and prevents capacity throttling errors (*HTTP 430*).

![Data Lineage](lineage.png)

---

## 2. Repository Contents

* `nb_ingest_fx.ipynb` — Ingests daily USD/EUR exchange rates (CSV) from the European Central Bank API into the Bronze layer.
* `nb_ingest_gdp.ipynb` — Ingests annual US GDP indicators (JSON) from the World Bank API into the Bronze layer.
* `nb_ingest_openaq.ipynb` — Ingests air quality station metadata and pollutant parameters from the OpenAQ API into the Bronze layer.
* `nb_transform_fx.ipynb` — Cleans and standardizes exchange rate records for the Silver layer.
* `nb_transform_gdp.ipynb` — Normalizes annual macroeconomic GDP data for the Silver layer.
* `nb_transform_openaq.ipynb` — Deduplicates and structures sensor and location attributes for the Silver layer.
* `nb_transform_taxi.ipynb` — Cleans NYC Taxi trip records, derives temporal attributes, and enriches fares with daily EUR conversions.
* `lineage.png` — End-to-end Microsoft Fabric data lineage diagram.
* `README.md` — Project architecture overview and Gold-layer data dictionary.

---

## 3. Data Dictionary (Gold Layer Star Schema)

### Fact Table: `fact_taxi_trips`
| Column | Description |
| :--- | :--- |
| `date_key` | Foreign key linking each trip to the calendar dimension (`dim_date`) |
| `gdp_year` | Foreign key linking the trip year to the macroeconomic dimension (`dim_gdp_yearly`) |
| `pickup_datetime` | Timestamp when the meter was engaged |
| `dropoff_datetime` | Timestamp when the meter was disengaged |
| `pickup_hour` | Hour of the day (0–23) extracted from pickup time for rush-hour analysis |
| `pu_location_id` | TLC Taxi Zone ID where the trip originated |
| `do_location_id` | TLC Taxi Zone ID where the trip ended |
| `passenger_count` | Number of passengers in the vehicle |
| `trip_distance_miles` | Elapsed trip distance in miles |
| `trip_duration_minutes` | Total trip duration calculated in minutes |
| `fare_amount_usd` | Base time-and-distance fare in USD |
| `tip_amount_usd` | Tip amount in USD |
| `total_amount_usd` | Total gross amount charged to passengers in USD |
| `usd_per_eur_rate` | Daily USD/EUR reference exchange rate sourced from the ECB |
| `total_amount_eur` | Total gross amount converted to EUR using the daily exchange rate |
| `payment_type` | Numeric code signifying how the passenger paid for the trip |
| `vendor_id` | Code indicating the TPEP provider that provided the record |

### Dimension Table: `dim_date`
| Column | Description |
| :--- | :--- |
| `date_key` | Primary key representing the calendar date |
| `full_date` | Standard calendar date |
| `year` / `month` / `day` | Numeric calendar components (year, month, day of month) |
| `month_name` / `day_name` | Full names of the month and day of the week |
| `day_of_week` | Numeric day of the week index |
| `is_weekend` | Boolean flag indicating whether the date falls on a weekend |

### Dimension Table: `dim_air_quality_sensors`
| Column | Description |
| :--- | :--- |
| `sensor_id` | Unique identifier of the individual measurement sensor |
| `location_id` | Unique identifier of the OpenAQ monitoring station |
| `location_name` | Human-readable name of the monitoring site |
| `country_code` / `country_name` | ISO country code and full country name |
| `latitude` / `longitude` | Geographic coordinates of the monitoring station |
| `parameter_name` | Standardized pollutant code (e.g., `pm25`, `pm10`, `no2`) |
| `parameter_display_name` | Display label for the measured pollutant |
| `parameter_units` | Unit of measurement for the pollutant concentration |
| `provider_name` | Organization or network providing the sensor data |
| `is_monitor` | Flag indicating whether the source is a stationary reference monitor |

### Dimension Table: `dim_gdp_yearly`
| Column | Description |
| :--- | :--- |
| `gdp_year` | Primary key representing the reference year |
| `country_iso3` / `country_name` | Three-letter ISO code (`USA`) and country name |
| `indicator_id` / `indicator_name` | World Bank indicator code (`NY.GDP.MKTP.CD`) and official description |
| `gdp_usd` | Nominal Gross Domestic Product in current US dollars |

---

## 4. Analytics & Key Insights (`UrbanAnalytics_Dashboard`)

The Power BI dashboard connects directly to `UrbanAnalytics_SemanticModel` and provides a cross-domain overview:
* **Currency-Adjusted Mobility Revenue:** Compares total gross taxi revenue in **USD ($74.66M)** against **EUR (€68.44M)** to highlight exchange-rate impact.
* **Intraday Demand Patterns:** Tracks hourly revenue and trip intensity across the 24-hour cycle, clearly identifying early-morning troughs and afternoon/evening rush-hour peaks.
* **Environmental Monitoring Coverage:** Ranks active OpenAQ monitoring locations by measured pollutant parameters (`PM2.5`, `PM10`, `NO2`, `O3`).
* **Macroeconomic Context:** Visualizes multi-decade US GDP growth alongside urban transport activity.

---

## 5. Data Governance & Automation

* **End-to-End Lineage:** Full upstream-to-downstream dependency tracking from external sources to Power BI visuals is maintained within Microsoft Fabric Lineage View.
* **Scheduled Orchestration:** The master pipeline `pl_master_e2e` supports automated scheduled refreshes across all Medallion layers without manual intervention.
