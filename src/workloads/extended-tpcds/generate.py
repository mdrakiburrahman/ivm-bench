"""Prepare an Extended TPC-DS dataset from an existing DuckDB TPC-DS database."""

import argparse
import hashlib
import json
import math
from datetime import date
from pathlib import Path

import duckdb

HERE = Path(__file__).resolve().parent


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def prepare(connection, base_schema, profile="native", seed=17, report_date=None,
            history_months=12, accounts_per_customer=2, metadata_versions=2,
            base_scale_factor=None, base_generator=None):
    if profile not in ("native", "calibrated", "jcc_stress"):
        raise ValueError("unknown profile")
    if any(type(value) is not int or value < 1 for value in
           [history_months, accounts_per_customer, metadata_versions]):
        raise ValueError("history, accounts and metadata versions must be positive")
    if base_scale_factor is not None and (not math.isfinite(base_scale_factor) or base_scale_factor <= 0):
        raise ValueError("base scale factor must be positive and finite")
    if not all(part.isidentifier() for part in base_schema.split(".")):
        raise ValueError("base schema must contain SQL identifiers")
    if connection.execute("SELECT count(*) FROM information_schema.tables WHERE table_name='xt_run'").fetchone()[0]:
        raise ValueError("this database already contains an Extended TPC-DS dataset")

    def execute(sql):
        connection.execute(sql)

    def random_fraction(key):
        # MD5 and the versioned key encoding keep assignments independent of
        # input order, parallelism and DuckDB's implementation of hash().
        return f"(CAST('0x' || substr(md5({quote(seed)} || ':' || {key}), 1, 8) AS UBIGINT)::DOUBLE / 4294967296.0)"

    channels = [("store", "store_sales", "ss", "customer_sk", "ticket_number"),
                ("catalog", "catalog_sales", "cs", "bill_customer_sk", "order_number"),
                ("web", "web_sales", "ws", "bill_customer_sk", "order_number")]
    projections = []
    for channel, table, prefix, customer, order in channels:
        projections.append(f"""
            SELECT {quote(channel)} AS channel,
                   {quote(channel)} || ':' || s.{prefix}_{order}::VARCHAR || ':' || s.{prefix}_item_sk::VARCHAR AS purchase_line_id,
                   s.{prefix}_{order}::VARCHAR AS order_id,
                   s.{prefix}_item_sk AS source_item_sk,
                   coalesce(c.c_customer_id, 'unknown') AS customer_id,
                   coalesce(i.i_item_id, 'unknown') AS item_id,
                   d.d_date + coalesce(t.t_time, 0) * INTERVAL 1 SECOND AS event_time,
                   s.{prefix}_quantity AS purchase_quantity,
                   s.{prefix}_sales_price AS original_sales_price,
                   c.c_customer_sk IS NULL AS missing_customer,
                   i.i_item_sk IS NULL AS missing_item,
                   d.d_date IS NULL AS missing_date,
                   coalesce(i.i_category, 'unknown') AS category
            FROM {base_schema}.{table} s
            LEFT JOIN {base_schema}.customer c ON s.{prefix}_{customer}=c.c_customer_sk
            LEFT JOIN {base_schema}.item i ON s.{prefix}_item_sk=i.i_item_sk
            LEFT JOIN {base_schema}.date_dim d ON s.{prefix}_sold_date_sk=d.d_date_sk
            LEFT JOIN {base_schema}.time_dim t ON s.{prefix}_sold_time_sk=t.t_time_sk
        """)
    execute("CREATE TABLE normalized_purchase AS " + " UNION ALL ".join(projections))
    count, distinct = connection.execute("SELECT count(*), count(DISTINCT purchase_line_id) FROM normalized_purchase").fetchone()
    if count != distinct:
        raise ValueError("TPC-DS sales line identities are not unique")
    if report_date is None:
        report_date = connection.execute("SELECT max(event_time)::DATE FROM normalized_purchase").fetchone()[0]
    if report_date is None:
        raise ValueError("TPC-DS input has no dated purchases")
    report_date = date.fromisoformat(str(report_date))
    clock = f"DATE {quote(report_date)}"
    execute("CREATE TABLE purchase_quarantine AS SELECT *, 'missing_date' AS reason FROM normalized_purchase WHERE missing_date")
    execute(f"""CREATE TABLE xt_customers AS
        SELECT *, '/customers/' || lower(customer_id) AS customer_path,
               CASE WHEN customer_id='unknown' THEN -2147483648 ELSE (customer_number - 1) // 2 + 1 END AS household_id,
               CASE WHEN customer_id='unknown' THEN 'unknown'
                    WHEN ((customer_number - 1) // 2 + 1) % 2 = 0 THEN 'hot' ELSE 'cold' END AS household_cohort
        FROM (SELECT customer_id, row_number() OVER (ORDER BY md5({quote(seed)} || ':' || customer_id), customer_id) AS customer_number
              FROM (SELECT c_customer_id AS customer_id FROM {base_schema}.customer
                    UNION SELECT 'unknown')) c""")
    execute(f"""CREATE TABLE xt_accounts AS
        SELECT c.*, 'account:' || lower(customer_id) || ':' || slot::VARCHAR AS account_id
        FROM xt_customers c CROSS JOIN range({accounts_per_customer}) s(slot)""")
    slot = f"floor({random_fraction('p.channel || p.order_id')} * {accounts_per_customer})::INTEGER"
    execute(f"""CREATE TABLE xt_purchase AS
        SELECT p.*, a.account_id, a.customer_path, a.household_id, a.household_cohort,
               '/accounts/' || a.account_id || '/engagements/' || lower(p.item_id) AS engagement_path,
               md5(a.account_id || ':' || lower(p.item_id)) AS engagement_registry_key
        FROM normalized_purchase p
        JOIN xt_accounts a ON p.customer_id=a.customer_id AND a.account_id='account:' || lower(p.customer_id) || ':' || {slot}
        WHERE NOT p.missing_date""")
    execute(f"""CREATE TABLE xt_engagements AS
        SELECT *, row_number() OVER (ORDER BY md5({quote(seed)} || ':' || engagement_path), engagement_path) AS engagement_number
        FROM (SELECT engagement_path, engagement_registry_key, account_id, customer_path, household_id, household_cohort,
                     item_id, min(purchase_line_id) AS origin_purchase_line_id, min(event_time) AS first_event,
                     max(event_time) AS last_event, count(*) AS purchase_lines
              FROM xt_purchase GROUP BY ALL) e""")
    references = json.loads((HERE / "profiles.json").read_text())
    targets = {r["metric"]: r["value"] for r in references["measured_reference_targets"]}
    entity_count = connection.execute("SELECT count(*) FROM xt_engagements").fetchone()[0]
    eligible_count = math.ceil(entity_count * targets["purchase_relationship_metadata_fraction"]) if profile == "calibrated" else entity_count
    execute(f"ALTER TABLE xt_engagements ADD COLUMN eligible_relationship BOOLEAN DEFAULT FALSE")
    execute(f"UPDATE xt_engagements SET eligible_relationship=(engagement_number <= {eligible_count})")
    if profile == "calibrated":
        density = targets["status_events_per_engagement_collection_key"]
        base_events = math.floor(density)
        extra_entities = math.floor(entity_count * (density - base_events))
        event_day = f"{clock} - floor({random_fraction('e.engagement_path || v.n::VARCHAR')} * 365)::INTEGER"
        execute(f"""CREATE TABLE xt_status_events AS
            SELECT 'synthetic:' || e.engagement_registry_key || ':' || v.n::VARCHAR AS event_id,
                   e.engagement_registry_key, {event_day} + INTERVAL 14 HOUR AS event_time,
                   e.origin_purchase_line_id, 'synthetic_calibrated' AS origin
            FROM xt_engagements e CROSS JOIN range({base_events + 1}) v(n)
            WHERE v.n < {base_events} OR (v.n={base_events} AND e.engagement_number <= {extra_entities})""")
    else:
        execute("""CREATE TABLE xt_status_events AS
            SELECT purchase_line_id AS event_id, engagement_registry_key, event_time,
                   purchase_line_id AS origin_purchase_line_id, 'tpcds_purchase' AS origin FROM xt_purchase""")
    if profile == "jcc_stress":
        hot = connection.execute("""SELECT e.engagement_registry_key FROM xt_engagements e
            JOIN xt_customers c USING(customer_path)
            WHERE e.household_cohort='hot' AND e.engagement_number%10<>0 AND c.customer_number%20<>0
            ORDER BY engagement_number LIMIT 5""").fetchall()
        if len(hot) != 5:
            raise ValueError("JCC stress requires five hot engagements and both household cohorts")
        execute("CREATE TEMP TABLE hot_engagements (n INTEGER, registry_key VARCHAR)")
        connection.executemany("INSERT INTO hot_engagements VALUES (?, ?)", list(enumerate(x[0] for x in hot)))
        execute(f"""CREATE TEMP TABLE reassigned AS
            SELECT e.event_id, h.registry_key
            FROM (SELECT event_id, row_number() OVER (ORDER BY md5({quote(seed)} || ':' || event_id), event_id) AS n FROM xt_status_events) e
            JOIN hot_engagements h ON (e.n-1)%5=h.n
            WHERE e.n <= (SELECT floor(count(*) * 0.25) FROM xt_status_events)""")
        execute("UPDATE xt_status_events e SET engagement_registry_key=r.registry_key, origin='synthetic_jcc_assignment' FROM reassigned r WHERE e.event_id=r.event_id")

    schema = json.loads((HERE / "source-schema.json").read_text())

    def source(name, sql):
        columns = ", ".join(f"CAST({column} AS {kind}) AS {column}" for column, kind in schema[name].items())
        execute(f"CREATE TABLE {name} AS SELECT md5({quote(name)} || ':' || row_key) AS _source_row_id, {columns} FROM ({sql}) generated")
        total, unique = connection.execute(f"SELECT count(*), count(DISTINCT _source_row_id) FROM {name}").fetchone()
        if total != unique:
            raise ValueError(f"duplicate physical identities in {name}")

    # History rows are real extension inputs; staging retains only current rows.
    versions = f"range({metadata_versions}) versions(version)"
    effective = f"{clock} - ({metadata_versions}-1-version)*INTERVAL 30 DAY"
    current = f"version={metadata_versions}-1"
    region = "'retail' AS region_partition"
    source("ext_entity_registry", f"""
        SELECT customer_path || ':' || version AS row_key, md5(customer_path) AS registry_key,
               'account:' || lower(customer_id) || ':0' AS account_id, customer_path AS entity_path,
               'customers' AS entity_type, {effective} AS effective_start, {region}
        FROM xt_customers CROSS JOIN {versions}
        UNION ALL
        SELECT engagement_path || ':' || version, engagement_registry_key, account_id, engagement_path,
               'engagements', {effective}, 'retail' FROM xt_engagements CROSS JOIN {versions}
        UNION ALL
        SELECT 'line:' || purchase_line_id, md5('line:' || purchase_line_id), account_id,
               engagement_path || '/order-lines/' || md5(purchase_line_id), 'order_lines', event_time, 'retail'
        FROM xt_purchase""")
    source("ext_account_profile", f"""SELECT account_id || ':' || version AS row_key, account_id,
        'household:' || household_id AS loyalty_household_id, household_id,
        {effective} AS effective_start, {effective} AS ingestion_time,
        CASE WHEN customer_number%2=0 THEN 'Member' ELSE 'Guest' END AS offer_type, 'Shopping' AS usage_type,
        household_cohort AS household_type, 'Retail' AS region_group, 'Area' AS sales_area, 'US' AS country_name,
        'Retail' AS household_category, 'Consumer' AS household_segment, 'General' AS household_subcategory,
        'Retail' AS household_vertical, household_id::VARCHAR AS membership_number,
        'Member ' || household_id AS membership_name, household_id%10=0 AS is_priority,
        'Household ' || household_id AS household_name_translated, NULL AS household_name_raw,
        NULL AS household_name_secondary, 'Household ' || household_id AS household_name,
        {current} AS is_current FROM xt_accounts CROSS JOIN {versions}""")
    source("ext_account_household", "SELECT account_id AS row_key, account_id, 'household:' || household_id AS loyalty_household_id FROM xt_accounts")
    source("ext_calendar", f"""SELECT d_date_sk::VARCHAR AS row_key, strftime(d_date,'%Y%m%d') AS dim_date_id,
        d_date AS date, year(d_date) AS year, month(d_date) AS month, day(d_date) AS day,
        'th' AS day_suffix, isodow(d_date) AS week_day, dayname(d_date) AS week_day_name,
        ceil(day(d_date)/7.0) AS dow_in_month, dayofyear(d_date) AS day_of_year,
        ceil(day(d_date)/7.0) AS week_of_month, week(d_date) AS week_of_year, week(d_date) AS iso_week_of_year,
        monthname(d_date) AS month_name, quarter(d_date) AS quarter, 'Q' || quarter(d_date) AS quarter_name,
        strftime(d_date,'%m%Y') AS mmyyyy, strftime(d_date,'%Y-%m') AS month_year,
        date_trunc('month',d_date) AS first_day_of_month, last_day(d_date) AS last_day_of_month,
        date_trunc('quarter',d_date) AS first_day_of_quarter,
        date_trunc('quarter',d_date)+INTERVAL 3 MONTH-INTERVAL 1 DAY AS last_day_of_quarter,
        date_trunc('year',d_date) AS first_day_of_year,
        date_trunc('year',d_date)+INTERVAL 1 YEAR-INTERVAL 1 DAY AS last_day_of_year,
        date_trunc('month',d_date)+INTERVAL 1 MONTH AS first_day_of_next_month,
        date_trunc('year',d_date)+INTERVAL 1 YEAR AS first_day_of_next_year
        FROM {base_schema}.date_dim WHERE d_date IS NOT NULL""")
    # Preserve raw decimal prices in normalized_purchase. Catalog text exercises
    # the original parsing shape without rewriting purchase amounts.
    catalog = {column: "'Retail'" for column in schema["ext_price_catalog"]}
    catalog.update(price_catalog_id="i_item_id", catalog_size="'1'", catalog_price="max(i_current_price)::VARCHAR",
                   catalog_consumption_multiplier="'1'", catalog_average_revenue_per_unit="max(i_current_price)::VARCHAR",
                   catalog_currency="'USD'", catalog_unit_of_measure="'Item'", catalog_description="max(i_item_desc)")
    source("ext_price_catalog", "SELECT i_item_id AS row_key, " + ",".join(f"{value} AS {key}" for key, value in catalog.items()) + f" FROM {base_schema}.item GROUP BY i_item_id")
    source("ext_purchase_usage_events", """SELECT purchase_line_id AS row_key, engagement_registry_key AS registry_key,
        strftime(event_time,'%Y%m%d') AS purchase_date_key, date_diff('second',date_trunc('day',event_time),event_time)::VARCHAR AS purchase_second_key,
        item_id AS price_catalog_id, purchase_quantity, strftime(event_time,'%Y%m') AS event_month,
        'retail' AS region_partition FROM xt_purchase""")
    metadata = f"xt_engagements e CROSS JOIN {versions}"
    common = f"{effective} AS effective_start, {current} AS is_current, {region}"
    source("ext_engagement_capacity_history", f"""SELECT engagement_path || ':' || version AS row_key,
        engagement_path, customer_path, CASE WHEN eligible_relationship THEN 'purchase' ELSE 'interest' END AS relationship_type,
        1+engagement_number%16 AS reported_units, 32-engagement_number%16 AS allocated_units, {common}
        FROM {metadata} WHERE engagement_number%10<>0""")
    source("ext_engagement_activity_history", f"""SELECT engagement_path || ':' || version AS row_key,
        engagement_path, CASE WHEN engagement_number%2=0 THEN 'observed' ELSE 'reported' END AS activity_mode,
        {common} FROM {metadata}""")
    source("ext_engagement_service_history", f"SELECT engagement_path || ':' || version AS row_key, engagement_path, CASE WHEN engagement_number%3=0 THEN NULL ELSE 'standard' END AS service_role, {common} FROM {metadata}")
    source("ext_engagement_product_history", f"""SELECT engagement_path || ':' || version AS row_key, engagement_path,
        'V' || (1+engagement_number%3) AS product_version,
        CASE WHEN engagement_number%3=0 THEN 'BASIC' WHEN engagement_number%3=1 THEN 'STANDARD' ELSE 'PREMIUM' END AS product_tier,
        {common} FROM {metadata}""")
    assessment_probability = targets["channel_assessment_nonnull_fraction"] if profile == "calibrated" else 0.75
    assessment = {}
    for index, channel in enumerate(["store", "catalog", "web"]):
        probability = targets["third_channel_assessment_nonnull_fraction"] if profile == "calibrated" and channel == "web" else assessment_probability
        assessment[channel] = f"CASE WHEN {random_fraction('engagement_path || ' + quote(channel))} < {probability} THEN CASE WHEN (engagement_number+{index})%3=0 THEN 'notready' ELSE 'ready' END ELSE NULL END"
    source("ext_engagement_channel_history", f"""SELECT engagement_path || ':' || version AS row_key,
        md5('assessment:' || engagement_path) AS assessment_key,
        {assessment['store']} AS store_eligibility, {assessment['catalog']} AS catalog_eligibility, {assessment['web']} AS web_eligibility,
        {effective} AS ingestion_time, {common} FROM {metadata}""")
    source("ext_engagement_status_events", """SELECT event_id AS row_key, s.engagement_registry_key,
        strftime(event_time,'%Y%m%d') AS event_date_key,
        date_diff('second',date_trunc('day',event_time),event_time)::VARCHAR AS event_second_key,
        event_time AS observation_time, event_time+INTERVAL 1 HOUR AS ingestion_time,
        md5('assessment:' || e.engagement_path) AS assessment_key,
        strftime(event_time,'%Y%m%d') AS event_date, strftime(event_time,'%Y%m') AS event_month, 'retail' AS region_partition
        FROM xt_status_events s JOIN xt_engagements e USING (engagement_registry_key)""")
    parent_metadata = f"xt_customers CROSS JOIN {versions}"
    source("ext_customer_profile_history", f"""SELECT customer_path || ':' || version AS row_key, customer_path,
        'Retail' AS platform_name, 'Retail' AS reported_platform,
        1+customer_number%16 AS parent_capacity, 2*(1+customer_number%16) AS parent_limit,
        'Consumer' AS customer_segment, 'General' AS customer_segment_detail,
        TRUE AS store_active, customer_number%2=0 AS catalog_active, customer_number%3=0 AS web_active,
        {effective} AS ingestion_time, {common} FROM {parent_metadata}""")
    source("ext_customer_preference_history", f"""SELECT customer_path || ':' || version AS row_key, customer_path,
        CASE WHEN customer_number%2=0 THEN 'Loyalty Member' ELSE 'Guest Shopper' END AS preference_sku,
        'retail' AS preference_type, {effective} AS ingestion_time, {common} FROM {parent_metadata}""")
    source("ext_customer_policy_history", f"""SELECT customer_path || ':' || version AS row_key, customer_path,
        CASE WHEN customer_number%3=0 THEN NULL ELSE 'standard' END AS policy_type,
        {effective} AS ingestion_time, {common} FROM {parent_metadata}""")
    source("ext_customer_status_events", """SELECT p.purchase_line_id AS row_key, md5(p.customer_path) AS customer_registry_key,
        strftime(p.event_time,'%Y%m%d') AS event_date_key,
        date_diff('second',date_trunc('day',p.event_time),p.event_time)::VARCHAR AS event_second_key,
        CASE WHEN c.customer_number%20=0 THEN 'inactive' ELSE 'active' END AS customer_status,
        p.event_time+INTERVAL 1 HOUR AS ingestion_time, strftime(p.event_time,'%Y%m') AS event_month,
        'retail' AS region_partition FROM xt_purchase p JOIN xt_customers c USING(customer_path)""")
    source("ext_purchase_receipt_events", """SELECT purchase_line_id AS row_key, event_time AS receipt_time, engagement_path,
        to_json(struct_pack(CustomerPath := customer_path, ReportedUnits := purchase_quantity)) AS receipt_payload,
        'retail' AS region, strftime(event_time,'%Y%m') AS event_month FROM xt_purchase""")
    source("ext_engagement_observation_events", """SELECT purchase_line_id AS row_key, event_time AS observation_time,
        engagement_path, customer_path, purchase_quantity::VARCHAR AS reported_units,
        'retail' AS region, strftime(event_time,'%Y%m') AS event_month FROM xt_purchase""")
    base_signature = {}
    for table in ["customer", "item", "date_dim", "time_dim", "store_sales", "catalog_sales", "web_sales"]:
        rows, checksum = connection.execute(f"SELECT count(*), bit_xor(md5_number(to_json(t)))::VARCHAR FROM {base_schema}.{table} t").fetchone()
        base_signature[table] = {"rows": rows, "row_checksum_xor": checksum}
    configuration = {"generator_version": 1, "duckdb_version": duckdb.__version__, "profile": profile,
                     "base_scale_factor": base_scale_factor, "base_generator": base_generator,
                     "base_signature": base_signature,
                     "seed": seed, "report_date": str(report_date), "history_months": history_months,
                     "accounts_per_customer": accounts_per_customer, "household_size": 2,
                     "metadata_versions": metadata_versions, "base_rows_transformed": False,
                     "missing_time_policy": "midnight when time lookup fails or t_time is null",
                     "primary_metadata_omission": {"origin": "chosen_fallback_scenario", "rule": "every tenth engagement"},
                     "status_date_distribution": "seeded_uniform_365_days_at_14h" if profile == "calibrated" else "purchase_timestamps",
                     "channel_assessment_probabilities": {
                         channel: targets["third_channel_assessment_nonnull_fraction" if channel == "web" else "channel_assessment_nonnull_fraction"] if profile == "calibrated" else 0.75
                         for channel in ["store", "catalog", "web"]},
                     "implemented_calibration_targets": ["status_events_per_engagement_collection_key",
                        "purchase_relationship_metadata_fraction", "channel_assessment_nonnull_fraction",
                        "third_channel_assessment_nonnull_fraction"] if profile == "calibrated" else [],
                     "stress_assignment": {"hot_keys": 5, "event_share": 0.25} if profile == "jcc_stress" else None}
    configuration["calibration_target_values"] = {
        metric: targets[metric] for metric in configuration["implemented_calibration_targets"]}
    configuration["dataset_id"] = hashlib.sha256(json.dumps(configuration, sort_keys=True).encode()).hexdigest()[:16]
    execute("CREATE TABLE xt_run (configuration JSON)")
    connection.execute("INSERT INTO xt_run VALUES (?)", [json.dumps(configuration)])
    return configuration


def statistics(connection):
    sources = json.loads((HERE / "source-schema.json").read_text())
    tables = {table: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0] for table in sources}
    normalized = connection.execute("SELECT count(*), sum(missing_customer)::BIGINT, sum(missing_item)::BIGINT, sum(missing_date)::BIGINT FROM normalized_purchase").fetchone()
    status_rows, keys = connection.execute("SELECT count(*), count(DISTINCT engagement_registry_key) FROM xt_status_events").fetchone()
    cohorts = connection.execute("""SELECT e.household_cohort, count(*) AS events, count(DISTINCT e.engagement_registry_key) AS keys
        FROM xt_status_events s JOIN xt_engagements e USING(engagement_registry_key) GROUP BY 1 ORDER BY 1""").fetchall()
    hot_share = connection.execute("SELECT count(*) FILTER (WHERE origin='synthetic_jcc_assignment')::DOUBLE / nullif(count(*),0) FROM xt_status_events").fetchone()[0]
    config = json.loads(connection.execute("SELECT configuration FROM xt_run").fetchone()[0])
    references = json.loads((HERE / "profiles.json").read_text())["measured_reference_targets"]
    metadata_fraction = connection.execute("SELECT count(*) FILTER(WHERE relationship_type='purchase')::DOUBLE/nullif(count(*),0) FROM ext_engagement_capacity_history WHERE is_current").fetchone()[0]
    channel_fractions = connection.execute("SELECT count(store_eligibility)::DOUBLE/nullif(count(*),0), count(catalog_eligibility)::DOUBLE/nullif(count(*),0), count(web_eligibility)::DOUBLE/nullif(count(*),0) FROM ext_engagement_channel_history WHERE is_current").fetchone()
    observed = {"status_events_per_engagement_collection_key": status_rows / keys if keys else None,
                "purchase_relationship_metadata_fraction": metadata_fraction,
                "channel_assessment_nonnull_fraction": {"store": channel_fractions[0], "catalog": channel_fractions[1]},
                "third_channel_assessment_nonnull_fraction": channel_fractions[2]}
    calibration = {entry["metric"]: {"target": entry["value"], "observed": observed.get(entry["metric"]),
                   "implemented": entry["metric"] in config["implemented_calibration_targets"],
                   "reference_population": entry["population"]} for entry in references}
    return {"extension_rows": tables, "calibration": calibration,
            "normalized_purchase_rows": normalized[0], "missing_customer_rows": normalized[1],
            "missing_item_rows": normalized[2], "quarantined_missing_date_rows": normalized[3],
            "status_event_rows": status_rows, "status_registry_keys": keys,
            "status_events_per_registry_key": status_rows / keys if keys else None,
            "cohorts": [{"cohort": c, "events": n, "registry_keys": k} for c, n, k in cohorts],
            "reassigned_stress_event_share": hot_share}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-db", required=True, type=Path)
    parser.add_argument("--output-db", required=True, type=Path)
    parser.add_argument("--profile", choices=["native", "calibrated", "jcc_stress"], default="native")
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--report-date", type=date.fromisoformat)
    parser.add_argument("--history-months", type=int, default=12)
    parser.add_argument("--base-scale-factor", type=float, help="record the scale used to generate the input")
    parser.add_argument("--base-generator", help="record the input generator and version; unknown if omitted")
    args = parser.parse_args()
    if not args.input_db.is_file() or args.output_db.exists():
        parser.error("input database must exist and output database must be a new path")
    with duckdb.connect(str(args.output_db)) as connection:
        connection.execute("ATTACH " + quote(args.input_db.resolve()) + " AS raw (READ_ONLY)")
        connection.execute("BEGIN")
        try:
            config = prepare(connection, "raw.main", args.profile, args.seed, args.report_date, args.history_months,
                             base_scale_factor=args.base_scale_factor, base_generator=args.base_generator)
            achieved = statistics(connection)
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
    artifact = {"configuration": config, "achieved": achieved}
    args.output_db.with_suffix(".generation.json").write_text(json.dumps(artifact, indent=2) + "\n")
    print(json.dumps(artifact, indent=2))


if __name__ == "__main__":
    main()
