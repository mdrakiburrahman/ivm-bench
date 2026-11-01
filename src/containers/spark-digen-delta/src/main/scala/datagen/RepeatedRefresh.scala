package datagen

import java.io.{File, PrintWriter}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.time.LocalDate
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable

/** Partition one generated stream without sampling, key rewriting, or replay.
  * Within a native day, parents (customers/accounts/trades) precede children.
  * CDC sequence order within each source is retained even across refresh cuts.
  */
class RepeatedRefresh(
    spark: SparkSession, deltaPath: String, augmented: Boolean,
    horizon: Int, refreshCount: Int, refreshPct: BigDecimal,
) {
  require(refreshCount >= 1 && refreshPct > 0)
  private val initial = mutable.LinkedHashMap.empty[String, DataFrame]
  private val streams = mutable.ArrayBuffer.empty[DataFrame]
  private val schemas = mutable.LinkedHashMap.empty[String, StructType]
  private val priorities = Map(
    "customer" -> 0, "account" -> 1, "batch_date" -> 2, "prospect" -> 3,
    "trade" -> 4, "holding_history" -> 5, "cash_transaction" -> 6,
    "watch_history" -> 7, "daily_market" -> 8,
  )
  // Category B starts empty; time is not loaded by any benchmark engine.
  private val unloadedInitial = Set("account", "customer", "batch_date", "time")
  private val nativeStartDate = LocalDate.parse("2017-07-08")
  private val nativeStart = java.time.temporal.ChronoUnit.DAYS.between(
    TpcdiToDelta.AugmentedStart, nativeStartDate,
  )

  def add(df: DataFrame, table: String, batch: Int): Unit = {
    if (batch == 1) {
      initial(table) = if (table == "audit") df.filter(col("batchid") <= 3) else df
    } else if (augmented) {
      addStream(df, table, datediff(
        to_date(from_unixtime(col("cdc_dsn"))), lit(TpcdiToDelta.AugmentedStart.toString),
      ))
    } else addNative(df, table, batch)
  }

  def addNative(df: DataFrame, table: String, batch: Int): Unit = {
    // The CRM models use epoch seconds for action timestamps, while native
    // CDC files carry sequence IDs. Preserve the original sequence for cuts;
    // give each key's actions their native day and within-day order in payloads.
    val date = nativeStartDate.plusDays(batch - 2L).toString
    val crmTimestamp = if (Set("customer", "account")(table)) {
      val key = if (table == "customer") "customerid" else "accountid"
      Some(unix_timestamp(lit(date).cast(TimestampType)) +
        row_number().over(Window.partitionBy(key).orderBy("cdc_dsn")) - 1)
    } else None
    val eventColumn = Map("trade" -> "t_dts", "cash_transaction" -> "ct_dts",
      "watch_history" -> "w_dts", "daily_market" -> "dm_date").get(table)
    val timestamp = crmTimestamp.orElse {
      if (augmented) Some(eventColumn.map(c => unix_timestamp(col(c)))
        .getOrElse(unix_timestamp(lit(date).cast(TimestampType)))) else None
    }
    addStream(df, table, lit((if (augmented) nativeStart else 0L) + batch - 2), timestamp)
  }

  private def addStream(
      df: DataFrame, table: String, day: Column, cdcTimestamp: Option[Column] = None,
  ): Unit = {
    require(priorities.contains(table), s"unexpected incremental table: $table")
    schemas.get(table).foreach(schema => require(schema.fields.map(f => (f.name, f.dataType)).toSeq ==
      df.schema.fields.map(f => (f.name, f.dataType)).toSeq, s"schema changed for $table"))
    schemas(table) = df.schema
    val sequence = if (df.columns.contains("cdc_dsn")) col("cdc_dsn").cast(LongType) else lit(0L)
    val payloadColumns = df.columns.map { name =>
      if (name == "cdc_dsn" && cdcTimestamp.nonEmpty) cdcTimestamp.get.cast(LongType).alias(name)
      else col(name)
    }
    streams += df.select(
      day.cast(LongType).alias("day"), lit(priorities(table)).alias("priority"),
      sequence.alias("sequence"), lit(table).alias("table"),
      to_json(struct(payloadColumns: _*)).alias("payload"),
    )
  }

  private def save(df: DataFrame, batch: Int, table: String): Unit = {
    val path = if (table == "audit" && batch == 1) s"$deltaPath/audit"
      else s"$deltaPath/batch$batch/$table"
    df.write.format("delta").option("delta.enableChangeDataFeed", "true")
      .mode("overwrite").save(path)
  }

  private def manifest(value: Any): Unit = {
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    mapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
    new File(deltaPath).mkdirs()
    val writer = new PrintWriter(s"$deltaPath/refresh-plan.json")
    try writer.println(mapper.writeValueAsString(value)) finally writer.close()
  }

  def write(): Unit = {
    val planFile = new File(deltaPath, "refresh-plan.json")
    if (planFile.isFile) {
      val plan = new com.fasterxml.jackson.databind.ObjectMapper().readTree(planFile)
      if (plan.get("status").asText() == "ready") {
        require(plan.get("workload").asText() == (if (augmented) "databricks" else "standard") &&
          plan.get("refresh_count").asInt() == refreshCount &&
          BigDecimal(plan.get("refresh_pct").asText()) == refreshPct,
          "refresh configuration changed; use a fresh DELTA_PATH")
        // DuckLake loads physical Parquet files, so Delta overwrite would
        // leave old inactive files readable as duplicate rows. Reuse the
        // complete immutable payload instead of writing another version.
        println("=== SKIP: repeated refresh payload already complete ===")
        return
      }
    }
    require(!new File(deltaPath, "batch1").exists(),
      "incomplete or different generated payload exists; use a fresh DELTA_PATH")
    val initialCounts = initial.toSeq.map { case (table, df) =>
      table -> (if (unloadedInitial(table)) 0L else df.count())
    }.toMap
    val initialRows = initialCounts.values.sum
    val budgets = RepeatedRefresh.budgets(initialRows, refreshCount, refreshPct)
    val needed = budgets.foldLeft(0L)(Math.addExact)
    require(streams.nonEmpty, "generator produced no incremental tables")
    val pool = streams.reduce(_.unionByName(_)).persist(StorageLevel.DISK_ONLY)
    try {
      val available = pool.count()
      val base = Map[String, Any](
        "workload" -> (if (augmented) "databricks" else "standard"),
        "refresh_count" -> refreshCount, "refresh_pct" -> refreshPct.toString,
        "growth_target" -> (BigDecimal(1) + refreshPct / 100).pow(refreshCount).toString,
        "generator_incremental_batches" -> horizon,
        "initial_rows" -> initialRows, "initial_tables" -> initialCounts,
        "available_insert_rows" -> available, "required_insert_rows" -> needed,
      )
      if (available < needed) {
        // Use actual native workload density to estimate, then remeasure.
        val nativeRows = pool.filter(col("day") >= (if (augmented) nativeStart else 0L)).count()
        require(nativeRows > 0, "generator produced no native incremental rows")
        val extra = (BigDecimal(needed - available) * horizon / nativeRows)
          .setScale(0, BigDecimal.RoundingMode.CEILING).bigDecimal.intValueExact() + 1
        manifest(base ++ Map("status" -> "needs_more_data", "next_horizon" -> (horizon + extra)))
        println(s"=== Reservoir insufficient: $available available, $needed required ===")
        return
      }
      // Distributed 64-bit positions; no single-partition row_number window.
      val sorted = pool.orderBy("day", "priority", "sequence", "payload")
      val rankSchema = StructType(pool.schema.fields :+ StructField("position", LongType, false))
      val ranked = spark.createDataFrame(
        sorted.rdd.zipWithIndex().map { case (row, index) => Row.fromSeq(row.toSeq :+ index) }, rankSchema,
      ).persist(StorageLevel.DISK_ONLY)
      try {
        ranked.count()
        initial.foreach { case (table, df) => save(df, 1, table) }
        var current = initialCounts
        var offset = 0L
        val rounds = budgets.zipWithIndex.map { case (budget, index) =>
          val slice = ranked.filter(col("position") >= offset && col("position") < offset + budget)
          val inserts = slice.groupBy("table").count().collect()
            .map(row => row.getString(0) -> row.getLong(1)).toMap
          schemas.foreach { case (table, schema) =>
            val rows = slice.filter(col("table") === table)
              .select(from_json(col("payload"), schema).alias("row")).select("row.*")
            // Empty tables are required by cloud loaders for every payload.
            save(rows, index + 2, table)
          }
          val before = current
          current = (initialCounts.keySet ++ schemas.keySet).map { table =>
            table -> (current.getOrElse(table, 0L) + inserts.getOrElse(table, 0L))
          }.toMap
          val tables = current.keys.toSeq.sorted.map { table =>
            val counts = Map(
              "initial_rows" -> initialCounts.getOrElse(table, 0L),
              "before_rows" -> before.getOrElse(table, 0L),
              "inserted_rows" -> inserts.getOrElse(table, 0L),
              "resulting_rows" -> current(table),
            )
            println(s"[refresh] round=${index + 1} table=$table initial=${counts("initial_rows")} " +
              s"before=${counts("before_rows")} inserted=${counts("inserted_rows")} resulting=${counts("resulting_rows")}")
            table -> counts
          }.toMap
          offset += budget
          Map[String, Any]("round" -> (index + 1), "batch_num" -> (index + 2),
            "before_rows" -> before.values.sum, "inserted_rows" -> budget,
            "resulting_rows" -> current.values.sum, "tables" -> tables)
        }
        manifest(base ++ Map("status" -> "ready", "rounds" -> rounds))
      } finally ranked.unpersist()
    } finally pool.unpersist()
  }
}

object RepeatedRefresh {
  /** Round up to whole rows once per refresh, then compound actual source rows.
    * Static tables contribute to the denominator and never receive inserts.
    */
  def budgets(initialRows: Long, refreshCount: Int, pct: BigDecimal): Seq[Long] = {
    require(initialRows > 0 && refreshCount >= 1 && pct > 0)
    var current = initialRows
    (1 to refreshCount).map { _ =>
      val insert = (BigDecimal(current) * pct / 100)
        .setScale(0, BigDecimal.RoundingMode.CEILING).bigDecimal.longValueExact()
      current = Math.addExact(current, insert)
      insert
    }
  }
}
