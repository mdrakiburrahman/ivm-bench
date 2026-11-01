package datagen

import java.time.LocalDate
import java.sql.Timestamp
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class TpcdiToDeltaTest extends AnyFunSuite {
  private def withSpark(appName: String)(test: SparkSession => Unit): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName(appName)
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    try test(spark)
    finally spark.stop()
  }

  test("augmented windows start where the Databricks workload starts") {
    assert(TpcdiToDelta.AugmentedStart == LocalDate.parse("2016-07-06"))
  }

  test("Batch 2 end is exclusive and Batch 3 starts on the following day") {
    assert(TpcdiToDelta.AugmentedEnd(37) == LocalDate.parse("2016-08-12"))
    assert(TpcdiToDelta.AugmentedEnd(183) == LocalDate.parse("2017-01-05"))
  }

  test("daily windows outside the Databricks horizon are rejected") {
    assertThrows[IllegalArgumentException] {
      TpcdiToDelta.AugmentedEnd(0)
    }
    assertThrows[IllegalArgumentException] {
      TpcdiToDelta.AugmentedEnd(365)
    }
  }

  test("customer updates create account events from the prior daily state") {
    withSpark("customer-account-update-test") { spark =>
      import spark.implicits._
      def ts(value: String): Timestamp = Timestamp.valueOf(value)

      val direct = Seq(
        ("I", 1L, 10L, 100L, 7L, "original", 1.toByte, "ACTV", ts("2016-07-01 09:00:00")),
        ("U", 2L, 10L, 100L, 7L, "same-day direct", 1.toByte, "ACTV", ts("2016-07-07 08:00:00")),
        ("I", 3L, 20L, 100L, 7L, "same-day new", 1.toByte, "ACTV", ts("2016-07-07 07:00:00")),
        ("I", 4L, 30L, 100L, 7L, "closed", 1.toByte, "INAC", ts("2016-07-05 09:00:00")),
      ).toDF(
        "cdc_flag", "cdc_dsn", "accountid", "ca_b_id", "ca_c_id",
        "accountdesc", "taxstatus", "ca_st_id", "action_ts",
      )
      val updates = Seq(
        (7L, ts("2016-07-07 12:00:00")),
      ).toDF("customerid", "action_ts")

      val result = TpcdiToDelta.AddCustomerAccountUpdates(direct, updates)
      assert(result.columns.toSeq == direct.columns.toSeq)

      val rows = result
        .select("cdc_flag", "accountid", "accountdesc", "ca_st_id", "action_ts")
        .as[(String, Long, String, String, Timestamp)]
        .collect()
        .toSet

      assert(rows == Set(
        ("I", 10L, "original", "ACTV", ts("2016-07-01 09:00:00")),
        ("I", 20L, "same-day new", "ACTV", ts("2016-07-07 07:00:00")),
        ("I", 30L, "closed", "INAC", ts("2016-07-05 09:00:00")),
        ("U", 10L, "original", "ACTV", ts("2016-07-07 12:00:00")),
        ("U", 30L, "closed", "INAC", ts("2016-07-07 12:00:00")),
      ))
    }
  }

  test("Spark appends generated columns by name") {
    withSpark("named-append-test") { spark =>
      import spark.implicits._
      val table = s"named_append_${System.nanoTime()}"
      try {
        spark.sql(s"CREATE TABLE $table (cdc_flag STRING, cdc_dsn BIGINT) USING parquet")
        spark.sql(s"INSERT INTO $table BY NAME SELECT 7L AS cdc_dsn, 'I' AS cdc_flag")
        assert(spark.table(table).as[(String, Long)].collect().toSeq == Seq(("I", 7L)))
      } finally {
        spark.sql(s"DROP TABLE IF EXISTS $table")
      }
    }
  }
  test("refresh row budgets compound current rows and round only at each cut") {
    assert(RepeatedRefresh.budgets(1000, 3, BigDecimal("1")) == Seq(10, 11, 11))
    assert(RepeatedRefresh.budgets(10000, 2, BigDecimal("10")) == Seq(1000, 1100))
    assert(RepeatedRefresh.budgets(1, 3, BigDecimal("0.01")) == Seq(1, 1, 1))
    assertThrows[IllegalArgumentException] { RepeatedRefresh.budgets(0, 1, BigDecimal(1)) }
    assertThrows[ArithmeticException] { RepeatedRefresh.budgets(Long.MaxValue, 1, BigDecimal(100)) }
  }

  test("both refresh workloads conserve events, retain order, and leave references static") {
    val spark = SparkSession.builder().master("local[1]").appName("repeated-refresh-test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.databricks.delta.snapshotPartitions", "1")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try {
      import spark.implicits._
      for (augmented <- Seq(false, true)) {
        val root = java.nio.file.Files.createTempDirectory("refresh-test").toFile
        val plan = new RepeatedRefresh(spark, root.getPath, augmented, 2, 3, BigDecimal(50))
        plan.add(Seq(1L, 2L, 3L, 4L).toDF("id"), "date", 1)
        val parents = Seq(("I", 1L, 10L), ("I", 2L, 20L)).toDF("cdc_flag", "cdc_dsn", "accountid")
        val trades = (1L to 8L).map(i => ("I", i, i, if (i < 5) 10L else 20L))
          .toDF("cdc_flag", "cdc_dsn", "t_id", "t_ca_id")
          .withColumn("t_dts", org.apache.spark.sql.functions.to_timestamp(
            org.apache.spark.sql.functions.from_unixtime(org.apache.spark.sql.functions.col("cdc_dsn") + 1499472000L)
          ))
        // Input partitions/order are intentionally shuffled.
        plan.addNative(parents.orderBy(org.apache.spark.sql.functions.col("cdc_dsn").desc), "account", 2)
        plan.addNative(trades.orderBy(org.apache.spark.sql.functions.col("cdc_dsn").desc), "trade", 2)
        plan.write()
        plan.write() // Reuse immutable payloads without another Delta version.
        assert(spark.read.format("delta").load(s"$root/batch1/date").count() == 4)
        val counts = (2 to 4).map { batch =>
          val account = spark.read.format("delta").load(s"$root/batch$batch/account")
          val trade = spark.read.format("delta").load(s"$root/batch$batch/trade")
          account.count() + trade.count()
        }
        assert(counts == Seq(2L, 3L, 5L))
        // Both parents arrive before any trades, even though refresh cuts a day.
        assert(spark.read.format("delta").load(s"$root/batch2/trade").count() == 0)
        val accountTimes = spark.read.format("delta").load(s"$root/batch2/account")
          .select("cdc_dsn").as[Long].collect()
        // Native sequence IDs 1/2 must become July 2017 action times, so an
        // update can never sort before the initial XML's historical actions.
        assert(accountTimes.forall(_ > 1490000000L))
        val ids = (2 to 4).flatMap { batch =>
          spark.read.format("delta").load(s"$root/batch$batch/trade")
            .select("t_id").as[Long].collect().sorted.toSeq
        }
        assert(ids == (1L to 8L))
        assert(!(new java.io.File(root, "batch2/date")).exists())
        assert(new java.io.File(root, "batch2/trade/_delta_log").listFiles()
          .count(_.getName.endsWith(".json")) == 1)
        val manifest = scala.io.Source.fromFile(new java.io.File(root, "refresh-plan.json"))
        try assert(manifest.mkString.contains("\"status\":\"ready\"")) finally manifest.close()
        val changed = new RepeatedRefresh(spark, root.getPath, augmented, 2, 4, BigDecimal(50))
        assertThrows[IllegalArgumentException] { changed.write() }
        assert(new java.io.File(root, "refresh-plan.json").delete())
        assertThrows[IllegalArgumentException] { plan.write() }
        val fs = new org.apache.hadoop.fs.Path(root.getPath).getFileSystem(spark.sparkContext.hadoopConfiguration)
        fs.delete(new org.apache.hadoop.fs.Path(root.getPath), true)
      }
    } finally spark.stop()
  }

  test("an insufficient reservoir reports measured sizing before writing any batches") {
    val spark = SparkSession.builder().master("local[1]").appName("refresh-sizing-test")
      .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try {
      import spark.implicits._
      val root = java.nio.file.Files.createTempDirectory("refresh-sizing-test").toFile
      val refresh = new RepeatedRefresh(spark, root.getPath, false, 2, 2, BigDecimal(10))
      refresh.add((1L to 100L).toDF("id"), "date", 1)
      refresh.addNative(Seq(("I", 1L, 10L), ("I", 2L, 20L))
        .toDF("cdc_flag", "cdc_dsn", "accountid"), "account", 2)
      refresh.write()
      val plan = new com.fasterxml.jackson.databind.ObjectMapper()
        .readTree(new java.io.File(root, "refresh-plan.json"))
      assert(plan.get("status").asText() == "needs_more_data")
      assert(plan.get("initial_rows").asLong() == 100)
      assert(plan.get("available_insert_rows").asLong() == 2)
      assert(plan.get("required_insert_rows").asLong() == 21)
      assert(plan.get("next_horizon").asInt() == 22)
      assert(!(new java.io.File(root, "batch1")).exists())
      val fs = new org.apache.hadoop.fs.Path(root.getPath).getFileSystem(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(root.getPath), true)
    } finally spark.stop()
  }

}
