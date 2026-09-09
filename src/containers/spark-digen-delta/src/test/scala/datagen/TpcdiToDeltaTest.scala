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
}
