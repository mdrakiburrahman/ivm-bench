package datagen

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.io.File

object TpcdiToDelta {

  private def deltaExists(path: String): Boolean =
    new File(s"$path/_delta_log").isDirectory

  private def sourceExists(path: String): Boolean =
    new File(path).exists()

  def main(args: Array[String]): Unit = {
    val digenPath = sys.env.getOrElse("DIGEN_PATH", "/data/digen")
    val deltaPath = sys.env.getOrElse("DELTA_PATH", "/data/delta")

    val spark = SparkSession.builder()
      .appName("TpcdiToDelta")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    var written = 0
    var skipped = 0

    def writeDelta(df: DataFrame, outPath: String, label: String): Unit = {
      if (deltaExists(outPath)) {
        println(s"  SKIP: $label — Delta table already exists")
        skipped += 1
      } else {
        println(s"  WRITE: $label → $outPath")
        df.write.format("delta")
          .option("delta.enableChangeDataFeed", "true")
          .mode("overwrite")
          .save(outPath)
        println(s"  DONE: $label")
        written += 1
      }
    }

    def readCsv(path: String, schema: StructType, delimiter: String = "|"): DataFrame =
      spark.read
        .option("header", "false")
        .option("delimiter", delimiter)
        .schema(schema)
        .csv(path)

    // ════════════════════════════════════════════════════════════════════════
    // Reference tables — Batch1 only
    // ════════════════════════════════════════════════════════════════════════
    println("=== Processing reference tables (Batch1) ===")

    val dateSchema = new StructType()
      .add("sk_dateid", LongType)
      .add("datevalue", DateType)
      .add("datedesc", StringType)
      .add("calendaryearid", IntegerType)
      .add("calendaryeardesc", StringType)
      .add("calendarqtrid", IntegerType)
      .add("calendarqtrdesc", StringType)
      .add("calendarmonthid", IntegerType)
      .add("calendarmonthdesc", StringType)
      .add("calendarweekid", IntegerType)
      .add("calendarweekdesc", StringType)
      .add("dayofweeknum", IntegerType)
      .add("dayofweekdesc", StringType)
      .add("fiscalyearid", IntegerType)
      .add("fiscalyeardesc", StringType)
      .add("fiscalqtrid", IntegerType)
      .add("fiscalqtrdesc", StringType)
      .add("holidayflag", BooleanType)

    val timeSchema = new StructType()
      .add("sk_timeid", LongType)
      .add("timevalue", StringType)
      .add("hourid", IntegerType)
      .add("hourdesc", StringType)
      .add("minuteid", IntegerType)
      .add("minutedesc", StringType)
      .add("secondid", IntegerType)
      .add("seconddesc", StringType)
      .add("markethoursflag", BooleanType)
      .add("officehoursflag", BooleanType)

    val industrySchema = new StructType()
      .add("in_id", StringType)
      .add("in_name", StringType)
      .add("in_sc_id", StringType)

    val statusTypeSchema = new StructType()
      .add("st_id", StringType)
      .add("st_name", StringType)

    val taxRateSchema = new StructType()
      .add("tx_id", StringType)
      .add("tx_name", StringType)
      .add("tx_rate", FloatType)

    val tradeTypeSchema = new StructType()
      .add("tt_id", StringType)
      .add("tt_name", StringType)
      .add("tt_is_sell", IntegerType)
      .add("tt_is_mrkt", IntegerType)

    val refTables = Seq(
      ("Date.txt",       dateSchema,       "date"),
      ("Time.txt",       timeSchema,       "time"),
      ("Industry.txt",   industrySchema,   "industry"),
      ("StatusType.txt", statusTypeSchema, "status_type"),
      ("TaxRate.txt",    taxRateSchema,    "tax_rate"),
      ("TradeType.txt",  tradeTypeSchema,  "trade_type"),
    )
    for ((file, schema, table) <- refTables) {
      val src = s"$digenPath/Batch1/$file"
      if (sourceExists(src))
        writeDelta(readCsv(src, schema), s"$deltaPath/batch1/$table", s"batch1/$table")
      else
        println(s"  WARN: Source not found: $src")
    }

    // HR.csv — Batch1 only, comma-delimited
    val hrSchema = new StructType()
      .add("employeeid", StringType)
      .add("managerid", StringType)
      .add("employeefirstname", StringType)
      .add("employeelastname", StringType)
      .add("employeemi", StringType)
      .add("employeejobcode", StringType)
      .add("employeebranch", StringType)
      .add("employeeoffice", StringType)
      .add("employeephone", StringType)

    val hrSrc = s"$digenPath/Batch1/HR.csv"
    if (sourceExists(hrSrc))
      writeDelta(readCsv(hrSrc, hrSchema, ","), s"$deltaPath/batch1/hr", "batch1/hr")

    // ════════════════════════════════════════════════════════════════════════
    // BatchDate — all batches, same schema
    // ════════════════════════════════════════════════════════════════════════
    println("=== Processing BatchDate (all batches) ===")

    val batchDateSchema = new StructType()
      .add("batchdate", DateType)

    for (b <- 1 to 3) {
      val src = s"$digenPath/Batch$b/BatchDate.txt"
      if (sourceExists(src))
        writeDelta(readCsv(src, batchDateSchema), s"$deltaPath/batch$b/batch_date", s"batch$b/batch_date")
    }

    // ════════════════════════════════════════════════════════════════════════
    // Batch1 historical transactional tables (no CDC columns)
    // ════════════════════════════════════════════════════════════════════════
    println("=== Processing Batch1 historical transactional tables ===")

    val tradeHistoricalSchema = new StructType()
      .add("t_id", LongType)
      .add("t_dts", TimestampType)
      .add("t_st_id", StringType)
      .add("t_tt_id", StringType)
      .add("t_is_cash", ByteType)
      .add("t_s_symb", StringType)
      .add("t_qty", IntegerType)
      .add("t_bid_price", DoubleType)
      .add("t_ca_id", LongType)
      .add("t_exec_name", StringType)
      .add("t_trade_price", DoubleType)
      .add("t_chrg", DoubleType)
      .add("t_comm", DoubleType)
      .add("t_tax", DoubleType)

    val tradeHistoryRawSchema = new StructType()
      .add("th_t_id", LongType)
      .add("th_dts", TimestampType)
      .add("th_st_id", StringType)

    val dailyMarketHistoricalSchema = new StructType()
      .add("dm_date", DateType)
      .add("dm_s_symb", StringType)
      .add("dm_close", DoubleType)
      .add("dm_high", DoubleType)
      .add("dm_low", DoubleType)
      .add("dm_vol", IntegerType)

    val watchHistoricalSchema = new StructType()
      .add("w_c_id", LongType)
      .add("w_s_symb", StringType)
      .add("w_dts", TimestampType)
      .add("w_action", StringType)

    val holdingHistoricalSchema = new StructType()
      .add("hh_h_t_id", IntegerType)
      .add("hh_t_id", IntegerType)
      .add("hh_before_qty", IntegerType)
      .add("hh_after_qty", IntegerType)

    val cashTxnHistoricalSchema = new StructType()
      .add("ct_ca_id", LongType)
      .add("ct_dts", TimestampType)
      .add("ct_amt", DoubleType)
      .add("ct_name", StringType)

    val batch1Txn = Seq(
      ("Trade.txt",           tradeHistoricalSchema,       "trade"),
      ("TradeHistory.txt",    tradeHistoryRawSchema,       "trade_history"),
      ("DailyMarket.txt",     dailyMarketHistoricalSchema, "daily_market"),
      ("WatchHistory.txt",    watchHistoricalSchema,        "watch_history"),
      ("HoldingHistory.txt",  holdingHistoricalSchema,      "holding_history"),
      ("CashTransaction.txt", cashTxnHistoricalSchema,      "cash_transaction"),
    )
    for ((file, schema, table) <- batch1Txn) {
      val src = s"$digenPath/Batch1/$file"
      if (sourceExists(src))
        writeDelta(readCsv(src, schema), s"$deltaPath/batch1/$table", s"batch1/$table")
      else
        println(s"  WARN: Source not found: $src")
    }

    // ════════════════════════════════════════════════════════════════════════
    // Batch2/3 incremental transactional tables (with CDC prefix columns)
    // ════════════════════════════════════════════════════════════════════════
    println("=== Processing Batch2/3 incremental transactional tables ===")

    val tradeIncrSchema = new StructType()
      .add("cdc_flag", StringType)
      .add("cdc_dsn", LongType)
      .add("t_id", LongType)
      .add("t_dts", TimestampType)
      .add("t_st_id", StringType)
      .add("t_tt_id", StringType)
      .add("t_is_cash", ByteType)
      .add("t_s_symb", StringType)
      .add("t_qty", IntegerType)
      .add("t_bid_price", DoubleType)
      .add("t_ca_id", LongType)
      .add("t_exec_name", StringType)
      .add("t_trade_price", DoubleType)
      .add("t_chrg", DoubleType)
      .add("t_comm", DoubleType)
      .add("t_tax", DoubleType)

    val dailyMarketIncrSchema = new StructType()
      .add("cdc_flag", StringType)
      .add("cdc_dsn", LongType)
      .add("dm_date", DateType)
      .add("dm_s_symb", StringType)
      .add("dm_close", DoubleType)
      .add("dm_high", DoubleType)
      .add("dm_low", DoubleType)
      .add("dm_vol", IntegerType)

    val watchIncrSchema = new StructType()
      .add("cdc_flag", StringType)
      .add("cdc_dsn", LongType)
      .add("w_c_id", LongType)
      .add("w_s_symb", StringType)
      .add("w_dts", TimestampType)
      .add("w_action", StringType)

    val holdingIncrSchema = new StructType()
      .add("cdc_flag", StringType)
      .add("cdc_dsn", LongType)
      .add("hh_h_t_id", IntegerType)
      .add("hh_t_id", IntegerType)
      .add("hh_before_qty", IntegerType)
      .add("hh_after_qty", IntegerType)

    val cashTxnIncrSchema = new StructType()
      .add("cdc_flag", StringType)
      .add("cdc_dsn", LongType)
      .add("ct_ca_id", LongType)
      .add("ct_dts", TimestampType)
      .add("ct_amt", DoubleType)
      .add("ct_name", StringType)

    val incrTxn = Seq(
      ("Trade.txt",           tradeIncrSchema,       "trade"),
      ("DailyMarket.txt",     dailyMarketIncrSchema, "daily_market"),
      ("WatchHistory.txt",    watchIncrSchema,        "watch_history"),
      ("HoldingHistory.txt",  holdingIncrSchema,      "holding_history"),
      ("CashTransaction.txt", cashTxnIncrSchema,      "cash_transaction"),
    )
    for (b <- 2 to 3; (file, schema, table) <- incrTxn) {
      val src = s"$digenPath/Batch$b/$file"
      if (sourceExists(src))
        writeDelta(readCsv(src, schema), s"$deltaPath/batch$b/$table", s"batch$b/$table")
      else
        println(s"  WARN: Source not found: $src")
    }

    // ════════════════════════════════════════════════════════════════════════
    // Customer.txt — all batches, CDC schema
    // ════════════════════════════════════════════════════════════════════════
    println("=== Processing Customer (all batches) ===")

    val customerSchema = new StructType()
      .add("cdc_flag", StringType)
      .add("cdc_dsn", LongType)
      .add("customerid", LongType)
      .add("taxid", StringType)
      .add("status", StringType)
      .add("lastname", StringType)
      .add("firstname", StringType)
      .add("middleinitial", StringType)
      .add("gender", StringType)
      .add("tier", ByteType)
      .add("dob", DateType)
      .add("addressline1", StringType)
      .add("addressline2", StringType)
      .add("postalcode", StringType)
      .add("city", StringType)
      .add("stateprov", StringType)
      .add("country", StringType)
      .add("c_ctry_1", StringType)
      .add("c_area_1", StringType)
      .add("c_local_1", StringType)
      .add("c_ext_1", StringType)
      .add("c_ctry_2", StringType)
      .add("c_area_2", StringType)
      .add("c_local_2", StringType)
      .add("c_ext_2", StringType)
      .add("c_ctry_3", StringType)
      .add("c_area_3", StringType)
      .add("c_local_3", StringType)
      .add("c_ext_3", StringType)
      .add("email1", StringType)
      .add("email2", StringType)
      .add("lcl_tx_id", StringType)
      .add("nat_tx_id", StringType)

    for (b <- 1 to 3) {
      val src = s"$digenPath/Batch$b/Customer.txt"
      if (sourceExists(src))
        writeDelta(readCsv(src, customerSchema), s"$deltaPath/batch$b/customer", s"batch$b/customer")
    }

    // ════════════════════════════════════════════════════════════════════════
    // Account.txt — all batches, CDC schema
    // ════════════════════════════════════════════════════════════════════════
    println("=== Processing Account (all batches) ===")

    val accountSchema = new StructType()
      .add("cdc_flag", StringType)
      .add("cdc_dsn", LongType)
      .add("accountid", LongType)
      .add("ca_b_id", LongType)
      .add("ca_c_id", LongType)
      .add("accountdesc", StringType)
      .add("taxstatus", ByteType)
      .add("ca_st_id", StringType)

    for (b <- 1 to 3) {
      val src = s"$digenPath/Batch$b/Account.txt"
      if (sourceExists(src))
        writeDelta(readCsv(src, accountSchema), s"$deltaPath/batch$b/account", s"batch$b/account")
    }

    // ════════════════════════════════════════════════════════════════════════
    // Prospect.csv — all batches, comma-delimited, no CDC
    // ════════════════════════════════════════════════════════════════════════
    println("=== Processing Prospect (all batches) ===")

    val prospectSchema = new StructType()
      .add("agencyid", StringType)
      .add("lastname", StringType)
      .add("firstname", StringType)
      .add("middleinitial", StringType)
      .add("gender", StringType)
      .add("addressline1", StringType)
      .add("addressline2", StringType)
      .add("postalcode", StringType)
      .add("city", StringType)
      .add("state", StringType)
      .add("country", StringType)
      .add("phone", StringType)
      .add("income", StringType)
      .add("numbercars", IntegerType)
      .add("numberchildren", IntegerType)
      .add("maritalstatus", StringType)
      .add("age", IntegerType)
      .add("creditrating", IntegerType)
      .add("ownorrentflag", StringType)
      .add("employer", StringType)
      .add("numbercreditcards", IntegerType)
      .add("networth", IntegerType)

    for (b <- 1 to 3) {
      val src = s"$digenPath/Batch$b/Prospect.csv"
      if (sourceExists(src))
        writeDelta(readCsv(src, prospectSchema, ","), s"$deltaPath/batch$b/prospect", s"batch$b/prospect")
    }

    // ════════════════════════════════════════════════════════════════════════
    // FINWIRE — Batch1 only, fixed-width text lines
    // ════════════════════════════════════════════════════════════════════════
    println("=== Processing FINWIRE (Batch1) ===")

    val finwireOutPath = s"$deltaPath/batch1/finwire"
    if (deltaExists(finwireOutPath)) {
      println(s"  SKIP: batch1/finwire — already exists")
      skipped += 1
    } else {
      val finwireDir = new File(s"$digenPath/Batch1")
      if (finwireDir.isDirectory) {
        val finwireFiles = finwireDir.listFiles()
          .filter(f => f.getName.matches("FINWIRE\\d{4}Q[1-4]"))
          .map(_.getAbsolutePath)
        if (finwireFiles.nonEmpty) {
          val rawDf = spark.read.text(finwireFiles: _*)
            .withColumnRenamed("value", "line")
            .withColumn("rec_type", substring(col("line"), 16, 3))
            .withColumn("pts", to_timestamp(substring(col("line"), 1, 15), "yyyyMMdd-HHmmss"))
          writeDelta(rawDf, finwireOutPath, "batch1/finwire")
        } else {
          println("  WARN: No FINWIRE files found in Batch1")
        }
      }
    }

    // ════════════════════════════════════════════════════════════════════════
    // CustomerMgmt.xml — Batch1 only, parsed with spark-xml
    // ════════════════════════════════════════════════════════════════════════
    println("=== Processing CustomerMgmt.xml (Batch1) ===")

    val custMgmtOutPath = s"$deltaPath/batch1/customer_mgmt"
    if (deltaExists(custMgmtOutPath)) {
      println(s"  SKIP: batch1/customer_mgmt — already exists")
      skipped += 1
    } else {
      val xmlSrc = s"$digenPath/Batch1/CustomerMgmt.xml"
      if (sourceExists(xmlSrc)) {
        val xmlDf = spark.read
          .format("xml")
          .option("rowTag", "TPCDI:Action")
          .load(xmlSrc)
        writeDelta(xmlDf, custMgmtOutPath, "batch1/customer_mgmt")
      } else {
        println("  WARN: CustomerMgmt.xml not found")
      }
    }

    // ════════════════════════════════════════════════════════════════════════
    // Audit CSV — root level, comma-delimited, with header
    // ════════════════════════════════════════════════════════════════════════
    println("=== Processing audit CSV ===")

    val auditOutPath = s"$deltaPath/audit"
    if (deltaExists(auditOutPath)) {
      println(s"  SKIP: audit — already exists")
      skipped += 1
    } else {
      val auditSchema = new StructType()
        .add("dataset", StringType)
        .add("batchid", IntegerType)
        .add("date", DateType)
        .add("attribute", StringType)
        .add("value", LongType)
        .add("dvalue", DecimalType(15, 5))

      val digenDir = new File(digenPath)
      if (digenDir.isDirectory) {
        val auditFiles = digenDir.listFiles()
          .filter(f => f.getName.endsWith("_audit.csv"))
          .map(_.getAbsolutePath)
        if (auditFiles.nonEmpty) {
          val auditDf = spark.read
            .option("header", "true")
            .option("delimiter", ",")
            .schema(auditSchema)
            .csv(auditFiles: _*)
          writeDelta(auditDf, auditOutPath, "audit")
        } else {
          println("  WARN: No audit CSV files found")
        }
      }
    }

    // ════════════════════════════════════════════════════════════════════════
    // Summary
    // ════════════════════════════════════════════════════════════════════════
    println(s"=== Complete: $written tables written, $skipped tables skipped (already existed) ===")

    spark.stop()
  }
}
