package datagen

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.io.File
import java.time.LocalDate

object TpcdiToDelta {

  val AugmentedStart: LocalDate = LocalDate.parse("2016-07-06")

  def AugmentedEnd(batch2Days: Int): LocalDate = {
    require(batch2Days >= 1 && batch2Days <= 365, "TPCDI_BATCH_2_DAYS must be between 1 and 365")
    AugmentedStart.plusDays(batch2Days.toLong)
  }

  private def deltaExists(path: String): Boolean =
    new File(s"$path/_delta_log").isDirectory

  private def sourceExists(path: String): Boolean =
    new File(path).exists()

  def main(args: Array[String]): Unit = {
    val digenPath = sys.env.getOrElse("DIGEN_PATH", "/data/digen")
    val deltaPath = sys.env.getOrElse("DELTA_PATH", "/data/delta")
    val batch2Days = sys.env.getOrElse("TPCDI_BATCH_2_DAYS", "0").toInt
    require(batch2Days >= 0 && batch2Days <= 365, "TPCDI_BATCH_2_DAYS must be between 0 and 365")
    val augmented = batch2Days > 0
    if (augmented) {
      println(
        s"=== Databricks-style augmented window: Batch 2 $AugmentedStart until " +
          s"${AugmentedEnd(batch2Days)} (exclusive), Batch 3 ${AugmentedEnd(batch2Days)} ==="
      )
    }

    def insertPct(batch: Int): Double =
      sys.env
        .get(s"BATCH_${batch}_INSERT_PCT")
        .filter(_.nonEmpty)
        .orElse(sys.env.get(s"BATCH_${batch}_PCT"))
        .getOrElse(sys.error(s"BATCH_${batch}_INSERT_PCT or BATCH_${batch}_PCT env var required"))
        .toDouble

    val batchPct = Map(
      1 -> insertPct(1),
      2 -> insertPct(2),
      3 -> insertPct(3),
    )
    println(s"=== Batch limits: B1=${batchPct(1)}%, B2=${batchPct(2)}%, B3=${batchPct(3)}% ===")

    val spark = SparkSession.builder()
      .appName("TpcdiToDelta")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    def augmentedBatch(df: DataFrame, eventColumn: String, batch: Int): DataFrame = {
      require(batch >= 1 && batch <= 3, "batch must be 1, 2, or 3")
      val eventDate = to_date(col(eventColumn))
      val start = lit(AugmentedStart.toString).cast(DateType)
      val end = lit(AugmentedEnd(batch2Days).toString).cast(DateType)
      batch match {
        case 1 => df.filter(eventDate < start)
        case 2 => df.filter(eventDate >= start && eventDate < end)
        case 3 => df.filter(eventDate >= end && eventDate < date_add(end, 1))
      }
    }

    var written = 0
    var skipped = 0

    def applyLimit(df: DataFrame, batch: Int, label: String): DataFrame = {
      val pct = batchPct(batch)
      if (pct >= 100.0) return df
      val totalRows = df.count()
      val limitRows = math.max(1L, math.ceil(totalRows * pct / 100.0).toLong).toInt
      println(s"  LIMIT: $label — ${pct}% of $totalRows rows = $limitRows rows")
      df.limit(limitRows)
    }

    def writeDelta(df: DataFrame, outPath: String, label: String, batch: Int): Unit = {
      if (deltaExists(outPath)) {
        println(s"  SKIP: $label — Delta table already exists")
        skipped += 1
      } else {
        val limited = applyLimit(df, batch, label)
        println(s"  WRITE: $label → $outPath")
        limited.write.format("delta")
          .option("delta.enableChangeDataFeed", "true")
          .mode("overwrite")
          .save(outPath)
        println(s"  DONE: $label")
        written += 1
      }
    }

    def sourcePaths(outputBatch: Int, file: String): Seq[String] =
      Seq(s"$digenPath/Batch$outputBatch/$file").filter(sourceExists)

    def readCsv(paths: Seq[String], schema: StructType, delimiter: String = "|"): DataFrame =
      spark.read
        .option("header", "false")
        .option("delimiter", delimiter)
        .schema(schema)
        .csv(paths: _*)

    def writeSources(outputBatch: Int, file: String, schema: StructType, table: String,
                     delimiter: String = "|"): Unit = {
      val paths = sourcePaths(outputBatch, file)
      if (paths.nonEmpty) {
        val label = s"batch$outputBatch/$table"
        println(s"  SOURCES: $label <- ${paths.mkString(", ")}")
        writeDelta(readCsv(paths, schema, delimiter), s"$deltaPath/batch$outputBatch/$table", label, outputBatch)
      } else {
        println(s"  WARN: No sources found for batch$outputBatch/$table")
      }
    }

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
        writeDelta(readCsv(Seq(src), schema), s"$deltaPath/batch1/$table", s"batch1/$table", 1)
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
      writeDelta(readCsv(Seq(hrSrc), hrSchema, ","), s"$deltaPath/batch1/hr", "batch1/hr", 1)

    // ════════════════════════════════════════════════════════════════════════
    // BatchDate — all batches, same schema
    // ════════════════════════════════════════════════════════════════════════
    println("=== Processing BatchDate (all batches) ===")

    val batchDateSchema = new StructType()
      .add("batchdate", DateType)

    if (augmented) {
      val dates = Map(
        1 -> spark.range(1).select(date_add(lit(AugmentedStart.toString), -1).alias("batchdate")),
        2 -> spark.range(batch2Days).select(
          date_add(lit(AugmentedStart.toString), col("id").cast(IntegerType)).alias("batchdate")
        ),
        3 -> spark.range(1).select(lit(AugmentedEnd(batch2Days).toString).cast(DateType).alias("batchdate")),
      )
      for (b <- 1 to 3)
        writeDelta(dates(b), s"$deltaPath/batch$b/batch_date", s"batch$b/batch_date", b)
    } else {
      for (b <- 1 to 3)
        writeSources(b, "BatchDate.txt", batchDateSchema, "batch_date")
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
    val historicalFrames = batch1Txn.flatMap { case (file, schema, table) =>
      val src = s"$digenPath/Batch1/$file"
      if (sourceExists(src)) Some(table -> readCsv(Seq(src), schema))
      else {
        println(s"  WARN: Source not found: $src")
        None
      }
    }.toMap

    val eventColumns = Map(
      "trade" -> "t_dts",
      "trade_history" -> "th_dts",
      "daily_market" -> "dm_date",
      "watch_history" -> "w_dts",
      "cash_transaction" -> "ct_dts",
    )
    val holdingEventDates = if (augmented) {
      Some(
        historicalFrames("holding_history")
          .join(
            historicalFrames("trade_history")
              .groupBy("th_t_id")
              .agg(max("th_dts").alias("event_dts")),
            col("hh_t_id") === col("th_t_id"),
            "inner",
          )
          .drop("th_t_id")
      )
    } else None

    val tradeEvents = if (augmented) {
      Some(
        historicalFrames("trade_history")
          .withColumn(
            "cdc_flag",
            when(
              row_number().over(Window.partitionBy("th_t_id").orderBy("th_dts")) === 1,
              lit("I"),
            ).otherwise(lit("U")),
          )
          .join(historicalFrames("trade"), col("th_t_id") === col("t_id"), "inner")
          .select(
            col("cdc_flag"),
            unix_timestamp(col("th_dts")).alias("cdc_dsn"),
            col("t_id"),
            col("th_dts").alias("t_dts"),
            col("th_st_id").alias("t_st_id"),
            col("t_tt_id"),
            col("t_is_cash"),
            col("t_s_symb"),
            col("t_qty"),
            col("t_bid_price"),
            col("t_ca_id"),
            col("t_exec_name"),
            when(col("th_st_id") === "CMPT", col("t_trade_price")).alias("t_trade_price"),
            when(col("th_st_id") === "CMPT", col("t_chrg")).alias("t_chrg"),
            when(col("th_st_id") === "CMPT", col("t_comm")).alias("t_comm"),
            when(col("th_st_id") === "CMPT", col("t_tax")).alias("t_tax"),
          )
      )
    } else None

    historicalFrames.foreach { case (table, df) =>
      val initial = if (!augmented) df else if (table == "holding_history") {
        augmentedBatch(holdingEventDates.get, "event_dts", 1).drop("event_dts")
      } else if (table == "trade") {
        augmentedBatch(tradeEvents.get, "t_dts", 1)
          .withColumn("latest", row_number().over(Window.partitionBy("t_id").orderBy(col("t_dts").desc)))
          .filter(col("latest") === 1)
          .drop("latest", "cdc_flag", "cdc_dsn")
      } else {
        augmentedBatch(df, eventColumns(table), 1)
      }
      writeDelta(initial, s"$deltaPath/batch1/$table", s"batch1/$table", 1)
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
    if (augmented) {
      def withInsertCdc(df: DataFrame, eventColumn: String): DataFrame =
        df.withColumn("cdc_flag", lit("I"))
          .withColumn("cdc_dsn", unix_timestamp(col(eventColumn)))
          .select((Seq("cdc_flag", "cdc_dsn") ++ df.columns).map(col): _*)

      val simpleEvents = Seq(
        ("daily_market", "dm_date"),
        ("watch_history", "w_dts"),
        ("cash_transaction", "ct_dts"),
      )
      for (b <- 2 to 3) {
        simpleEvents.foreach { case (table, eventColumn) =>
          val rows = withInsertCdc(augmentedBatch(historicalFrames(table), eventColumn, b), eventColumn)
          writeDelta(rows, s"$deltaPath/batch$b/$table", s"batch$b/$table", b)
        }
        val holdingRows = withInsertCdc(
          augmentedBatch(holdingEventDates.get, "event_dts", b), "event_dts",
        ).drop("event_dts")
        writeDelta(holdingRows, s"$deltaPath/batch$b/holding_history", s"batch$b/holding_history", b)

        val trades = augmentedBatch(tradeEvents.get, "t_dts", b)
        writeDelta(trades, s"$deltaPath/batch$b/trade", s"batch$b/trade", b)
      }
    } else {
      for (b <- 2 to 3; (file, schema, table) <- incrTxn)
        writeSources(b, file, schema, table)
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

    lazy val customerActions = spark.read
      .format("xml")
      .option("rowTag", "TPCDI:Action")
      .load(s"$digenPath/Batch1/CustomerMgmt.xml")
      .withColumn("action_ts", to_timestamp(col("_ActionTS")))

    if (augmented) {
      val customerEvents = customerActions
        .filter(col("_ActionType").isin("NEW", "INACT", "UPDCUST"))
        .select(
          when(col("_ActionType") === "NEW", lit("I")).otherwise(lit("U")).alias("cdc_flag"),
          unix_timestamp(col("action_ts")).alias("cdc_dsn"),
          col("Customer._C_ID").cast(LongType).alias("customerid"),
          col("Customer._C_TAX_ID").cast(StringType).alias("taxid"),
          when(col("_ActionType") === "INACT", lit("INAC")).otherwise(lit("ACTV")).alias("status"),
          col("Customer.Name.C_L_NAME").cast(StringType).alias("lastname"),
          col("Customer.Name.C_F_NAME").cast(StringType).alias("firstname"),
          col("Customer.Name.C_M_NAME").cast(StringType).alias("middleinitial"),
          upper(col("Customer._C_GNDR")).cast(StringType).alias("gender"),
          col("Customer._C_TIER").cast(ByteType).alias("tier"),
          col("Customer._C_DOB").cast(DateType).alias("dob"),
          col("Customer.Address.C_ADLINE1").cast(StringType).alias("addressline1"),
          col("Customer.Address.C_ADLINE2").cast(StringType).alias("addressline2"),
          col("Customer.Address.C_ZIPCODE").cast(StringType).alias("postalcode"),
          col("Customer.Address.C_CITY").cast(StringType).alias("city"),
          col("Customer.Address.C_STATE_PROV").cast(StringType).alias("stateprov"),
          col("Customer.Address.C_CTRY").cast(StringType).alias("country"),
          col("Customer.ContactInfo.C_PHONE_1.C_CTRY_CODE").cast(StringType).alias("c_ctry_1"),
          col("Customer.ContactInfo.C_PHONE_1.C_AREA_CODE").cast(StringType).alias("c_area_1"),
          col("Customer.ContactInfo.C_PHONE_1.C_LOCAL").cast(StringType).alias("c_local_1"),
          col("Customer.ContactInfo.C_PHONE_1.C_EXT").cast(StringType).alias("c_ext_1"),
          col("Customer.ContactInfo.C_PHONE_2.C_CTRY_CODE").cast(StringType).alias("c_ctry_2"),
          col("Customer.ContactInfo.C_PHONE_2.C_AREA_CODE").cast(StringType).alias("c_area_2"),
          col("Customer.ContactInfo.C_PHONE_2.C_LOCAL").cast(StringType).alias("c_local_2"),
          col("Customer.ContactInfo.C_PHONE_2.C_EXT").cast(StringType).alias("c_ext_2"),
          col("Customer.ContactInfo.C_PHONE_3.C_CTRY_CODE").cast(StringType).alias("c_ctry_3"),
          col("Customer.ContactInfo.C_PHONE_3.C_AREA_CODE").cast(StringType).alias("c_area_3"),
          col("Customer.ContactInfo.C_PHONE_3.C_LOCAL").cast(StringType).alias("c_local_3"),
          col("Customer.ContactInfo.C_PHONE_3.C_EXT").cast(StringType).alias("c_ext_3"),
          col("Customer.ContactInfo.C_PRIM_EMAIL").cast(StringType).alias("email1"),
          col("Customer.ContactInfo.C_ALT_EMAIL").cast(StringType).alias("email2"),
          col("Customer.TaxInfo.C_LCL_TX_ID").cast(StringType).alias("lcl_tx_id"),
          col("Customer.TaxInfo.C_NAT_TX_ID").cast(StringType).alias("nat_tx_id"),
          col("action_ts"),
        )
      for (b <- 2 to 3) {
        val rows = augmentedBatch(customerEvents, "action_ts", b).drop("action_ts")
        writeDelta(rows, s"$deltaPath/batch$b/customer", s"batch$b/customer", b)
      }
    } else {
      for (b <- 1 to 3)
        writeSources(b, "Customer.txt", customerSchema, "customer")
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

    if (augmented) {
      val accountEvents = customerActions
        .filter(!col("_ActionType").isin("UPDCUST", "INACT"))
        .filter(col("Customer.Account._CA_ID").isNotNull)
        .select(
          when(col("_ActionType").isin("NEW", "ADDACCT"), lit("I")).otherwise(lit("U")).alias("cdc_flag"),
          unix_timestamp(col("action_ts")).alias("cdc_dsn"),
          col("Customer.Account._CA_ID").cast(LongType).alias("accountid"),
          col("Customer.Account.CA_B_ID").cast(LongType).alias("ca_b_id"),
          col("Customer._C_ID").cast(LongType).alias("ca_c_id"),
          col("Customer.Account.CA_NAME").cast(StringType).alias("accountdesc"),
          col("Customer.Account._CA_TAX_ST").cast(ByteType).alias("taxstatus"),
          when(col("_ActionType") === "CLOSEACCT", lit("INAC")).otherwise(lit("ACTV")).alias("ca_st_id"),
          col("action_ts"),
        )
      for (b <- 2 to 3) {
        val rows = augmentedBatch(accountEvents, "action_ts", b).drop("action_ts")
        writeDelta(rows, s"$deltaPath/batch$b/account", s"batch$b/account", b)
      }
    } else {
      for (b <- 1 to 3)
        writeSources(b, "Account.txt", accountSchema, "account")
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

    for (b <- 1 to 3 if batch2Days == 0 || b == 1)
      writeSources(b, "Prospect.csv", prospectSchema, "prospect", ",")

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
          writeDelta(rawDf, finwireOutPath, "batch1/finwire", 1)
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
        val xmlDf = if (augmented) augmentedBatch(customerActions, "action_ts", 1).drop("action_ts")
          else customerActions.drop("action_ts")
        writeDelta(xmlDf, custMgmtOutPath, "batch1/customer_mgmt", 1)
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
          writeDelta(auditDf, auditOutPath, "audit", 1)
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
