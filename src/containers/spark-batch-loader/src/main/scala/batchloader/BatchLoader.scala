package batchloader

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.lit
import org.apache.hadoop.fs.Path
import scala.math.BigDecimal.RoundingMode

object BatchLoader {

  val deltaPath: String = sys.env.getOrElse("DELTA_PATH", "/data/delta")
  val scoreModulus: Long = 1000000L

  // Category A: tables that start with batch1 data
  val categoryA: Seq[String] = Seq(
    "cash_transaction", "daily_market", "holding_history",
    "prospect", "trade", "watch_history"
  )

  // Category B: tables that start empty
  val categoryB: Seq[String] = Seq("account", "customer", "batch_date")

  // Tables that have CDC columns in batch2/3 (all Category A except prospect)
  val cdcTables: Set[String] = Set(
    "cash_transaction", "daily_market", "holding_history", "trade", "watch_history"
  )

  // Superset schemas for Category B tables (always have same schema across batches)
  val categoryBSchemas: Map[String, StructType] = Map(
    "account" -> StructType(Seq(
      StructField("cdc_flag", StringType, true),
      StructField("cdc_dsn", LongType, true),
      StructField("accountid", LongType, true),
      StructField("ca_b_id", LongType, true),
      StructField("ca_c_id", LongType, true),
      StructField("accountdesc", StringType, true),
      StructField("taxstatus", ByteType, true),
      StructField("ca_st_id", StringType, true)
    )),
    "customer" -> StructType(Seq(
      StructField("cdc_flag", StringType, true),
      StructField("cdc_dsn", LongType, true),
      StructField("customerid", LongType, true),
      StructField("taxid", StringType, true),
      StructField("status", StringType, true),
      StructField("lastname", StringType, true),
      StructField("firstname", StringType, true),
      StructField("middleinitial", StringType, true),
      StructField("gender", StringType, true),
      StructField("tier", ByteType, true),
      StructField("dob", DateType, true),
      StructField("addressline1", StringType, true),
      StructField("addressline2", StringType, true),
      StructField("postalcode", StringType, true),
      StructField("city", StringType, true),
      StructField("stateprov", StringType, true),
      StructField("country", StringType, true),
      StructField("c_ctry_1", StringType, true),
      StructField("c_area_1", StringType, true),
      StructField("c_local_1", StringType, true),
      StructField("c_ext_1", StringType, true),
      StructField("c_ctry_2", StringType, true),
      StructField("c_area_2", StringType, true),
      StructField("c_local_2", StringType, true),
      StructField("c_ext_2", StringType, true),
      StructField("c_ctry_3", StringType, true),
      StructField("c_area_3", StringType, true),
      StructField("c_local_3", StringType, true),
      StructField("c_ext_3", StringType, true),
      StructField("email1", StringType, true),
      StructField("email2", StringType, true),
      StructField("lcl_tx_id", StringType, true),
      StructField("nat_tx_id", StringType, true)
    )),
    "batch_date" -> StructType(Seq(
      StructField("batchdate", DateType, true)
    ))
  )

  case class MutationSpec(
    table: String,
    scoreExpr: String,
    updateAssignments: String
  )

  val mutationSpecs: Seq[MutationSpec] = Seq(
    MutationSpec(
      "cash_transaction",
      "pmod(coalesce(ct_ca_id, 0L), 1000000L) * 2654435761L",
      "ct_amt = coalesce(ct_amt, 0D) + 0.01D"
    ),
    MutationSpec(
      "daily_market",
      "datediff(dm_date, DATE '1970-01-01') * 2654435761L + coalesce(ascii(substr(dm_s_symb, 1, 1)), 0) * 9176L + coalesce(ascii(substr(dm_s_symb, 2, 1)), 0)",
      "dm_close = dm_close + 0.01D, dm_high = dm_high + 0.01D, dm_vol = dm_vol + 1"
    ),
    MutationSpec(
      "holding_history",
      "pmod(coalesce(hh_h_t_id, 0), 1000000) * 2654435761L + pmod(coalesce(hh_t_id, 0), 1000000)",
      "hh_after_qty = hh_after_qty + 1"
    ),
    MutationSpec(
      "prospect",
      "coalesce(age, 0) * 2654435761L + coalesce(creditrating, 0) * 9176L + coalesce(numbercars, 0) * 271L + coalesce(numberchildren, 0)",
      "networth = networth + 1"
    ),
    MutationSpec(
      "trade",
      "pmod(coalesce(t_id, 0L), 1000000L) * 2654435761L",
      "t_qty = t_qty + 1"
    ),
    MutationSpec(
      "watch_history",
      "pmod(coalesce(w_c_id, 0L), 1000000L) * 2654435761L",
      "w_dts = w_dts + INTERVAL 1 SECOND"
    ),
    MutationSpec(
      "account",
      "pmod(coalesce(accountid, 0L), 1000000L) * 2654435761L",
      "accountdesc = concat(accountdesc, ' upd')"
    ),
    MutationSpec(
      "customer",
      "pmod(coalesce(customerid, 0L), 1000000L) * 2654435761L",
      "tier = CASE WHEN tier IS NULL THEN NULL ELSE CAST(((CAST(tier AS INT) + 1) % 100) AS TINYINT) END"
    )
  )

  def main(args: Array[String]): Unit = {
    args(0) match {
      case "init"   => initStaging()
      case "append" => appendBatch(args(1).toInt)
      case other    => sys.error(s"Unknown mode: $other. Use 'init' or 'append <batch_number>'")
    }
  }

  private def spark: SparkSession = {
    SparkSession.builder()
      .appName("BatchLoader")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
  }

  private def initStaging(): Unit = {
    val session = spark
    val stagingDir = s"$deltaPath/staging"

    // Delete staging directory if it exists
    val hadoopConf = session.sparkContext.hadoopConfiguration
    val fs = new Path(stagingDir).getFileSystem(hadoopConf)
    val stagingPath = new Path(stagingDir)
    if (fs.exists(stagingPath)) {
      println(s"Deleting existing staging directory: $stagingDir")
      fs.delete(stagingPath, true)
    }

    // Category A: copy batch1 data with superset schema
    categoryA.foreach { table =>
      val srcPath = s"$deltaPath/batch1/$table"
      val dstPath = s"$stagingDir/$table"
      println(s"[init] Category A: $table -> $dstPath")

      val df = session.read.format("delta").load(srcPath)

      val outputDf = if (cdcTables.contains(table)) {
        // Add null cdc_flag and cdc_dsn as first columns
        val withCdc = df
          .withColumn("cdc_flag", lit(null).cast(StringType))
          .withColumn("cdc_dsn", lit(null).cast(LongType))
        // Reorder to put cdc_flag and cdc_dsn first
        val cols = Seq("cdc_flag", "cdc_dsn") ++ df.columns
        withCdc.select(cols.map(withCdc.col): _*)
      } else {
        // prospect: no CDC columns needed
        df
      }

      outputDf.write
        .format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .mode("overwrite")
        .save(dstPath)
    }

    // Category B: create empty tables with correct schema
    categoryB.foreach { table =>
      val dstPath = s"$stagingDir/$table"
      println(s"[init] Category B (empty): $table -> $dstPath")

      val schema = categoryBSchemas(table)
      val emptyDf = session.createDataFrame(
        session.sparkContext.emptyRDD[org.apache.spark.sql.Row],
        schema
      )

      emptyDf.write
        .format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .mode("overwrite")
        .save(dstPath)
    }

    println("[init] Staging initialization complete.")
    session.stop()
  }

  private def appendBatch(batchNum: Int): Unit = {
    val session = spark
    val stagingDir = s"$deltaPath/staging"
    val allTables = categoryA ++ categoryB

    applyMutations(session, stagingDir, batchNum)

    allTables.foreach { table =>
      val srcPath = s"$deltaPath/batch$batchNum/$table"
      val dstPath = s"$stagingDir/$table"

      // Check if source exists
      val hadoopConf = session.sparkContext.hadoopConfiguration
      val fs = new Path(srcPath).getFileSystem(hadoopConf)
      if (fs.exists(new Path(srcPath))) {
        println(s"[append] batch$batchNum/$table -> staging/$table")
        val df = session.read.format("delta").load(srcPath)
        df.write
          .format("delta")
          .mode("append")
          .save(dstPath)
      } else {
        println(s"[append] Skipping $table (no data in batch$batchNum)")
      }
    }

    println(s"[append] Batch $batchNum append complete.")
    session.stop()
  }

  private def mutationPct(batchNum: Int, op: String): BigDecimal = {
    val name = s"BATCH_${batchNum}_${op.toUpperCase}_PCT"
    val value = BigDecimal(sys.env.getOrElse(name, "0"))
    if (value < 0 || value > 100) {
      sys.error(s"$name must be between 0 and 100")
    }
    value
  }

  private def mutationBuckets(batchNum: Int): Map[String, (Long, Long, BigDecimal)] = {
    if (batchNum == 1) return Map.empty
    val pcts = Map(
      "update" -> mutationPct(batchNum, "update"),
      "delete" -> mutationPct(batchNum, "delete")
    )
    val total = pcts.values.sum
    if (total > 100) {
      sys.error(s"Batch $batchNum mutation percentages sum to $total; max is 100")
    }

    var start = 0L
    Seq("update", "delete").map { op =>
      val width = ((pcts(op) * scoreModulus) / 100).setScale(0, RoundingMode.HALF_UP).toLong
      val bucket = (start, start + width, pcts(op))
      start += width
      op -> bucket
    }.toMap
  }

  private def scorePredicate(spec: MutationSpec, batchNum: Int, start: Long, end: Long): String = {
    if (start == end) return "false"
    val score = s"pmod(cast((${spec.scoreExpr} + ${batchNum * 7919L}) as long), $scoreModulus)"
    s"$score >= $start AND $score < $end"
  }

  private def applyMutations(session: SparkSession, stagingDir: String, batchNum: Int): Unit = {
    val buckets = mutationBuckets(batchNum)
    if (buckets.isEmpty || buckets.values.forall { case (start, end, _) => start == end }) {
      return
    }

    mutationSpecs.foreach { spec =>
      val tablePath = s"$stagingDir/${spec.table}"
      val relation = s"delta.`$tablePath`"
      val (updateStart, updateEnd, updatePct) = buckets("update")
      val (deleteStart, deleteEnd, deletePct) = buckets("delete")

      if (updateStart != updateEnd) {
        val pred = scorePredicate(spec, batchNum, updateStart, updateEnd)
        println(s"[mutate] batch$batchNum/${spec.table}: UPDATE ${updatePct}%")
        session.sql(s"UPDATE $relation SET ${spec.updateAssignments} WHERE $pred")
      }
      if (deleteStart != deleteEnd) {
        val pred = scorePredicate(spec, batchNum, deleteStart, deleteEnd)
        println(s"[mutate] batch$batchNum/${spec.table}: DELETE ${deletePct}%")
        session.sql(s"DELETE FROM $relation WHERE $pred")
      }
    }
  }
}
