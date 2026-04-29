package batchloader

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.lit
import org.apache.hadoop.fs.Path

object BatchLoader {

  val deltaPath: String = sys.env.getOrElse("DELTA_PATH", "/data/delta")

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
}
