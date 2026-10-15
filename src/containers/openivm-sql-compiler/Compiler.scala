package org.openivm.spark.commands

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.openivm.spark.common.SparkRefreshRewriter
import org.openivm.spark.compiler.{CompileRequest, LptsSparkDialect}
import scala.collection.JavaConverters._

/** Compile against empty schemas; all data processing happens remotely. */
object BenchSqlCompiler {
  private val mapper = new ObjectMapper()
  private def schema(node: JsonNode): StructType = StructType(node.elements().asScala.map { field =>
    StructField(field.get("name").asText(), DataType.fromDDL(field.get("ddl_type").asText()), true)
  }.toSeq)
  private lazy val spark = SparkSession.builder().master("local[1]")
    .appName("openivm-sql-compiler").config("spark.ui.enabled", "false")
    .config("spark.sql.warehouse.dir", "/tmp/openivm-compiler-warehouse")
    .getOrCreate()

  def run(input: String): String = synchronized {
    val in = mapper.readTree(input)
    val name = in.get("name").asText()
    val ident = TableIdentifier(name, Some("openivm_compile"))
    val out = mapper.createObjectNode()
    if (in.get("operation").asText() == "compile") {
      spark.sql("CREATE DATABASE IF NOT EXISTS openivm_compile")
      in.get("schemas").fields().asScala.foreach { entry =>
        val sourceSchema = schema(entry.getValue)
        val table = "openivm_compile.`" + entry.getKey.replace("`", "``") + "`"
        spark.sql(s"DROP TABLE IF EXISTS $table")
        spark.sql(s"CREATE TABLE $table (${sourceSchema.toDDL}) USING parquet")
      }
      val query = in.get("query").asText()
      val result = MvDryCompile.dryCompile(spark, ident, query)
      if (result.compiled.sql.isEmpty && result.compiled.initialLoadSql.isEmpty) {
        // The introspection API suppresses native compiler errors. Recover the
        // exception here so the warehouse harness retains the actual cause.
        val (_, _, schemas, qualified) = MvCommandHelper.collectSourceSchemas(spark, query)
        OpenIvmCompilers.forSession(spark).compile(CompileRequest(name, query, schemas, qualified))
        throw new IllegalStateException("OpenIVM compilation failed: " + name)
      }
      out.put("refresh_type", result.classification.refreshType)
      out.put("refresh_type_name", result.classification.refreshTypeName)
      out.put("classification_reason", result.classification.reason)
      out.put("compiled_sql", result.compiled.sql)
      out.put("initial_sql", if (result.classification.refreshType == 3 || result.compiled.initialLoadSql.isEmpty)
        query else LptsSparkDialect.translate(result.compiled.initialLoadSql))
      out.set[JsonNode]("output_schema", mapper.readTree(result.outputSchema.json))
      if (result.classification.refreshType == 4) {
        val predicate = MvCommandHelper.extractHavingPredicateSql(spark.sql(query).queryExecution.analyzed)
          .getOrElse(throw new IllegalStateException("Missing HAVING predicate: " + name))
        out.put("having_predicate", predicate)
      }
    } else {
      val schemaColumns = in.get("schemas").fields().asScala.map { entry =>
        entry.getKey -> schema(entry.getValue).fieldNames.toSeq
      }.toMap
      val qualified = schemaColumns.keys.map(n => n -> s"openivm_compile.$n").toMap
      val versions = in.get("source_versions").fields().asScala
        .map(e => s"openivm_compile.${e.getKey}" -> e.getValue.asLong()).toMap
      val rewritten = SparkRefreshRewriter.rewrite(
        compiledSql = in.get("compiled_sql").asText(), mvName = ident,
        mvLocation = "openivm-mv", viewLogicalName = name,
        sourceTempViews = Map.empty, viewDeltaPath = "openivm-view-delta",
        postProcess = LptsSparkDialect.translate, sourceSchemas = schemaColumns,
        sourceQualifiedNames = qualified, sourceSnapshotVersions = versions,
        mvVersionBeforeRefresh = Some(in.get("mv_version").asLong()))
      val statements = out.putArray("statements")
      rewritten.statements.foreach(s => statements.add(s))
    }
    mapper.writeValueAsString(out)
  }
}
