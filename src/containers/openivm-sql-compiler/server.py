"""Schema-only Spark/OpenIVM compiler bridge; never executes workload data."""

import json
from flask import Flask, jsonify, request
from pyspark.sql import SparkSession

app = Flask(__name__)
spark = SparkSession.builder.master("local[1]").appName("openivm-compiler-gateway").getOrCreate()


@app.get("/health")
def health():
    return jsonify(status="ok")


@app.post("/compile")
def compile_sql():
    try:
        result = spark._jvm.org.openivm.spark.commands.BenchSqlCompiler.run(json.dumps(request.get_json()))
        return jsonify(json.loads(result))
    except Exception as exc:
        app.logger.exception("OpenIVM SQL compiler failed")
        return jsonify(status="error", error=str(exc)), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8998, threaded=False)
