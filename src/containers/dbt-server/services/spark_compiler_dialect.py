"""Spark function spellings shared with the pinned Spark-to-DuckDB bridge."""
from sqlglot import exp
from sqlglot.dialects.spark import Spark
from sqlglot.dialects.databricks import Databricks


class _CompilerFunctions:
    def strposition_sql(self, expression):
        # INSTR keeps argument order across both engines. Preserve explicit
        # start-position calls rather than changing their edge semantics.
        if expression.args.get('position') is None:
            return self.func('INSTR',expression.this,expression.args['substr'])
        return self._original_position(self,expression)

    def ignorenulls_sql(self, expression):
        if isinstance(expression.this,(exp.FirstValue,exp.LastValue)):
            return self.func(expression.this.sql_name(),expression.this.this,exp.true())
        return super().ignorenulls_sql(expression)

    def respectnulls_sql(self, expression):
        if isinstance(expression.this,(exp.FirstValue,exp.LastValue)):
            return self.sql(expression,'this')
        return super().respectnulls_sql(expression)


class CompilerSpark(Spark):
    class Generator(_CompilerFunctions,Spark.Generator):
        _original_position = staticmethod(Spark.Generator.TRANSFORMS[exp.StrPosition])
        TRANSFORMS = {**Spark.Generator.TRANSFORMS,
                      exp.StrPosition:lambda generator,expression:generator.strposition_sql(expression)}


class CompilerDatabricks(Databricks):
    class Generator(_CompilerFunctions,Databricks.Generator):
        _original_position = staticmethod(Databricks.Generator.TRANSFORMS[exp.StrPosition])
        TRANSFORMS = {**Databricks.Generator.TRANSFORMS,
                      exp.StrPosition:lambda generator,expression:generator.strposition_sql(expression)}
