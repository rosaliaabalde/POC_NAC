// Databricks notebook source
import org.apache.spark.sql.types.{StringType, DecimalType, StructField, StructType, DateType}
import spark.implicits._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lit, udf, trim, regexp_replace}
import org.apache.spark.sql.functions.column
import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.functions.{when, _}
import scala.math.Ordering.Implicits._
import org.apache.spark.sql.types.Decimal

// COMMAND ----------

// Carga de tabla TR_RAMO_POL
val TR_RAMO_POL_df = spark.table("TR_RAMO_POL")

// COMMAND ----------

// Carga de F0
val F0_df = spark.table("default.F0")

// COMMAND ----------

// Transformación nombres TR_COD_TIP_RECIB
val F0_1_df = F0_df.withColumn("TR_COD_TIP_RECIB",
                               when(col("COD_TIP_RECIB") === "RPA", lit("RPRI"))
                               .otherwise(col("COD_TIP_RECIB")))

// COMMAND ----------

display(F0_1_df)

// COMMAND ----------

// Transformación nombres TR_COD_EST_RECIB
val F0_2_df = F0_1_df.withColumn("TR_COD_EST_RECIB", 
                                  when(col("COD_EST_REC") === "CB", lit("COB"))
                                  .when(col("COD_EST_REC") === "DV", lit("DEV"))
                                  .when(col("COD_EST_REC") === "PT", lit("PTE"))
                                  .otherwise(col("COD_EST_REC")))

// COMMAND ----------

display(F0_2_df)

// COMMAND ----------

//Tranformación nombres TR_COD_AGENCIA

val F0_3_df  = F0_2_df.withColumn("TR_COD_AGENCIA", 
                                  when(col("COD_AGENCIA") === "C", lit("CORR"))
                                  .when(col("COD_AGENCIA") === "E", lit("ECOL"))
                                  .when(col("COD_AGENCIA") === "X", lit("AGEX"))
                                  .when(col("COD_AGENCIA") === "L", lit("SLU"))
                                  .when(col("COD_AGENCIA") === "S", lit("SLU"))
                                  .when(col("COD_AGENCIA") === "U", lit("SLU"))
                                  .otherwise(col("COD_AGENCIA")))

// COMMAND ----------

display(F0_3_df)

// COMMAND ----------

// Creación de una tabla F1 con la adición de las columnas TR_NEGOCIO y TR_TIP_NEGOCIO a partir de un join
val F1_df = F0_3_df.join(TR_RAMO_POL_df, F0_3_df("COD_RAMO").cast("Integer") === TR_RAMO_POL_df("RAMO_POL").cast("Integer") && F0_3_df("COD_MODALIDAD").cast("Integer") === TR_RAMO_POL_df("MODALIDAD_POL").cast("Integer"), "left").select(F0_3_df("*"), TR_RAMO_POL_df("Negocio").alias("TR_NEGOCIO"), TR_RAMO_POL_df("Tipo_de_Negocio").alias("TR_COD_TIP_NEGOCIO"))
  //Se añade ID incremental
  .withColumn("TR_ID_EVNEG", monotonically_increasing_id().cast(StringType))

// COMMAND ----------

display(F1_df)

// COMMAND ----------

//Seleccion campos para tabla f1 e insertar en tabla
val columnsF1 = spark.catalog.listColumns("default", "f1").select("name").as[String].collect()
val F1_definitivo = F1_df.select(columnsF1.map(col):_*)
F1_definitivo.write.format("delta").mode("overwrite").saveAsTable("F1")
//F1_definitivo.repartition(1).write.option("header","true").format("csv").mode("overwrite").save("/FileStore/tables/F1/F1")

// COMMAND ----------

display(F1_definitivo)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM F1
