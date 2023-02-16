// Databricks notebook source
// MAGIC %sql
// MAGIC set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;

// COMMAND ----------

import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.types.DecimalType


// COMMAND ----------

val defaultColumnsF1 = List("TR_ID_EVNEG", "COD_SOCIEDAD", "COD_DIVISA", "CON_LIQUIDACION", "COD_MEDIADOR", "TR_COD_AGENCIA")
val defaultColumnsF2 = List("TR_ID_EVNEG", "TR_COD_TIP_EVCON", "TR_ID_EVCON", "TR_NAT_EVCON", "TR_CUENTA_CONT", "TR_CC_IMP", "TR_COD_RAMO_CONT", "TR_COD_MOD_CONT")
val f1_df = spark.table("default.f1").select(defaultColumnsF1.map(col):_*)
val f2_cont_df = spark.table("default.f2_cont").select(defaultColumnsF2.map(col):_*)

val PS_df = f2_cont_df.join(f1_df, "TR_ID_EVNEG")
//TODO SELECT DE CAMPOS NO COMUNES

display(PS_df)

// COMMAND ----------

PS_df.count()
//PS_df.printSchema
//display(PS_df)

// COMMAND ----------

val groupByColumns = List("COD_SOCIEDAD", "TR_COD_TIP_EVCON", "TR_ID_EVCON", "COD_DIVISA", "CON_LIQUIDACION", "TR_NAT_EVCON", "COD_MEDIADOR", "TR_CUENTA_CONT", "TR_COD_AGENCIA", "TR_COD_RAMO_CONT", "TR_COD_MOD_CONT")
val PS_agg = PS_df.groupBy(groupByColumns.map(col):_*)
.agg(sum("TR_CC_IMP").cast(DecimalType(13,2)).as("TR_CC_IMP"))

PS_agg.printSchema

// COMMAND ----------

//Esto ya lo hace directamente al crear PS_agg
//val PS_agg = PS_agg0.withColumn("TR_CC_IMP", col("TR_CC_IMP").cast(DecimalType(13,2)))

// COMMAND ----------

val todayDate = DateTimeFormatter.ofPattern("ddMMyyyy").format(LocalDateTime.now)

 
val PS_df_ren = PS_agg.select(
col("COD_SOCIEDAD"),
col("TR_COD_TIP_EVCON").alias("CLASE_DOC"),
col("TR_ID_EVCON").alias("REFER"),
col("COD_DIVISA").alias("DIV"),
col("CON_LIQUIDACION").alias("TEX_CABECERA"),
lit(todayDate).alias("FECHA_CONTAB"),
col("TR_NAT_EVCON").alias("CLAVE_CONTAB"),
col("COD_MEDIADOR").alias("TERC"),
lit("").alias("NOM_1"),
lit("").alias("NOM_2"),
lit("").alias("POBL"),
lit("").alias("NIF"),
col("TR_CUENTA_CONT").alias("NUM_CUENTA"),
col("TR_CC_IMP").alias("IMP"),
col("TR_COD_AGENCIA").alias("DIVIS"),
lit("").alias("C_COSTE"),
lit("").alias("ASIG"),
lit("").alias("TEXT"),
col("TR_COD_RAMO_CONT").alias("RAMO_CON"),
lit("").alias("CANL"),
lit("").alias("INDICADOR_CME"),
col("TR_COD_MOD_CONT").alias("MODALIDAD_CON"))

// COMMAND ----------

PS_df_ren.write.format("delta").mode("overwrite").option("readChangeFeed", "true").option("startingVersion", 0).saveAsTable("ps")

// COMMAND ----------

//PS_df_ren.coalesce(1).write.option("header","true").format("csv").mode("overwrite").save("/FileStore/tables/PS.csv")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM ps

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY F0

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY f1

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY f2_cont

// COMMAND ----------

spark.readStream.format("delta")
  .option("readChangeFeed", "true")
  .table("f2_cont")

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY ps

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM table_changes("ps", 0, 10)
