// Databricks notebook source
// MAGIC %sql
// MAGIC set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;

// COMMAND ----------

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
//F1_defintiivo se guarda como F1
F1_definitivo.write.format("delta").mode("overwrite").option("readChangeFeed", "true").option("startingVersion", 0).saveAsTable("F1")
//F1_definitivo.repartition(1).write.option("header","true").format("csv").mode("overwrite").save("/FileStore/tables/F1/F1")

// COMMAND ----------

columnsF1

// COMMAND ----------

/*import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

val changeDataFeed = spark.readStream.format("csv").schema(f00_schema).load("/FileStore/tables/xac-1")*/


// COMMAND ----------

// Write the change data feed to the temporary table
/*changeDataFeed
  .writeStream
  .foreachBatch { (df: Dataset[Row], batchId: Long) =>
    // Write the batch DataFrame to the temporary table
    df.write
      .format("csv")
      .option("path", "/FileStore/tables/")
      .mode("append")
      .save()
  }
  .outputMode("append")
  .option("checkpointLocation", checkPointDir)
  .trigger(Trigger.ProcessingTime("10 seconds"))
  .start()
  .awaitTermination()*/

// COMMAND ----------

/*val mergeStatement = s"""
  UPDATE F1
  SET TR_ID_EVNEG=t.TR_ID_EVNEG, COD_SOCIEDAD=t.COD_SOCIEDAD, COD_POLIZA=t.COD_POLIZA, COD_RAMO=t.COD_RAMO, COD_MODALIDAD=t.COD_MODALIDAD, =t.COD_GARANTIA, COD_CERTIFICADO=t.COD_CERTIFICADO, COD_RECIBO=t.COD_RECIBO, MAR_NUEV_RECIBO=t.MAR_NUEV_RECIBO, COD_DIVISA=t.COD_DIVISA, IMP_PRIM_TARIFA=t.IMP_PRIM_TARIFA, IMP_PRIM_UNIC=t.IMP_PRIM_UNIC, IMP_PRIM_PERIOD=t.IMP_PRIM_PERIOD, IMP_PRIM_EXTOR_EJER=t.IMP_PRIM_EXTOR_EJER, IMP_PRIM_EXTOR_EJER_ANT=t.IMP_PRIM_EXTOR_EJER_ANT, IMP_PRIM_ANUL_EJER_ANT=t.IMP_PRIM_ANUL_EJER_ANT, BON_N_SINIESTRALIDAD=t.BON_N_SINIESTRALIDAD, IPS=t.IPS, TAS_CSS_REC_RIES_EXTR=t.TAS_CSS_REC_RIES_EXTR, TAS_CSS_REC_FUN_LIQ=t.TAS_CSS_REC_FUN_LIQ, TAS_CSS_PRIM_PEND_COBR=t.TAS_CSS_PRIM_PEND_COBR, ARB_BOMBEROS=t.ARB_BOMBEROS, IMP_COMISIONES=t.IMP_COMISIONES, IMP_RETEN_FISC=t.IMP_RETEN_FISC, IMP_TOT_RECIB=t.IMP_TOT_RECIB, CAR_RECIB=t.CAR_RECIB, ID_MOV_ECON=t.ID_MOV_ECON, FEC_MOV_ECON=t.FEC_MOV_ECON, FEC_EMI=t.FEC_EMI, FEC_LIQ=t.FEC_LIQ, COD_TIP_RECIB=t.COD_TIP_RECIB, COD_EST_REC_ANT=t.COD_EST_REC_ANT, COD_EST_REC=t.COD_EST_REC, REC_FRACCIONADO=t.REC_FRACCIONADO, MAR_RECIB_SIMUL=t.MAR_RECIB_SIMUL, COMP=t.COMP, MAR_REINV=t.MAR_REINV, ENT_COLABORADORA=t.ENT_COLABORADORA, COD_TIP_SIN=t.COD_TIP_SIN, COD_AGENCIA=t.COD_AGENCIA, COD_MEDIADOR=t.COD_MEDIADOR, COD_TIP_GESTION=t.COD_TIP_GESTION, CON_LIQUIDACION=t.CON_LIQUIDACION, IMP_DESCUENTO=t.IMP_DESCUENTO, IMP_PRIM_REAL=t.IMP_PRIM_REAL, IMP_COA=t.IMP_COA, PRE_RECOBRADAS=t.PRE_RECOBRADAS, NOM_DESTINATARIO=t.NOM_DESTINATARIO, NOM_POBL=t.NOM_POBL, NIF=t.NIF, CTA_BANC=t.CTA_BANC, ENT_BANCARIA=t.ENT_BANCARIA, TR_COD_RAMO_CONT=t.TR_COD_RAMO_CONT, TR_COD_MOD_CONT=t.TR_COD_MOD_CONT, TR_COD_TIP_RECIB=.TR_COD_TIP_RECIB, TR_COD_EST_RECIB=t.TR_COD_EST_RECIB, TR_COD_AGENCIA=t.TR_COD_AGENCIA, TR_NEGOCIO=t.TR_NEGOCIO, TR_COD_TIP_NEGOCIO=t.TR_COD_TIP_NEGOCIO
  FROM F1_definitivo t
  WHERE F1.COD_POLIZA = t.COD_POLIZA
"""

spark.sql(mergeStatement)*/


// COMMAND ----------

display(F1_definitivo)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM F1
