// Databricks notebook source
import org.apache.spark.sql.types.{StringType, DecimalType, StructField, StructType, DateType}
import org.apache.spark.sql.Row;

// COMMAND ----------

// MAGIC %md
// MAGIC ### Cargar los datos con su estructura original

// COMMAND ----------

val f00_schema = StructType(
  Array(
    StructField("POLIZA", StringType, true),
    StructField("RAMO", StringType, true),
    StructField("MODALIDAD", StringType, true),
    StructField("RAMO CONT.", StringType, true),
    StructField("MODALI. CONT.", StringType, true),
    StructField("N.RECIBO", StringType, true),
    StructField("IMP.PRIMA", StringType, true),
    StructField("IMP.BONIFIC.", StringType, true),
    StructField("IMP.IPS", StringType, true),
    StructField("IMP.CONSORC.", StringType, true),
    StructField("IMP.CLEA", StringType, true),
    StructField("IMP.ARB.", StringType, true),
    StructField("IMP.COMI.", StringType, true),
    StructField("IMP.RECIBO", StringType, true),
    StructField("F.EFECTO", StringType, true),
    StructField("F.VENCIM.", StringType, true),
    StructField("F.LIQ.CAR.", StringType, true), 
    StructField("F.LIQ.ANU.", StringType, true), 
    StructField("F.LIQ.COB.", StringType, true), 
    StructField("F.PAGO", StringType, true),
    StructField("TIPO DOC.", StringType, true),
    StructField("AGENCIA", StringType, true),
    StructField("T.MEDIADOR", StringType, true),
    StructField("MEDIADOR", StringType, true),
    StructField("C.LIQUIDACION", StringType, true),
    StructField("T.COMISION", StringType, true),
    StructField("IMP.DTO.RECIBO", StringType, true)
  )
)

// COMMAND ----------

// Cargar la tabla con una muestra con 166 de uno de los archivos input
//val file_location = "/FileStore/tables/sample.csv"
val file_location = "/FileStore/tables/F0_input/"
val f0_df = spark.read.option("header", "true").option("delimiter", "|").schema(f00_schema).csv(file_location)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Crear una copia del dataframe para agregar y editar 

// COMMAND ----------

import org.apache.spark.sql.functions.{lit, udf, trim, regexp_replace}
import org.apache.spark.sql.functions.column
import org.apache.spark.sql.functions.{col, desc}

// COMMAND ----------

import org.apache.spark.sql.functions.{when, _}
import spark.implicits._
import scala.math.Ordering.Implicits._
import org.apache.spark.sql.types.Decimal

// COMMAND ----------

val f0_df2 = f0_df.select(
  col("POLIZA").alias("COD_POLIZA"),
  col("RAMO").alias("COD_RAMO"),
  col("MODALIDAD").alias("COD_MODALIDAD"),
  col("`N.RECIBO`").alias("COD_RECIBO"),
  col("`F.EFECTO`").alias("FEC_MOV_ECON"),
  col("`F.LIQ.CAR.`").alias("FEC_EMI"),
  col("`TIPO DOC.`").alias("COD_TIP_RECIB"),
  col("`T.MEDIADOR`").alias("COD_AGENCIA"),
  col("MEDIADOR").alias("COD_MEDIADOR"),
  col("`C.LIQUIDACION`").alias("CON_LIQUIDACION"),
  col("`RAMO CONT.`").alias("TR_COD_RAMO_CONT"),
  col("`MODALI. CONT.`").alias("TR_COD_MOD_CONT"),
  // COD_TIP_GESTION
  when(col("`T.MEDIADOR`").isin("C", "E", "X"), lit("D"))
  .otherwise(lit("C")).alias("COD_TIP_GESTION"),
  regexp_replace(col("`IMP.PRIMA`"), ",", ".").cast(DecimalType(13,2)).alias("IMP_PRIM_TARIFA"),//*
  regexp_replace(col("`IMP.PRIMA`"), ",", ".").cast(DecimalType(13,2)).alias("IMP_PRIM_UNIC"),//*
  regexp_replace(col("`IMP.BONIFIC.`"), ",", ".").cast(DecimalType(13,2)).alias("BON_N_SINIESTRALIDAD"),
  regexp_replace(col("`IMP.IPS`"), ",", ".").cast(DecimalType(13,2)).alias("IPS"),
  regexp_replace(col("`IMP.CONSORC.`"), ",", ".").cast(DecimalType(13,2)).alias("TAS_CSS_REC_RIES_EXTR"),
  regexp_replace(col("`IMP.CLEA`"), ",", ".").cast(DecimalType(13,2)).alias("TAS_CSS_REC_FUN_LIQ"),
  regexp_replace(col("`IMP.ARB.`"), ",", ".").cast(DecimalType(13,2)).alias("ARB_BOMBEROS"),
  regexp_replace(col("`IMP.COMI.`"), ",", ".").cast(DecimalType(13,2)).alias("IMP_COMISIONES"),
  regexp_replace(col("`IMP.RECIBO`"), ",", ".").cast(DecimalType(13,2)).alias("IMP_TOT_RECIB"), //**
  regexp_replace(col("`IMP.RECIBO`"), ",", ".").cast(DecimalType(13,2)).alias("CAR_RECIB"), //**
  regexp_replace(col("`IMP.DTO.RECIBO`"), ",", ".").cast(DecimalType(13,2)).alias("IMP_DESCUENTO"),
  regexp_replace(col("`IMP.RECIBO`"), ",", ".").cast(DecimalType(13,2)).alias("IMP_PRIM_REAL"),//** 
  lit("SSL1").alias("COD_SOCIEDAD"),
  lit("").alias("COD_GARANTIA"),
  lit("").alias("COD_CERTIFICADO"),
  lit("N").alias("MAR_NUEV_RECIBO"),
  lit("EUR").alias("COD_DIVISA"),
  lit(Decimal(0)).alias("IMP_PRIM_PERIOD"),
  lit(Decimal(0)).alias("IMP_PRIM_EXTOR_EJER"),
  lit(Decimal(0)).alias("IMP_PRIM_EXTOR_EJER_ANT"),
  lit(Decimal(0)).alias("IMP_PRIM_ANUL_EJER_ANT"),
  lit(Decimal(0)).alias("TAS_CSS_PRIM_PEND_COBR"),
  lit(Decimal(0)).alias("IMP_RETEN_FISC"),
  lit("").alias("ID_MOV_ECON"),
  lit("N").alias("REC_FRACCIONADO"),
  lit("N").alias("MAR_RECIB_SIMUL"),
  lit("N").alias("COMP"),
  lit("N").alias("MAR_REINV"),
  lit("").alias("ENT_COLABORADORA"),
  lit("").alias("COD_TIP_SIN"),
  lit(Decimal(0)).alias("IMP_COA"),
  lit(Decimal(0)).alias("PRE_RECOBRADAS"),
  lit("Nombre Tomador 1").alias("NOM_DESTINATARIO"),
  lit("Comunidad de Madrid").alias("NOM_POBL"),
  lit("11223344A").alias("NIF"),
  lit("01112222330011223344").alias("CTA_BANC"),
  lit("Banco A").alias("ENT_BANCARIA"),
  lit("").alias("COD_EST_REC_ANT"),
// COD_EST_REC
when(col("`F.LIQ.CAR.`") <= col("`F.LIQ.COB.`") && col("`F.LIQ.ANU.`") <= col("`F.LIQ.COB.`") && col("`F.LIQ.COB.`") =!= "0001-01", lit("CB"))
//.when(col("`F.LIQ.CAR.`") <= col("`F.LIQ.COB.`") && col("`F.LIQ.ANU.`") =!= col("`F.LIQ.COB.`") && col("`F.LIQ.COB.`") === "0001-01", lit("DV"))
.when(col("`F.LIQ.CAR.`") <= col("`F.LIQ.ANU.`") && col("`F.LIQ.ANU.`") =!= "0001-01" && col("`F.LIQ.COB.`") === "0001-01", lit("DV"))
//.when(col("`F.LIQ.CAR.`") =!= col("`F.LIQ.ANU.`") && col("`F.LIQ.ANU.`") === "0001-01" && col("`F.LIQ.COB.`") === "0001-01", lit("PT"))
.when(col("`F.LIQ.CAR.`") =!= "0001-01" && col("`F.LIQ.ANU.`") === "0001-01" && col("`F.LIQ.COB.`") === "0001-01", lit("PT"))
.otherwise(lit("NA")).alias("COD_EST_REC"),
// Added just for generating new column
  col("`F.LIQ.COB.`"),
  col("`F.LIQ.ANU.`"),
  col("`F.LIQ.CAR.`")
)                      

// COMMAND ----------

val f0_df3 = f0_df2.withColumn("FEC_LIQ", when(col("COD_EST_REC") === "CB", col("`F.LIQ.COB.`"))
                                          .when(col("COD_EST_REC") === "DV", col("`F.LIQ.ANU.`"))
                                          .when(col("COD_EST_REC") === "PT", col("`F.LIQ.CAR.`"))
                                          .otherwise(lit("")))
.drop("F.LIQ.COB.", "F.LIQ.ANU.", "F.LIQ.CAR.") //DROP AUXILIAR COLUMNS

// COMMAND ----------

//CREATE TEMP VIEW
f0_df3.createOrReplaceTempView("f0Temp")

// COMMAND ----------

import spark.implicits._

// COMMAND ----------

val columns = spark.catalog.listColumns("default", "F0").select("name").as[String].collect()

// COMMAND ----------

spark.sql(s"INSERT OVERWRITE TABLE F0 SELECT ${columns.mkString(",")} FROM f0Temp");

// COMMAND ----------

//spark.table("F0").write.format("csv").mode("overwrite").save("/FileStore/tables/F0.csv")

// COMMAND ----------

// MAGIC %sql
// MAGIC --SELECT count(*) FROM F0

// COMMAND ----------


