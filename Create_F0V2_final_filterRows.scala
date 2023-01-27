// Databricks notebook source
import org.apache.spark.sql.types.{StringType, DecimalType, StructField, StructType, DateType}
import org.apache.spark.sql.Row;

// COMMAND ----------

// MAGIC %sql
// MAGIC set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;

// COMMAND ----------

// MAGIC %md
// MAGIC ### Crear estructura de la tabla F0 y guardarla en formato Delta

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS F0;
// MAGIC CREATE TABLE IF NOT EXISTS F0 (
// MAGIC COD_SOCIEDAD STRING,
// MAGIC COD_POLIZA STRING,
// MAGIC COD_RAMO STRING,
// MAGIC COD_MODALIDAD STRING,
// MAGIC COD_GARANTIA STRING,
// MAGIC COD_CERTIFICADO STRING,
// MAGIC COD_RECIBO STRING,
// MAGIC MAR_NUEV_RECIBO STRING,
// MAGIC COD_DIVISA STRING,
// MAGIC IMP_PRIM_TARIFA DECIMAL(13,2),
// MAGIC IMP_PRIM_UNIC DECIMAL(13,2),
// MAGIC IMP_PRIM_PERIOD DECIMAL(13,2),
// MAGIC IMP_PRIM_EXTOR_EJER DECIMAL(13,2),
// MAGIC IMP_PRIM_EXTOR_EJER_ANT DECIMAL(13,2),
// MAGIC IMP_PRIM_ANUL_EJER_ANT DECIMAL(13,2),
// MAGIC BON_N_SINIESTRALIDAD DECIMAL(13,2),
// MAGIC IPS DECIMAL(13,2),
// MAGIC TAS_CSS_REC_RIES_EXTR DECIMAL(13,2),
// MAGIC TAS_CSS_REC_FUN_LIQ DECIMAL(13,2),
// MAGIC TAS_CSS_PRIM_PEND_COBR DECIMAL(13,2), --estaba como INT
// MAGIC ARB_BOMBEROS DECIMAL(13,2),
// MAGIC IMP_COMISIONES DECIMAL(13,2),
// MAGIC IMP_RETEN_FISC DECIMAL(13,2),
// MAGIC IMP_TOT_RECIB DECIMAL(13,2),
// MAGIC CAR_RECIB STRING,
// MAGIC ID_MOV_ECON STRING,
// MAGIC FEC_MOV_ECON STRING,
// MAGIC FEC_EMI STRING,
// MAGIC FEC_LIQ STRING,
// MAGIC COD_TIP_RECIB STRING,
// MAGIC COD_EST_REC_ANT STRING,
// MAGIC COD_EST_REC STRING,
// MAGIC REC_FRACCIONADO STRING,
// MAGIC MAR_RECIB_SIMUL STRING,
// MAGIC COMP STRING,
// MAGIC MAR_REINV STRING,
// MAGIC ENT_COLABORADORA STRING,
// MAGIC COD_TIP_SIN STRING,
// MAGIC COD_AGENCIA STRING,
// MAGIC COD_MEDIADOR STRING,
// MAGIC COD_TIP_GESTION STRING,
// MAGIC CON_LIQUIDACION STRING,
// MAGIC IMP_DESCUENTO DECIMAL(13,2),
// MAGIC IMP_PRIM_REAL DECIMAL(13,2),
// MAGIC IMP_COA DECIMAL(13,2),
// MAGIC PRE_RECOBRADAS DECIMAL(13,2), -- INT
// MAGIC NOM_DESTINATARIO STRING,
// MAGIC NOM_POBL STRING,
// MAGIC NIF STRING,
// MAGIC CTA_BANC STRING,
// MAGIC ENT_BANCARIA STRING,
// MAGIC TR_COD_RAMO_CONT STRING,
// MAGIC TR_COD_MOD_CONT STRING)
// MAGIC USING DELTA
// MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)

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
val file_location = "/FileStore/tables/sample"
//val file_location = "/FileStore/tables/F0_input/"

val f0_df = sqlContext.read.format("csv")
.schema(f00_schema)
.option("header", "true")
.option("delimiter", "|")
.load(file_location)

display(f0_df)

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
  lit(0).alias("IMP_PRIM_PERIOD"),
  lit(0).alias("IMP_PRIM_EXTOR_EJER"),
  lit(0).alias("IMP_PRIM_EXTOR_EJER_ANT"),
  lit(0).alias("IMP_PRIM_ANUL_EJER_ANT"),
  lit(0).alias("TAS_CSS_PRIM_PEND_COBR"),
  lit(0).alias("IMP_RETEN_FISC"),
  lit("").alias("ID_MOV_ECON"),
  lit("N").alias("REC_FRACCIONADO"),
  lit("N").alias("MAR_RECIB_SIMUL"),
  lit("N").alias("COMP"),
  lit("N").alias("MAR_REINV"),
  lit("").alias("ENT_COLABORADORA"),
  lit("").alias("COD_TIP_SIN"),
  lit(0).alias("IMP_COA"),
  lit(0).alias("PRE_RECOBRADAS"),
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
  //.when(col("`F.LIQ.CAR.`") =!= col("`F.LIQ.ANU.`") && col("`F.LIQ.ANU.`")  === "0001-01" && col("`F.LIQ.COB.`") === "0001-01", lit("PT"))
  .when(col("`F.LIQ.CAR.`") =!= "0001-01" && col("`F.LIQ.ANU.`")  === "0001-01" && col("`F.LIQ.COB.`") === "0001-01", lit("PT"))
  .otherwise(lit("NA")).alias("COD_EST_REC"),
  // Added just for generating new column
  col("`F.LIQ.COB.`"),
  col("`F.LIQ.ANU.`"),
  col("`F.LIQ.CAR.`")
)

//display(f0_df2)

// COMMAND ----------

display(f0_df2)

// COMMAND ----------

f0_df2.select(col("COD_AGENCIA")).distinct().show()

// COMMAND ----------

val f0_df3 = f0_df2.withColumn("FEC_LIQ", when(col("COD_EST_REC") === "CB", col("`F.LIQ.COB.`"))
                                          .when(col("COD_EST_REC") === "DV", col("`F.LIQ.ANU.`"))
                                          .when(col("COD_EST_REC") === "PT", col("`F.LIQ.CAR.`"))
                                          .otherwise(lit("")))
.drop("F.LIQ.COB.", "F.LIQ.ANU.", "F.LIQ.CAR.") //DROP AUXILIAR COLUMNS



// COMMAND ----------

f0_df3.persist()

// COMMAND ----------

//01 y 06 corresponde con registros No vida
//04 cirresponde con registros Vida
//Hay más códigos pero con tener alguno de No vida y de Vida nos vale
val dfFilter = f0_df3.filter(col("COD_RAMO").isin(List("01", "06", "04"):_*))

// COMMAND ----------

display(f0_df3)

// COMMAND ----------

//Posibles valores COD_EST_REC
val a1 = List(col("COD_EST_REC") === lit("DV"))
val a2 = a1 ++ List(col("COD_EST_REC") === lit("PT"))
val a3 = a2 ++ List(col("COD_EST_REC") === lit("CB"))
val a = a3 ++ List(col("COD_EST_REC") === lit("NA"))

//Posibles valores COD_RAMO
val b1 = List(col("COD_RAMO").isin(List("01", "06"):_*))
val b = b1 ++ List(col("COD_RAMO").isin(List("04"):_*))

//Posibles valores COD_AGENCIA
val c1 = List(col("COD_AGENCIA") === lit("C"))
val c2 = c1 ++ List(col("COD_AGENCIA") === lit("A"))
val c3 = c2 ++ List(col("COD_AGENCIA") === lit("S"))
val c4 = c3 ++ List(col("COD_AGENCIA") === lit("X"))
val c = c4 ++ List(col("COD_AGENCIA") === lit("E"))

// COMMAND ----------

// Combinación de todo las casuísticas anteriores
val ab = a.flatMap(x => b.map(y => List(x, y)))
val abc = ab.flatMap(x => c.map(y => x :+ y))

// COMMAND ----------

//Filter filtrando por cada casuística compuesta en la lista anterior
import org.apache.spark.sql.DataFrame

//val limite = 5
val dfCasos: List[DataFrame] = abc.map(cond => {
  dfFilter.filter(cond.reduce(_ and _)).limit(limite)
})

val dfTodasCasuisticas= dfCasos.reduce(_ union _)

// COMMAND ----------

dfTodasCasuisticas.count()

// COMMAND ----------

display(dfTodasCasuisticas)

// COMMAND ----------

val f0_df3 = dfTodasCasuisticas

// COMMAND ----------

//CREATE TEMP VIEW
f0_df3.createOrReplaceTempView("f0Temp")

// COMMAND ----------

import spark.implicits._

// COMMAND ----------

val columns = spark.catalog.listColumns("default", "F0").select("name").as[String].collect()
println(columns)

// COMMAND ----------

f0_df3.select(columns.map(col):_*).schema

// COMMAND ----------

display(f0_df3)

// COMMAND ----------

// MAGIC %sql
// MAGIC SHOW CREATE TABLE F0

// COMMAND ----------

spark.sql(s"INSERT OVERWRITE TABLE F0 SELECT ${columns.mkString(",")} FROM f0Temp");

// COMMAND ----------

spark.table("F0").write.format("csv").mode("overwrite").save("/FileStore/tables/F0.csv")

// COMMAND ----------

spark.table("F0").repartition(1).write.option("header","true").format("csv").mode("overwrite").save("/FileStore/tables/F0_limit")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM f0
