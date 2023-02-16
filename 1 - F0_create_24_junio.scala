// Databricks notebook source
// MAGIC %scala
// MAGIC import org.apache.spark.sql.types.{StringType, DecimalType, StructField, StructType, DateType}
// MAGIC import org.apache.spark.sql.Row;
// MAGIC import org.apache.spark.sql.functions.{input_file_name, current_timestamp}
// MAGIC import org.apache.spark.sql.streaming.Trigger
// MAGIC import spark.implicits._

// COMMAND ----------

// MAGIC %sql
// MAGIC set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;

// COMMAND ----------

// MAGIC %md
// MAGIC ### Cargar los datos con su estructura original

// COMMAND ----------

// MAGIC %scala
// MAGIC //Crea la estructura de f00
// MAGIC val f00_schema = StructType(
// MAGIC   Array(
// MAGIC     StructField("POLIZA", StringType, true),
// MAGIC     StructField("RAMO", StringType, true),
// MAGIC     StructField("MODALIDAD", StringType, true),
// MAGIC     StructField("RAMO_CONT.", StringType, true),
// MAGIC     StructField("MODALI.CONT.", StringType, true),
// MAGIC     StructField("N.RECIBO", StringType, true),
// MAGIC     StructField("IMP.PRIMA", StringType, true),
// MAGIC     StructField("IMP.BONIFIC.", StringType, true),
// MAGIC     StructField("IMP.IPS", StringType, true),
// MAGIC     StructField("IMP.CONSORC.", StringType, true),
// MAGIC     StructField("IMP.CLEA", StringType, true),
// MAGIC     StructField("IMP.ARB.", StringType, true),
// MAGIC     StructField("IMP.COMI.", StringType, true),
// MAGIC     StructField("IMP.RECIBO", StringType, true),
// MAGIC     StructField("F.EFECTO", StringType, true),
// MAGIC     StructField("F.VENCIM.", StringType, true),
// MAGIC     StructField("F.LIQ.CAR.", StringType, true), 
// MAGIC     StructField("F.LIQ.ANU.", StringType, true), 
// MAGIC     StructField("F.LIQ.COB.", StringType, true), 
// MAGIC     StructField("F.PAGO", StringType, true),
// MAGIC     StructField("TIPO_DOC.", StringType, true),
// MAGIC     StructField("AGENCIA", StringType, true),
// MAGIC     StructField("T.MEDIADOR", StringType, true),
// MAGIC     StructField("MEDIADOR", StringType, true),
// MAGIC     StructField("C.LIQUIDACION", StringType, true),
// MAGIC     StructField("T.COMISION", StringType, true),
// MAGIC     StructField("IMP.DTO.RECIBO", StringType, true)
// MAGIC   )
// MAGIC )

// COMMAND ----------


//READING CHANGES IN BATCH QUERIES
// Cargar la tabla con una muestra con 166 de uno de los archivos input
//val file_location = "/FileStore/tables/2021_04_recibos_sg"
//val file_location = "/FileStore/tables/F0_input/"
//val f0_df = spark.read.option("header", "true").options(Map("delimiter"->"|")).option("startingVersion", 0).option("endingVersion", 10).schema(f00_schema).csv(file_location)

// COMMAND ----------

val f0path = "/FileStore/tables/samples"
//val username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first.get(0)
val checkpoint_path = "/tmp/_checkpoint"
val table_name = "F0"

// Clear out data from previous demo execution
spark.sql(s"DROP TABLE IF EXISTS F0")

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .schema(f00_schema)
    .load(f0path)
    .writeStream
  .option("checkpointLocation", checkpoint_path)
    .format("delta")
    .outputMode("append")
//   .trigger(once=True)
//   .trigger(processingTime="5 seconds")
//   .option("checkpointLocation", userhome + "/_bronze_checkpoint")
//   .start(bronzePath)
 .trigger(Trigger.AvailableNow)
 .toTable(table_name)
)



// COMMAND ----------

// MAGIC %scala
// MAGIC //READING CHANGES IN STREAMING QUERIES
// MAGIC // Cargar la tabla con una muestra con 166 de uno de los archivos input
// MAGIC //val file_location = "/FileStore/tables/xab"
// MAGIC //val file_location = "/FileStore/tables/F0_input/"
// MAGIC // not providing a starting version/timestamp will result in the latest snapshot being fetched first
// MAGIC //val f0_df = spark.read.option("header", "true").options(Map("delimiter"->"|")).schema(f00_schema).csv(file_location)

// COMMAND ----------

// MAGIC %scala
// MAGIC val f0_df = spark.read.table(table_name)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Crear una copia del dataframe para agregar y editar 

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.functions.{lit, udf, trim, regexp_replace}
// MAGIC import org.apache.spark.sql.functions.column
// MAGIC import org.apache.spark.sql.functions.{col, desc}

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.functions.{when, _}
// MAGIC import spark.implicits._
// MAGIC import scala.math.Ordering.Implicits._
// MAGIC import org.apache.spark.sql.types.Decimal

// COMMAND ----------

// MAGIC %scala
// MAGIC val f0_df2 = f0_df.select(
// MAGIC   //Cambios de nombres 
// MAGIC   col("POLIZA").alias("COD_POLIZA"),
// MAGIC   col("RAMO").alias("COD_RAMO"),
// MAGIC   col("MODALIDAD").alias("COD_MODALIDAD"),
// MAGIC   col("`N.RECIBO`").alias("COD_RECIBO"),
// MAGIC   col("`F.EFECTO`").alias("FEC_MOV_ECON"),
// MAGIC   col("`F.LIQ.CAR.`").alias("FEC_EMI"),
// MAGIC   col("`TIPO_DOC.`").alias("COD_TIP_RECIB"),
// MAGIC   col("`T.MEDIADOR`").alias("COD_AGENCIA"),
// MAGIC   col("MEDIADOR").alias("COD_MEDIADOR"),
// MAGIC   col("`C.LIQUIDACION`").alias("CON_LIQUIDACION"),
// MAGIC   col("`RAMO_CONT.`").alias("TR_COD_RAMO_CONT"),
// MAGIC   col("`MODALI.CONT.`").alias("TR_COD_MOD_CONT"),
// MAGIC   // COD_TIP_GESTION cmabia los códgios siguiendo las condiciones 
// MAGIC   when(col("`T.MEDIADOR`").isin("C", "E", "X"), lit("D"))
// MAGIC   .otherwise(lit("C")).alias("COD_TIP_GESTION"),
// MAGIC   regexp_replace(col("`IMP.PRIMA`"), ",", ".").cast(DecimalType(13,2)).alias("IMP_PRIM_TARIFA"),//*
// MAGIC   regexp_replace(col("`IMP.PRIMA`"), ",", ".").cast(DecimalType(13,2)).alias("IMP_PRIM_UNIC"),//*
// MAGIC   regexp_replace(col("`IMP.BONIFIC.`"), ",", ".").cast(DecimalType(13,2)).alias("BON_N_SINIESTRALIDAD"),
// MAGIC   regexp_replace(col("`IMP.IPS`"), ",", ".").cast(DecimalType(13,2)).alias("IPS"),
// MAGIC   regexp_replace(col("`IMP.CONSORC.`"), ",", ".").cast(DecimalType(13,2)).alias("TAS_CSS_REC_RIES_EXTR"),
// MAGIC   regexp_replace(col("`IMP.CLEA`"), ",", ".").cast(DecimalType(13,2)).alias("TAS_CSS_REC_FUN_LIQ"),
// MAGIC   regexp_replace(col("`IMP.ARB.`"), ",", ".").cast(DecimalType(13,2)).alias("ARB_BOMBEROS"),
// MAGIC   regexp_replace(col("`IMP.COMI.`"), ",", ".").cast(DecimalType(13,2)).alias("IMP_COMISIONES"),
// MAGIC   regexp_replace(col("`IMP.RECIBO`"), ",", ".").cast(DecimalType(13,2)).alias("IMP_TOT_RECIB"), //**
// MAGIC   regexp_replace(col("`IMP.RECIBO`"), ",", ".").cast(DecimalType(13,2)).alias("CAR_RECIB"), //**
// MAGIC   regexp_replace(col("`IMP.DTO.RECIBO`"), ",", ".").cast(DecimalType(13,2)).alias("IMP_DESCUENTO"),
// MAGIC   regexp_replace(col("`IMP.RECIBO`"), ",", ".").cast(DecimalType(13,2)).alias("IMP_PRIM_REAL"),//** 
// MAGIC   lit("SSL1").alias("COD_SOCIEDAD"),
// MAGIC   lit("").alias("COD_GARANTIA"),
// MAGIC   lit("").alias("COD_CERTIFICADO"),
// MAGIC   lit("N").alias("MAR_NUEV_RECIBO"),
// MAGIC   lit("EUR").alias("COD_DIVISA"),
// MAGIC   lit(Decimal(0)).alias("IMP_PRIM_PERIOD"),
// MAGIC   lit(Decimal(0)).alias("IMP_PRIM_EXTOR_EJER"),
// MAGIC   lit(Decimal(0)).alias("IMP_PRIM_EXTOR_EJER_ANT"),
// MAGIC   lit(Decimal(0)).alias("IMP_PRIM_ANUL_EJER_ANT"),
// MAGIC   lit(Decimal(0)).alias("TAS_CSS_PRIM_PEND_COBR"),
// MAGIC   lit(Decimal(0)).alias("IMP_RETEN_FISC"),
// MAGIC   lit("").alias("ID_MOV_ECON"),
// MAGIC   lit("N").alias("REC_FRACCIONADO"),
// MAGIC   lit("N").alias("MAR_RECIB_SIMUL"),
// MAGIC   lit("N").alias("COMP"),
// MAGIC   lit("N").alias("MAR_REINV"),
// MAGIC   lit("").alias("ENT_COLABORADORA"),
// MAGIC   lit("").alias("COD_TIP_SIN"),
// MAGIC   lit(Decimal(0)).alias("IMP_COA"),
// MAGIC   lit(Decimal(0)).alias("PRE_RECOBRADAS"),
// MAGIC   lit("Nombre Tomador 1").alias("NOM_DESTINATARIO"),
// MAGIC   lit("Comunidad de Madrid").alias("NOM_POBL"),
// MAGIC   lit("11223344A").alias("NIF"),
// MAGIC   lit("01112222330011223344").alias("CTA_BANC"),
// MAGIC   lit("Banco A").alias("ENT_BANCARIA"),
// MAGIC   lit("").alias("COD_EST_REC_ANT"),
// MAGIC // COD_EST_REC
// MAGIC when(col("`F.LIQ.CAR.`") <= col("`F.LIQ.COB.`") && col("`F.LIQ.ANU.`") <= col("`F.LIQ.COB.`") && col("`F.LIQ.COB.`") =!= "0001-01", lit("CB"))
// MAGIC .when(col("`F.LIQ.CAR.`") <= col("`F.LIQ.ANU.`") && col("`F.LIQ.ANU.`") =!= "0001-01" && col("`F.LIQ.COB.`") === "0001-01", lit("DV"))
// MAGIC .when(col("`F.LIQ.CAR.`") =!= "0001-01" && col("`F.LIQ.ANU.`") === "0001-01" && col("`F.LIQ.COB.`") === "0001-01", lit("PT"))
// MAGIC .otherwise(lit("NA")).alias("COD_EST_REC"),
// MAGIC // Added just for generating new column
// MAGIC   col("`F.LIQ.COB.`"),
// MAGIC   col("`F.LIQ.ANU.`"),
// MAGIC   col("`F.LIQ.CAR.`")
// MAGIC )                      

// COMMAND ----------

// MAGIC %scala
// MAGIC //display(f0_df2)

// COMMAND ----------

// MAGIC %scala
// MAGIC //más cambio de nombres
// MAGIC val f0_df3 = f0_df2.withColumn("FEC_LIQ", when(col("COD_EST_REC") === "CB", col("`F.LIQ.COB.`"))
// MAGIC                                           .when(col("COD_EST_REC") === "DV", col("`F.LIQ.ANU.`"))
// MAGIC                                           .when(col("COD_EST_REC") === "PT", col("`F.LIQ.CAR.`"))
// MAGIC                                           .otherwise(lit("")))
// MAGIC .drop("F.LIQ.COB.", "F.LIQ.ANU.", "F.LIQ.CAR.") //DROP AUXILIAR COLUMNS

// COMMAND ----------

// MAGIC %scala
// MAGIC //display(f0_df3)

// COMMAND ----------

// MAGIC %scala
// MAGIC //CREATE TEMP VIEW
// MAGIC f0_df3.createOrReplaceTempView("f0Temp")

// COMMAND ----------

// MAGIC %scala
// MAGIC import spark.implicits._

// COMMAND ----------

// MAGIC %scala
// MAGIC val columns = spark.catalog.listColumns("default", "F0").select("name").as[String].collect()

// COMMAND ----------

// MAGIC %scala
// MAGIC //spark.readStream.format("delta").option("readChangeFeed", "true").table("F0")

// COMMAND ----------

// MAGIC %scala
// MAGIC //spark.sql(s"INSERT OVERWRITE TABLE F0 SELECT ${columns.mkString(",")} FROM f0Temp");

// COMMAND ----------

// MAGIC %scala
// MAGIC //spark.table("F0").write.format("csv").mode("overwrite").save("/FileStore/tables/F0.csv")

// COMMAND ----------

val file_location = "/FileStore/tables/xac"
//val file_location = "/FileStore/tables/F0_input/"
// not providing a starting version/timestamp will result in the latest snapshot being fetched first
val df_nuevo = spark.read.option("header", "true").options(Map("delimiter"->"|")).schema(f00_schema).csv(file_location)

// COMMAND ----------

spark.createDataFrame(df_nuevo, columns).write.format("delta").mode("append").saveAsTable("F0")


// COMMAND ----------

//spark.sql(s"INSERT OVERWRITE TABLE F0 SELECT ${columns.mkString(",")} FROM f0Temp");

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT count(*) FROM F0
