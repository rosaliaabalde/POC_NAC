// Databricks notebook source
// MAGIC %md
// MAGIC ### Tabla Auxiliar para F1

// COMMAND ----------

// MAGIC %sql
// MAGIC set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;

// COMMAND ----------

import org.apache.spark.sql.types.{StringType, IntegerType, DecimalType, StructField, StructType, DateType}

// COMMAND ----------

spark.sql("DROP TABLE IF EXISTS TR_RAMO_POL")

// COMMAND ----------


val TR_RAMO_POL_raw = spark.read.option("header", "true").option("delimiter", ",").csv("/FileStore/tables/F1/TR_RAMO_POL.csv").select("RAMO_POL", "MODALIDAD_POL","Negocio", "Tipo de Negocio").distinct()
val TR_RAMO_POL = TR_RAMO_POL_raw.withColumnRenamed("Tipo de negocio", "Tipo_de_negocio").na.fill("")
TR_RAMO_POL.write.format("delta").saveAsTable("TR_RAMO_POL")

// COMMAND ----------

display(TR_RAMO_POL)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Tablas Auxiliares EVENTOS F2_cont

// COMMAND ----------

//se borran si existen
spark.sql("DROP TABLE IF EXISTS maestro_eventos_contables")
spark.sql("DROP TABLE IF EXISTS maestro_eventos_negocio")
spark.sql("DROP TABLE IF EXISTS relacion_eventos")

// COMMAND ----------

//se cargan las tablas
val maestro_eventos_contables = spark.read.option("header", true).option("delimiter", ",").csv("/FileStore/tables/F2_parametricas/maestro_eventos_contables.csv")
maestro_eventos_contables.write.format("delta").saveAsTable("maestro_eventos_contables")

val maestro_eventos_negocio = spark.read.option("header", "true").option("delimiter", ",").csv("/FileStore/tables/F2_parametricas/maestro_eventos_negocio.csv")
maestro_eventos_negocio.write.format("delta").saveAsTable("maestro_eventos_negocio")

val relacion_eventos = spark.read.option("header", "true").option("delimiter", ",").csv("/FileStore/tables/F2_parametricas/relacion_eventos.csv")
relacion_eventos.write.format("delta").saveAsTable("relacion_eventos")

// COMMAND ----------

display(maestro_eventos_contables)

// COMMAND ----------

display(maestro_eventos_negocio)

// COMMAND ----------

display(relacion_eventos)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Tablas Auxiliares ASIENTOS F2_cont

// COMMAND ----------

//se borran si existen las tablas
spark.sql("DROP TABLE IF EXISTS asientos_cuentas_contables")
spark.sql("DROP TABLE IF EXISTS asientos_estructura_basica")
spark.sql("DROP TABLE IF EXISTS asientos_importes")
spark.sql("DROP TABLE IF EXISTS maestro_cuentas_contables")

// COMMAND ----------

// se crea el esquema de asientos contables
val schemaAsientosContables = StructType(
  Array(
    StructField("COD_SOCIEDAD", StringType, true),
    StructField("ID_CUEN_ASIEN", StringType, true),
    StructField("ID_ESTRUCTURA", StringType, true),
    StructField("COD_ASIENTO", StringType, true),
    StructField("NATURALEZA", IntegerType, true),
    StructField("NOM_CAMPO", StringType, true),
    StructField("VALOR", StringType, true),
    StructField("PADRE", StringType, true),
    StructField("COD_CUENTA", DecimalType(10,0), true),
  )
)
// se crea el esquema de maestro contable
val schemaMaestroContables = StructType(
  Array(
    StructField("ID_MAE_CUENTA", StringType, true),
    StructField("COD_SOCIEDAD", StringType, true),
    StructField("NUM_CUENTA", DecimalType(10,0), true),
    StructField("DESC_CUENTA", StringType, true),
    StructField("PADRE", StringType, true),
    StructField("IND_TIPO", StringType, true),
    StructField("TABLA_AUX", StringType, true),
    StructField("CAMPO_AUX", StringType, true),
  )
)

// COMMAND ----------

// He vuelto a cargar el archivo asientos_cuentas_contables con el nombre corregido, ..que antes ponia asiento_cuentas_contablas

val asientos_cuentas_contables = spark.read.option("header", true).option("delimiter", ",").schema(schemaAsientosContables).csv("/FileStore/tables/F2_parametricas/asientos_cuentas_contables.csv")
asientos_cuentas_contables.write.format("delta").saveAsTable("asientos_cuentas_contables")

val asientos_estructura_basica = spark.read.option("header", "true").option("delimiter", ";").csv("/FileStore/tables/F2_parametricas/asientos_estructura_basica.csv")
asientos_estructura_basica.write.format("delta").saveAsTable("asientos_estructura_basica")

val asientos_importes = spark.read.option("header", "true").option("delimiter", ";").csv("/FileStore/tables/F2_parametricas/asientos_importes.csv")
asientos_importes.write.format("delta").saveAsTable("asientos_importes")

val maestro_cuentas_contables = spark.read.option("header", true).option("delimiter", ",").schema(schemaMaestroContables).option("encoding", "ISO-8859-1").csv("/FileStore/tables/F2_parametricas/maestro_cuentas_contables.csv")
maestro_cuentas_contables.write.format("delta").saveAsTable("maestro_cuentas_contables")

// COMMAND ----------

display(asientos_cuentas_contables)

// COMMAND ----------

display(asientos_estructura_basica)

// COMMAND ----------

display(asientos_importes)

// COMMAND ----------

display(maestro_cuentas_contables)
