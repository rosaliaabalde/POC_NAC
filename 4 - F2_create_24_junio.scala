// Databricks notebook source
// MAGIC %sql
// MAGIC set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;

// COMMAND ----------

import org.apache.spark.sql.types.{StringType, IntegerType, DecimalType, StructField, StructType, DateType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.RowEncoder

// COMMAND ----------

// MAGIC %md
// MAGIC ### Tabla maestro eventos de negocio

// COMMAND ----------

// de la tabla maestro_eventos_negocio se quiere solo EVENT_NEG
val maestro_eventos_negocio = spark.table("maestro_eventos_negocio").select("EVENT_NEG")

// COMMAND ----------

display(maestro_eventos_negocio)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Tabla Asientos Cuentas Contables

// COMMAND ----------

val defautlColumns = List("COD_SOCIEDAD", "ID_CUEN_ASIEN", "ID_ESTRUCTURA", "COD_ASIENTO", "NATURALEZA", "NOM_CAMPO", "VALOR", "COD_CUENTA")
val asientos_cuentas_contables_df = spark.table("asientos_cuentas_contables").select(defautlColumns.map(col):_*)
//val dinamicColumnsCuentasContables = asientos_cuentas_contables_df.select("NOM_CAMPO").distinct().collect().map(_.getString(0))
val dinamicColumnsCuentasContables = asientos_cuentas_contables_df.select("NOM_CAMPO").distinct().as[String].collect()

// COMMAND ----------

display(asientos_cuentas_contables_df)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Tabla maestro cuentas contables

// COMMAND ----------

//se queda con las distintas y en camp_aux y las que no sean null
val maestro_cuentas_contables_df = spark.table("maestro_cuentas_contables")
val dinamicMaestroCuentasContables = maestro_cuentas_contables_df
.select("CAMPO_AUX")
.filter(col("CAMPO_AUX").isNotNull)
.distinct().as[String]
.collect()

// COMMAND ----------

display(maestro_cuentas_contables_df)

// COMMAND ----------

println(dinamicMaestroCuentasContables)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Tabla relación de eventos (negocio - contable)

// COMMAND ----------

val defaultSelect = List("EVENT_NEG", "COD_ASIENTO")
val relacion_eventos = spark.table("relacion_eventos").select(defaultSelect.map(col):_*)

// COMMAND ----------

display(relacion_eventos)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Tablas Asiestos Estructura básica - Importes

// COMMAND ----------

//lists are like arrays
val defaultSelectAEB = List("COD_SOCIEDAD", "ID_ESTRUCTURA", "COD_ASIENTO", "NOM_IMPORTE", "NATURALEZA")
val defaultSelectAI = List("COD_SOCIEDAD", "ID_ESTRUCTURA", "COD_ASIENTO", "NOM_IMPORTE", "NOM_CAMPO")
val asientos_estructura_basica = spark.table("asientos_estructura_basica").select(defaultSelectAEB.map(col):_*)
val asientos_importes = spark.table("asientos_importes").select(defaultSelectAI.map(col):_*)
val dinamicColumnsAsientosImportes = asientos_importes.select("NOM_CAMPO").distinct().as[String].collect()

// COMMAND ----------

println(dinamicColumnsAsientosImportes)

// COMMAND ----------

//join para asientos_estructura_basica con asientos_importes
val asientos_estructura_basica_importes = asientos_estructura_basica.join(asientos_importes, Seq("COD_SOCIEDAD","ID_ESTRUCTURA","COD_ASIENTO","NOM_IMPORTE"))

// COMMAND ----------

display(asientos_estructura_basica_importes)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Prueba pivotado para join con Asientos cuentas contables.

// COMMAND ----------

//asientos_cuentas_contables_df.filter(distinct(col("NOM_CAMPO")).show()

// COMMAND ----------

///Pivot. En esta parte convertimos el campo "NOM_CAMPO" en dos columnas y le asignamos los valores de la columna "Valor"
val dfACCPivot = asientos_cuentas_contables_df
.filter(col("COD_CUENTA").isNotNull)
.groupBy("COD_SOCIEDAD", "ID_CUEN_ASIEN", "ID_ESTRUCTURA", "COD_ASIENTO", "NATURALEZA", "COD_CUENTA")
.pivot("NOM_CAMPO")
.agg(first(col("VALOR")))

// COMMAND ----------

dfACCPivot.show()

// COMMAND ----------

val cruceContable = dfACCPivot.join(maestro_cuentas_contables_df, dfACCPivot("COD_CUENTA") === maestro_cuentas_contables_df("NUM_CUENTA"))
.select(dfACCPivot("*"), maestro_cuentas_contables_df("DESC_CUENTA"), maestro_cuentas_contables_df("IND_TIPO"), maestro_cuentas_contables_df("CAMPO_AUX"))

// COMMAND ----------

cruceContable.show()

// COMMAND ----------

val defautlColumns_cc = List("COD_SOCIEDAD", "ID_ESTRUCTURA", "COD_ASIENTO", "NATURALEZA", "NOM_CAMPO", "VALOR", "COD_CUENTA", "DESC_CUENTA", "IND_TIPO", "CAMPO_AUX")

// COMMAND ----------

val columns = dinamicColumnsCuentasContables.toList
//val communColumns = asientos_cuentas_contables_df.columns.filter(!columns.contains(_)).filter(!List("NOM_CAMPO", "VALOR").contains(_))
val communColumns = defautlColumns_cc
.filter(!columns.contains(_))
.filter(!List("NOM_CAMPO", "VALOR").contains(_))

/*val dfSeparados = scala.collection.mutable.ListBuffer.empty[org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]]
for (column <- columns){
  val selectColumns = (communColumns ++ Array(column)).toList
  dfSeparados += dfACCPivot.filter(col(column).isNotNull).select(selectColumns.map(col):_*)
}*/

/*val dfSeparados : Array[DataFrame] = columns.map(column => {
  val selectColumns = (communColumns ++ Array(column)).toList
  dfACCPivot.filter(col(column).isNotNull).select(selectColumns.map(col):_*)
})*/

// COMMAND ----------

println(defautlColumns_cc)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Enriquecimiento F1

// COMMAND ----------

// Carga de F1
val defaultColumns = Seq("TR_ID_EVNEG", "COD_SOCIEDAD", "COD_RAMO", "TR_COD_TIP_RECIB", "TR_COD_EST_RECIB", "TR_NEGOCIO", "TR_COD_TIP_NEGOCIO", "TR_COD_RAMO_CONT", "TR_COD_MOD_CONT")
val selectColumns = (defaultColumns ++ dinamicColumnsCuentasContables ++ dinamicColumnsAsientosImportes ++ dinamicMaestroCuentasContables).distinct
val F1_df = spark.table("default.F1").select(selectColumns.map(col):_*)

// COMMAND ----------

display(F1_df)

// COMMAND ----------

val F1_1_df = F1_df.withColumn("EVENT_NEG", concat_ws("_", col("TR_COD_TIP_RECIB"), col("TR_COD_EST_RECIB")))

// COMMAND ----------

display(F1_1_df)

// COMMAND ----------

val F1_2_df = F1_1_df.join(maestro_eventos_negocio, "EVENT_NEG").select(F1_1_df("*"))

// COMMAND ----------

display(F1_2_df)

// COMMAND ----------

// Qué sucede con aquellos registros que no hagan "match" en esta tabla de relación, ¿se descartan?
val F1_3_df = F1_2_df.join(relacion_eventos, "EVENT_NEG").select(F1_2_df("*"), relacion_eventos("COD_ASIENTO"))

// COMMAND ----------

display(F1_3_df)

// COMMAND ----------

//val F1_3_df_limitado = F1_3_df
//val F1_3_df_limitado = F1_3_df.limit(500)

// COMMAND ----------

val F1_3_df_join_aebi = F1_3_df.join(asientos_estructura_basica_importes, Seq("COD_ASIENTO", "COD_SOCIEDAD")).cache() //TODO: ¿Se hace por Entidad? ¿A qué campo corresponde con F1?

// COMMAND ----------

display(F1_3_df_join_aebi)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Join F1 enriquecido con tabla Asientos Cuentas Contables pivotada

// COMMAND ----------

import org.apache.spark.sql.DataFrame

val schemaF14 = F1_3_df_join_aebi.schema.add("COD_CUENTA", DecimalType(10,0)).add("DESC_CUENTA", StringType).add("IND_TIPO", StringType).add("CAMPO_AUX", StringType)
val defautlJoin = Seq("COD_SOCIEDAD", "ID_ESTRUCTURA", "COD_ASIENTO", "NATURALEZA")
//var F1_4_df = spark.createDataFrame(sc.emptyRDD[Row], schemaF14)
val columnsF14 = schemaF14.fieldNames

/*for (i <- 1 to dfSeparados.size){
  val columnsJoin = defautlJoin :+ columns(i-1)
  //println("--> ", columnsJoin.mkString(";"))
  val dfPartialJoin = F1_3_df_join_aebi.join(dfSeparados(i-1), columnsJoin).select(columnsF14.map(col):_*)
  //println("Count union: ", dfPartialJoin.count())
  F1_4_df = F1_4_df.union(dfPartialJoin)
}*/

val dfSeparados :List[DataFrame] = columns.map(column => {
  val columnsJoin = defautlJoin :+ column
  //println("--> ", columnsJoin.mkString(";"))

  val selectColumns = (communColumns ++ Array(column)).toList
  val df = cruceContable.filter(col(column).isNotNull).select(selectColumns.map(col):_*)

  // quizas aqui podriamos filtrar los datos de F1_3_df_join_aebi darle una pensada
  val dfPartialJoin = F1_3_df_join_aebi.join(df, columnsJoin).select(columnsF14.map(col):_*)

  dfPartialJoin
})

// Esto podría ir al final del map anterior
val dfUnido = dfSeparados.reduce(_ union _)

// COMMAND ----------

display(dfUnido)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Join F1 enriquecido con Maestro cuentas contables

// COMMAND ----------

// Join F1 enriquecido con maestro_cuentas_contables, para añadir nuevos campos
//val F1_5_df = F1_4_df.join(maestro_cuentas_contables_df, F1_4_df("COD_CUENTA") === maestro_cuentas_contables_df("NUM_CUENTA")).select(F1_4_df("*"), maestro_cuentas_contables_df("NUM_CUENTA"), maestro_cuentas_contables_df("DESC_CUENTA"), maestro_cuentas_contables_df("IND_TIPO"), maestro_cuentas_contables_df("CAMPO_AUX"))
// ME LLEVO ESTO A ANTES DE LOS JOINS DE ASIENTO CUENTAS CONTABLES CON F1


// COMMAND ----------

// Calculo de campos para F2_CONT
val F1_6_df = dfUnido
    .withColumn("TR_CUENTA_CONT", col("COD_CUENTA"))
    .withColumn("TR_COD_TIP_EVCON", substring(col("COD_ASIENTO"), 0, 2))
    .withColumn("TR_ID_EVCON", col("COD_ASIENTO"))
    .withColumn("TR_NAT_EVCON", col("NATURALEZA"))

// COMMAND ----------

display(F1_6_df)

// COMMAND ----------

val schemaF2 = StructType(
  Array(
    StructField("TR_ID_EVNEG", StringType, true),
    StructField("TR_COD_RAMO_CONT", StringType, true),
    StructField("TR_COD_MOD_CONT", StringType, true),
    StructField("TR_CUENTA_CONT", DecimalType(10,0), true),
    StructField("TR_COD_TIP_EVCON", StringType, true),
    StructField("TR_ID_EVCON", StringType, true),
    StructField("TR_NAT_EVCON", StringType, true),
    StructField("TR_CC_DES", StringType, true),
    StructField("TR_CC_IMP", DecimalType(13,2), true)
  )
)
val encoder = RowEncoder(schemaF2)

// COMMAND ----------

val F1_6_df_map = F1_6_df.map(opRow => {
  val nombreColumnaImporte = opRow.getAs[String]("NOM_CAMPO")
  val campoAux = opRow.getAs[String]("CAMPO_AUX")
  val descCuenta = opRow.getAs[String]("DESC_CUENTA")
  //val descripcion = if (opRow.getAs[String]("IND_TIPO").toInt == 1) descCuenta + " - " + opRow.getAs[String](campoAux) else descCuenta
  val descripcion = if (opRow.getAs[String]("IND_TIPO").toInt == 1) s"${descCuenta} - ${opRow.getAs[String](campoAux)}" else descCuenta
  val rowGenerated = Row(
    opRow.getAs[String]("TR_ID_EVNEG"),
    opRow.getAs[String]("TR_COD_RAMO_CONT"),
    opRow.getAs[String]("TR_COD_MOD_CONT"),
    opRow.getAs[BigDecimal]("TR_CUENTA_CONT"),
    opRow.getAs[String]("TR_COD_TIP_EVCON"),
    opRow.getAs[String]("TR_ID_EVCON"),
    opRow.getAs[String]("TR_NAT_EVCON"),
    descripcion, //TR_CC_DES
    opRow.getAs[BigDecimal](nombreColumnaImporte) //TR_CC_IMP
  )
  rowGenerated
})(encoder) // Encoder con la estructura que va a tener para mantenerlo como DataFrame

// COMMAND ----------

// MAGIC %md
// MAGIC ### CREACIÓN E INSERCCIÓN DE TABLA F2_CONT

// COMMAND ----------

//Seleccion campos para tabla f2_cont
val columnsF2_cont = spark.catalog.listColumns("default", "f2_cont").select("name").as[String].collect()
val f2_cont_definitivo = F1_6_df_map.select(columnsF2_cont.map(col):_*)

// COMMAND ----------

f2_cont_definitivo.createOrReplaceTempView("f2Temp")
spark.sql(s"INSERT OVERWRITE TABLE f2_cont SELECT ${columnsF2_cont.mkString(",")} FROM f2Temp")

// COMMAND ----------

//f2_cont_definitivo.coalesce(1).write.option("header","true").mode("overwrite").csv("/FileStore/tables/f2_cont.csv")
//spark.table("f2_cont").write.format("csv").mode("overwrite").save("/FileStore/tables/f2_cont.csv")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT count(*) FROM f0

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT count(*) FROM f1

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT count(*) FROM f2_cont

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT count(*) FROM ps

// COMMAND ----------

//display(spark.table("f2_cont"))
//spark.table("f2_cont").limit(2000).show(2000, false)

// RARO No se repite ningún TR_ID_EVNEG
//display(f2_cont_definitivo)


// COMMAND ----------

f2_cont_definitivo.count()

// COMMAND ----------

display(f2_cont_definitivo.where(col("TR_NAT_EVCON") === 1))

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from f2_cont where TR_ID_EVNEG = 25769804659
