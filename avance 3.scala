// Databricks notebook source
// Importacion de librerias
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
// Creacion del esquema
val mySchemaDDL = "id FLOAT, anio INT, mes INT, provincia INT, canton INT, area STRING, genero STRING, edad INT, estado STRING, instruccion STRING, etnia STRING, ingreso INT, condicion STRING, sectorizacion STRING, ocupacion STRING, rama STRING, expansion DOUBLE"

// COMMAND ----------

// Lectura del archivo
val data = spark
  .read
  .schema(mySchemaDDL)
  .option("header","true")
  .option("delimiter", "\t")
  .csv("/FileStore/tables/Datos_ENEMDU_PEA_v2.csv")

// COMMAND ----------

// La Columna a trabajar es la de Ingreso Laboral
data.select("ingreso").summary()

// COMMAND ----------

// Cual es el ingreso máximo por estado civil 
data.groupBy("anio").pivot("estado").max("ingreso").orderBy($"anio"desc).show(5)

// COMMAND ----------

val estado = data.groupBy("anio").pivot("estado").max("ingreso").orderBy($"anio"desc)

// COMMAND ----------

display(estado)


// COMMAND ----------

// Comparación de ingresos máximos entre hombres y mujeres
data.groupBy("anio").pivot("genero").max("ingreso").orderBy("anio").show

// COMMAND ----------

val genero = data.groupBy("anio").pivot("genero").max("ingreso").orderBy("anio")

// COMMAND ----------

display(genero)

// COMMAND ----------

// Etnias con mayor ingreso laboral en orden ascendente
data.select($"etnia", $"ingreso").groupBy("etnia").avg("ingreso").sort(desc("avg(ingreso)")).show

// COMMAND ----------
val etnias = data.select($"etnia", $"ingreso").groupBy("etnia").avg("ingreso").sort(desc("avg(ingreso)"))

// COMMAND ----------

display(etnias)

// COMMAND ----------

// Ingresos máximos para hombres y mujeres en todas las ramas
data.groupBy("rama").pivot("genero").max("ingreso").orderBy("rama").show

// COMMAND ----------

val ramas = data.groupBy("rama").pivot("genero").max("ingreso").orderBy("rama")

// COMMAND ----------

display(ramas)

// COMMAND ----------


