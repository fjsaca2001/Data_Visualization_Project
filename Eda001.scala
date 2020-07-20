// Databricks notebook source
// Lectura de datos

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val myDataSchema = StructType(
  Array(StructField("id_persona", DecimalType(26, 0), true), 
        StructField("anio", IntegerType, true), 
        StructField("mes", IntegerType, true), 
        StructField("provincia", IntegerType, true), 
        StructField("canton", IntegerType, true), 
        StructField("area", StringType, true), 
        StructField("genero", StringType, true), 
        StructField("edad", IntegerType, true), 
        StructField("estado_civil", StringType, true), 
        StructField("nivel_de_instruccion", StringType, true), 
        StructField("etnia", StringType, true), 
        StructField("ingreso_laboral", IntegerType, true), 
        StructField("condicion_actividad", StringType, true), 
        StructField("sectorizacion", StringType, true), 
        StructField("grupo_ocupacion", StringType, true), 
        StructField("rama_actividad", StringType, true), 
        StructField("factor_expansion", DoubleType, true)));

val data = spark.read.schema(myDataSchema).option("header","true").option("delimiter","\t").csv("/FileStore/tables/Datos_ENEMDU_PEA_v2.csv");

// COMMAND ----------

// Cual es la frecuencia de hombres y mujeres existentes en las distintas categorias de nivel de estudio
display(data.groupBy("rama_actividad").pivot("genero").count.orderBy("rama_actividad"))

// COMMAND ----------

// Cual es la actividad a la que se dedican las personas que son de etnia "Indigena"
val etniaAct = data.where($"etnia" === "1 - Indígena").groupBy("rama_actividad").count.orderBy($"count".desc)
display(etniaAct.select($"rama_actividad" as ("Actividad"), $"count" as ("Total")));

// COMMAND ----------

// ¿Cual es la media de edad en cada area?
val meanEdad = data.groupBy("rama_actividad").agg(round(mean("edad")).cast(IntegerType)).orderBy("CAST(round(avg(edad), 0) AS INT)")
display(meanEdad.select($"rama_actividad" as ("Actividad"), $"CAST(round(avg(edad), 0) AS INT)" as ("Edad Media")));

// COMMAND ----------

// ¿Cual es el cambio de sueldo segun los años en la rama A. Agricultura, ganadería caza y silvicultura y pesca?
display(data.where($"rama_actividad" === "01 - A. Agricultura, ganadería caza y silvicultura y pesca").groupBy("anio").pivot("genero").agg(round(avg("ingreso_laboral")).cast(IntegerType)).orderBy("anio"));
