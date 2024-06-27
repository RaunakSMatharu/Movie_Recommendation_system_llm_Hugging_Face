# Databricks notebook source
"""
1. choose specific important columns by which user will search
2. clean the data.. all words should be in small letter
3. combine the data to make meaningful sentence
4. donot use comma, dot,etc should not be any null
"""

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

df=spark.read.table('llm_bronze.imbd_bronze')
df=df.select('Series_Title','Genre','Director','Overview')
df_silver=df.select([regexp_replace(lower(col(i)),r'[.,-:\']','').alias(i) for i in df.columns])
display(df_silver)

# COMMAND ----------

windowspec=Window.partitionBy(lit(1)).orderBy(lit(1))
df_silver = df_silver.withColumn(
    "content",
    concat(
        nvl(col("Series_Title"), lit(" ")),
        lit(" is "),
        nvl(col("Genre"), lit(" ")),
        lit(" directed by "),
        nvl(col("Director"), lit(" ")),
        lit(" which is "),
        nvl(col("Overview"), lit(" ")),
    ),
).withColumn('id',concat(lit('id-'),row_number().over(windowspec)))
display(df_silver)

# COMMAND ----------

df_silver.write.mode('overwrite').option('overwriteSchema',True).saveAsTable('llm_silver.imdb_silver')