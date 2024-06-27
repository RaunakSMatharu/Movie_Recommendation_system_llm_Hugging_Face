# Databricks notebook source
df_input=spark.read.format('csv').option('header',True).option('inferschema',True).load('/FileStore/tables/llm/imdb_top_1000.csv')
display(df_input)

# COMMAND ----------

df_input.write.mode('overwrite').saveAsTable('llm_bronze.imbd_bronze')