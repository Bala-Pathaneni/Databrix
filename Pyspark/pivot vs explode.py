# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC **Pivot**
# MAGIC
# MAGIC *The pivot() function is used to rotate rows into columns, creating a pivot table-like structure. It allows you to reshape the DataFrame by specifying a column to use as the pivot and aggregating values based on other columns*

# COMMAND ----------

# Import libraries
from pyspark.sql.functions import explode,asc,desc

# COMMAND ----------

# Create a DataFrame
df = spark.createDataFrame([(1, "A", 10), (1, "B", 20), (2, "A", 30), (2, "B", 40), (3,"C",56), (1,"A",3)], ["id", "category", "value"])

# Pivot the DataFrame
pivoted_df = df.groupBy("id").pivot("category").sum("value").sort(asc("id"))

df.show()
pivoted_df.show() #In this example, the DataFrame is pivoted based on the "category" column, and the values in the "value" column are aggregated (summed) for each pivot.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Explode**
# MAGIC
# MAGIC *The explode() function is used to split a column containing arrays or maps into multiple rows. It creates a new row for each element in the array or key-value pair in the map*

# COMMAND ----------

# Create a DataFrame
df = spark.createDataFrame([(1, ["apple", "banana"]), (2, ["orange", "grape"])], ["id", "fruits"])

# Explode the array column
exploded_df = df.select("id", explode("fruits").alias("fruit"))

df.show()
exploded_df.show() #In this example, the "fruits" column is exploded into separate rows, with each element in 
                   #the array becoming a separate row, while the "id" column is duplicated for each 
                   #exploded row

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame([
 (1, "John", 18, ["math", "science", "english"]),
 (2, "Mary", 19, ["history", "art", "music"])], ["id", "name","Enrollment","subjects"])

df_explode = df.withColumn("subjects", F.explode(df.subjects))

df_explode.show()

df_pivot = df_explode.groupBy("name").pivot("subjects").agg(F.count("subjects"))

df_pivot.show()

# COMMAND ----------

# Explode will do 1 NF and Pivot will denormalize the data in a generic way.
