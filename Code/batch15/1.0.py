# Databricks notebook source
# MAGIC %md
# MAGIC ##Basics of Pyspark/Scala

# COMMAND ----------



# COMMAND ----------

# MAGIC %sh
# MAGIC git clone https://github.com/pyexcel/pyexcel-xlsx.git
# MAGIC cd pyexcel-xlsx
# MAGIC python setup.py install

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType
import pandas as pd
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %fs
# MAGIC ls '/FileStore/tables/files'

# COMMAND ----------

modifySchema = StructType([
    StructField("cid",IntegerType(),True),
    StructField("ConainerName",StringType(),True),
    StructField("POL",StringType(),True),
    StructField("POD",StringType(),True),

])

# COMMAND ----------

path = "/FileStore/tables/files/Scd2_File2.csv"
df_csv = spark.read.option('delimiter',';').option('header',True).option('inferSchema',True).csv(path,schema=modifySchema)
df_csv.display()

# COMMAND ----------

df_csv.printSchema

# COMMAND ----------

print(modifySchema)

# COMMAND ----------

# DBTITLE 1,Excel file read
# MAGIC %fs
# MAGIC ls "/FileStore/tables/files/Financial_Sample.xlsx"
# MAGIC

# COMMAND ----------

path = "/FileStore/tables/files/Financial_Sample.xlsx"

# COMMAND ----------



# COMMAND ----------

# MAGIC %sh
# MAGIC pip install pyexcel-xlsx

# COMMAND ----------

pip install com.crealytics:spark-excel_2.12:0.13.5

# COMMAND ----------

pd.read_excel("/dbfs/FileStore/tables/files/Financial_Sample.xlsx",
                  engine = "openpyxl",
                  sheet_name = "Sheet1",
                  dtype = str)

# COMMAND ----------


excel_data=spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").load("/FileStore/tables/files/Financial_Sample.xlsx")

# COMMAND ----------

excel_data.display()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls '/FileStore/tables/files'

# COMMAND ----------

path="/FileStore/tables/files/MT_cars.parquet"
df_parquet = spark.read.format("parquet").load(path)
df_parquet.display()

# COMMAND ----------

json_schema = StructType([
    StructField("id",StringType(),True),
    StructField("name",StringType(),True),
    StructField("age",StringType(),True),
    StructField("email",StringType(),True),
    StructField("Cars",StructType([
        StructField("make",StringType(),True),
        StructField("year",StringType(),True),
        StructField("color",StringType(),True)]
    ))

])

# COMMAND ----------

# {
# "id": "01",
# "name": "Tom Hanks",
# "age": 20.0,
# "email": "th@hollywood.com",
# "Cars":
#   {
#   "make": "Bentley",
#   "year": 1973.0,
#   "color": "White"
#   }
# }

# COMMAND ----------

# DBTITLE 1,json file load
path="/FileStore/tables/files/jsonexample.json"
# df_json = spark.read.format("json").load(path)
df_json = spark.read.option('multiline',True).schema(json_schema).json(path)
df_json.display()

result_df = df_json.select(col("Cars.*"))
result_df.display()

# COMMAND ----------


