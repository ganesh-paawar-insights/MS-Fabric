# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists demo

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show catalogs

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists demo.testschema

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show schemas in demo

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table demo.testschema.testtable6 (id int)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into demo.testschema.testtable6 values(1)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from demo.testschema.testtable6

# COMMAND ----------

# MAGIC %sql
# MAGIC create table demo.testschema.testtable (id int) location 'abfss://catalog@unitycatalogstorage765.dfs.core.windows.net/sandard/testtable'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted demo.testschema.testtable6

# COMMAND ----------


def get_table_info(tablename):
  table_formated_info = spark.sql(f"describe formatted {tablename}").collect()
  for i in table_formated_info:
    # print(i)
    if i.col_name == "Type":
      if "EXTERNAL" in i.data_type:
        table_type = "EXTERNAL"
      elif "MANAGED" in i.data_type:
        table_type = "MANAGED"
  return table_type



get_table_info("demo.testschema.testtable ")



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in demo.testschema

# COMMAND ----------



# COMMAND ----------

def fn_getCount(db_name):
    list_tables = spark.sql(f"show tables in {db_name}").collect()
    managed_tables = []
    external_tables = []
    # print(external_tables)

    for table in list_tables:
        table_name = table["tableName"]
        db_name    = table["database"]
        catalog_name = "demo"
        actula_table_name = f"{catalog_name}.{db_name}.{table_name}"
        table_type = get_table_info(actula_table_name)

        if table_type == "EXTERNAL":
            # print("go")
            external_tables.append(table_name)
            # print("Total External table {}" .format(len(external_tables)))
        elif table_type == "MANAGED":            
            managed_tables.append(table_name)
            # print("Total Managed table {}" .format(len(managed_tables)))

    return managed_tables, external_tables




# COMMAND ----------

m_table, e_table = fn_getCount("demo.testschema")
print("Total Managed table {}" .format(m_table))
print("Total External table {}" .format(e_table))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show create table demo.testschema.testtable

# COMMAND ----------

spark.conf.set("fs.azure.account.key.unitycatalogstorage765.dfs.core.windows.net","oguEDEJT9x395gee9XqZXCT/EVpY/sSbwSMS4jK9Voa0GK14MwzM63o+Qaa37TCM6QjEJcAf95ck+AStCVn6QQ==")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table delta.`abfss://catalog@unitycatalogstorage765.dfs.core.windows.net/sandard/testtable6` clone delta.`abfss://catalog@unitycatalogstorage765.dfs.core.windows.net/581e9cbb-540e-4bae-99ac-0969b3deddb1/tables/e80862b4-428e-46c4-aa10-c673b53a7f44`

# COMMAND ----------

import re

result = spark.sql(f"show create table demo.testschema.testtable6")
# display(result)
create_statement = result.collect()[0][0]
create_statement=create_statement.replace("CREATE TABLE","CREATE OR REPLACE TABLE")
# create_statement=re.sub(r"LOCATION","" ,create_statement)
create_statement = re.sub( r"TBLPROPERTIES\s*\((.|\s)*?\)", "",create_statement)
create_statement += "Location 'abfss://catalog@unitycatalogstorage765.dfs.core.windows.net/sandard/testtable6'"


print(create_statement)
spark.sql("drop table demo.testschema.testtable6")
spark.sql(create_statement)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe formatted demo.testschema.testtable6
