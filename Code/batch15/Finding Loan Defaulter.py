# Databricks notebook source
# DBTITLE 1,Library
import pyspark.sql.types as typ
import pyspark.sql.functions as fn
import functools as ft
import matplotlib.pyplot as plt
from  pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
import pandas as pd
import seaborn as sns

# COMMAND ----------

labels = [
  ('ID', typ.IntegerType()),
  ('DISBURSED_VALUE', typ.IntegerType()),
  ('ASSET_COST', typ.IntegerType()),
  ('LOAN_TO_VALUE', typ.DoubleType()),
  ('BRANCH_ID', typ.IntegerType()),
  ('SUPPLIER_ID', typ.IntegerType()),
  ('MANUFACTURER_ID', typ.IntegerType()),
  ('PINCODE', typ.IntegerType()),
  ('DOB', typ.StringType()),
  ('EMP_TYPE', typ.StringType()),
  ('DISBURSED_DATE', typ.StringType()),
  ('REGION_ID', typ.IntegerType()),
  ('EMPLOYEE_CODE_ID', typ.IntegerType()),
  ('MOBILENO_AVL_FLAG', typ.IntegerType()),
  ('ID1_FLAG', typ.IntegerType()),
  ('ID2_FLAG', typ.IntegerType()),
  ('ID3_FLAG', typ.IntegerType()),
  ('ID4_FLAG', typ.IntegerType()),
  ('ID5_FLAG', typ.IntegerType()),
  ('BUREAU_SCORE', typ.IntegerType()),
  ('SCORE_CATEGORY', typ.StringType()),
  ('PRI_ACCS', typ.IntegerType()),
  ('ACTIVE_ACCS', typ.IntegerType()),
  ('OVERDUE_ACCS', typ.IntegerType()),
  ('TOTAL_BALANCE_OUTSTANDING', typ.IntegerType()),
  ('TOTAL_SANCTIONED_AMT', typ.IntegerType()),
  ('TOTAL_DISBURSED_AMT', typ.IntegerType()),
  ('SEC_ACCS', typ.IntegerType()),
  ('SEC_ACTIVE_ACCS', typ.IntegerType()),
  ('SEC_OVERDUE_ACCS', typ.IntegerType()),
  ('SEC_TOTAL_BALANCE_OUTSTANDING', typ.IntegerType()),
  ('SEC_TOTAL_SANCTIONED_AMT', typ.IntegerType()),
  ('SEC_TOTAL_DISBURSED_AMT', typ.IntegerType()),
  ('PRI_EMI', typ.IntegerType()),
  ('SEC_EMI', typ.IntegerType()),
  ('LOANS_6_MTHS', typ.IntegerType()),
  ('LOANS_DEFAULT_6_MTHS', typ.IntegerType()),
  ('AVG_LOAN_TENURE', typ.StringType()),
  ('CREDIT_HIST_LEN', typ.StringType()),
  ('INQUIRIES', typ.IntegerType()),
  ('DEFAULT', typ.IntegerType())
]

# COMMAND ----------

m_schema = typ.StructType([
    typ.StructField(i[0],i[1],True)
    for i in labels
])

# COMMAND ----------

print(m_schema)

# COMMAND ----------

filePath = "/FileStore/tables/loan_default.csv"
df_loan = spark.read.option("header",True).csv(filePath,schema = m_schema)

# COMMAND ----------

print("Count of rows : {0}".format(df_loan.count()))
print("Count of Distinct rows: {0}".format(df_loan.distinct().count()))
print("Count of Distinct rows without ID column: {0}".format(df_loan.select([c for c in df_loan.columns if c != "ID"]).distinct().count()))

# COMMAND ----------


#Below code provide count of id with /without disticnt value
df_loan.agg(fn.count('ID').alias("ID Count"),
            fn.countDistinct("ID").alias("ID Distinct Count")).display()
#Below code provides list of dublicate data
df_loan.groupBy(fn.col("ID")).agg(fn.count("ID").alias("cntID")).filter(fn.col("cntID")>1).display()


# COMMAND ----------

#finding null value in columns
df_null = df_loan.select([fn.count(fn.when(fn.col(c).isNull(),c)).alias(c) for c in df_loan.columns])
df_null.select([i for i in df_null.columns if df_null.select(i).first()[i]>0]).display()


# COMMAND ----------

# MAGIC %sql
# MAGIC select 50000-1754

# COMMAND ----------

#fill hard code value insted of Null
df_loan.fillna({"EMP_TYPE":"No Data"}).select("EMP_TYPE").filter("EMP_TYPE<>'No Data'").count()


# COMMAND ----------

#Finding outlier in certain column
outlier_column = ['DISBURSED_VALUE','ASSET_COST','LOAN_TO_VALUE','TOTAL_BALANCE_OUTSTANDING','TOTAL_SANCTIONED_AMT',
'TOTAL_DISBURSED_AMT','PRI_EMI','SEC_EMI']
bounds ={}

# COMMAND ----------

for nc in outlier_column:
    q = df_loan.approxQuantile(
        nc,[0.25,0.75],0.05

    )
    # print(q)
    IQR = q[1]-q[0]
    # print(IQR)

    bounds[nc]= [q[0]-1.5 * IQR,q[1]+1.5 * IQR]

# COMMAND ----------

print(bounds)

# COMMAND ----------

df_final_outlier = df_loan.select(
    *[fn.col(c) for c in outlier_column],
    *[fn.when(
    fn.col(c).between (bounds[c][0],bounds[c][1]),0).otherwise(1).alias(c+"_outlier") for c in outlier_column
    
      ]).where(
          ft.reduce(lambda x,y: x|y,(fn.col(c+"_outlier") == 1 for c in outlier_column))).toPandas()
      

# COMMAND ----------

df_sark_outlier = spark.createDataFrame(df_final_outlier)

# COMMAND ----------

# DBTITLE 1,Visulization
selected_column = 'LOAN_TO_VALUE'
histogram_data = df_sark_outlier.select(fn.col(selected_column)).rdd.flatMap(lambda x: x).histogram(10)
# print(histogram_data)

bin_x ,bin_y = histogram_data
print(bin_x)
print(bin_y)

#display graph
plt.figure(figsize=(8,6))
bars=plt.bar(bin_x[:-1],bin_y,width=0.8,align='edge')

for i in bars:
    cal_h = i.get_height()
    plt.text(
        i.get_x()+i.get_width()/2.0,cal_h, '%d' %  int(cal_h), ha = 'center', va = 'bottom'
    )


plt.show()

# COMMAND ----------

df_sark_outlier.display()

# COMMAND ----------

col_arry = ['DISBURSED_VALUE','ASSET_COST']

multi_col = dict([
    (c,df_sark_outlier.select(c).rdd.flatMap(lambda x:x).collect()) for c in col_arry
     ])
# print(multi_col)
a = multi_col[col_arry[0]]
print(a)
plt.figure(figsize=(8,6))
plt.scatter(multi_col[col_arry[0]],multi_col[col_arry[1]])
plt.show()

# COMMAND ----------

df_a = []

# COMMAND ----------

vector_col = "corr_features"
assembler = VectorAssembler(inputCols =outlier_column,outputCol =vector_col)
df_vector =assembler.transform(df_sark_outlier).select(vector_col)
display(df_vector)
# matrix =Correlation.corr(df_vector,vector_col).collect()[0][0]
# corrmatrix = matrix.toArray().tolist()
# print(corrmatrix)

# pd.DataFrame(corrmatrix,columns =outlier_column,index = outlier_column)


# COMMAND ----------

sns.heatmap(corrmatrix,xticklabels=outlier_column,yticklabels=outlier_column,cmap=sns.cm.rocket_r)

# COMMAND ----------

# Unity catalog
# Hive Metastore
# Azure Keyvalut
# Cluster configuration
#job configuration
# user administaration
# access external storage
# cluseter provide external storate
# repos
