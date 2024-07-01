# Databricks notebook source

from pyspark.sql.functions import sum
from pyspark.sql import DataFrame

data = [
    ("Alice", "2021", "Q1", 5000),
    ("Alice", "2021", "Q2", 6000),
    ("Alice", "2021", "Q3", 7000),
    ("Bob", "2021", "Q1", 4000),
    ("Bob", "2021", "Q2", 3000),
    ("Bob", "2021", "Q3", 2000)
]

columns = ["name", "year", "quarter", "revenue"]

df = spark.createDataFrame(data, columns)
df.show()

def perform_pivot(df, pivot_col, values_col, agg_func):
    pivoted_df = df.groupBy("name", "year").pivot(pivot_col).agg(agg_func(values_col))
    return pivoted_df

pivoted_df = perform_pivot(df, "quarter", "revenue", sum)
pivoted_df.show()


# COMMAND ----------

def read_file(file_path, file_type):
    if file_type=="csv":
        df= spark.read.csv(file_path, header="true", inferschema="true")
    elif file_type=="json":
        df= spark.read.json(file_path, multiline="True")
    else:
        raiseValueError("unsupported file type: csv or json")
    return df

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import *

def join_dataframe(df1, df2, join_type, join_keys):
    joined_df= df1.join(df2, join_key,join_type)
    return joined_df

join_udf= udf(join_dataframe)

# COMMAND ----------

def write_dataframe_to_table(df, table_name, mode, format, options={}):
    return df.write.mode(mode).format(format).options(**options).saveastable(table_name)

# COMMAND ----------

from pyspark.sql.functions import *
def rename_column(df, new_column):
    df= withColumnRenamed(old_name,new_name)
    return df

# COMMAND ----------

def rename_columns(df):
    df.columns=[col.lower().replace(' ','_') for col in df.columns]
    return df.columns

# COMMAND ----------

def concatenate(first_name, last_name):
   return f"{first_name} {last_name}"

# COMMAND ----------

def total_compensation(base_salary,bonus):
    return base_salary+bonus

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from datetime import datetime

def format_date(timestamp):
    date_obj = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    return date_obj.strftime("%d-%m-%Y")

format_date_udf = udf(format_date, StringType())

df = spark.createDataFrame([("2023-06-30 11:00:00",), ("2024-07-01 05:00:00",)], ["timestamp"])
df = df.withColumn("formatted_date", format_date_udf("timestamp"))
df.show()


# COMMAND ----------

def apply_explode_operations(df, column_name, operation='explode'):
    if operation == 'explode':
        return df.withColumn(column_name, explode(df[column_name]))
    elif operation == 'explode_outer':
        return df.withColumn(column_name, explode_outer(df[column_name]))
    else:
        raise ValueError("Operation must be 'explode' or 'explode_outer'")
