from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

spark = SparkSession.builder.appName("Split and getItem Example") .getOrCreate()

data = [("Alice,29",), ("Bob,35",), ("Charlie,40",)]
columns = ["name_age"]

df = spark.createDataFrame(data, columns)
df.show()

df = df.withColumn("name", split(col("name_age"), ",").getItem(0))
df = df.withColumn("age", split(col("name_age"), ",").getItem(1).cast("int"))

df.show()
