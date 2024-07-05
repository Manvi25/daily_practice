from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, expr


spark = SparkSession.builder.appName("data skewness salting").getOrCreate()

data1 = [(i, f"val_{i % 3}") for i in range(1000)]
data2 = [(f"val_{i}", i) for i in range(3)]

df1 = spark.createDataFrame(data1, ["key", "value"])
df2 = spark.createDataFrame(data2, ["key", "value2"])

salted_df1 = df1.withColumn("salt", expr("floor(rand() * 3)")) \
                .withColumn("salted_key", concat(col("key"), lit("_"), col("salt")))

salted_df2 = df2.crossJoin(spark.range(3).toDF("salt")) \
                .withColumn("salted_key", concat(col("key"), lit("_"), col("salt")))

result_df = salted_df1.join(salted_df2, "salted_key")

result_df.show()
