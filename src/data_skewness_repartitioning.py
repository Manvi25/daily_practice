from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("handling_data_skewness").getOrCreate()

data = [(i, f"value_{i % 10}") for i in range(1000)]

df = spark.createDataFrame(data, ["id", "value"])

repartitioned_df = df.repartition(10, "value")

repartitioned_df.show()
