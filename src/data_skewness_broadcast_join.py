from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName("data skewness broadcast join").getOrCreate()

data1 = [(i, f"value_{i % 10}") for i in range(1000)]
data2 = [(f"value_{i}", i) for i in range(10)]

df1 = spark.createDataFrame(data1, ["id", "value"])
df2 = spark.createDataFrame(data2, ["value", "value2"])

broadcast_df2 = broadcast(df2)

result_df = df1.join(broadcast_df2, "value")

result_df.show()

