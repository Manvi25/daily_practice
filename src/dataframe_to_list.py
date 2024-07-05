from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder.appName("dataframe to list").getOrCreate()

data = [
    ("Manvi", "Math", 85),
    ("Manvi", "English", 78),
    ("Ishita", "Math", 90),
    ("Ishita", "English", 82),
    ("Ishita", "Math", 87),
    ("Sakshi", "English", 91),
]

schema = ["Name", "Subject", "Score"]

df = spark.createDataFrame(data, schema)

filtered_df = df.filter(col("Score") > 80)

grouped_df = filtered_df.groupBy("Name").agg(avg("Score").alias("Average_Score"))

sorted_df = grouped_df.orderBy(col("Average_Score").desc())

data_list = sorted_df.collect()
data_list = [list(row) for row in data_list]

print(data_list)

