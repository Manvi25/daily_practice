from pyspark.sql import SparkSession
from pyspark.sql.functions import array_join, col, collect_list


spark = SparkSession.builder.appName("array to string").getOrCreate()

data1 = [
    ("Manvi", "Math", 85),
    ("Manvi", "English", 78),
    ("Ishita", "Math", 90),
    ("Ishita", "English", 82),
    ("Sakshi", "Math", 87),
    ("Sakshi", "English", 91)
]

data2 = [
    ("Math", "Mathematics"),
    ("English", "English Language Arts"),
    ("Science", "Physical Science")
]

df1 = spark.createDataFrame(data1, ["name", "subject", "score"])
df2 = spark.createDataFrame(data2, ["subject", "subject_full_name"])

joined_df = df1.join(df2, "subject")

grouped_df = joined_df.groupBy("name").agg(
    collect_list("subject").alias("subjects"),
    collect_list("score").alias("scores")
)


result_df = grouped_df.withColumn(
    "subjects_str",
    array_join(col("subjects"), ", ")
).withColumn(
    "scores_str",
    array_join(col("scores").cast("array<string>"), ", ")
)

result_df.select("name", "subjects_str", "scores_str").show()


