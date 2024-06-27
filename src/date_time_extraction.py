from pyspark.sql.functions import year, month, dayofmonth, hour, minute, second

df = spark.createDataFrame([("2023-06-27 12:34:56",)], ["datetime"])

df = df.select(
    year("datetime").alias("year"),
    month("datetime").alias("month"),
    dayofmonth("datetime").alias("day"),
    hour("datetime").alias("hour"),
    minute("datetime").alias("minute"),
    second("datetime").alias("second")
)
df.show()
