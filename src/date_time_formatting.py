from pyspark.sql.functions import date_format, to_date, to_timestamp

df = spark.createDataFrame([("2023-06-27 12:34:56",)], ["datetime"])


df = df.select(
    date_format("datetime", "yyyy-MM-dd").alias("formatted_date"),
    to_date("datetime", "yyyy-MM-dd HH:mm:ss").alias("to_date"),
    to_timestamp("datetime", "yyyy-MM-dd HH:mm:ss").alias("to_timestamp")
)
df.show()
