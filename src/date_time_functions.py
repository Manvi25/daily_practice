from pyspark.sql import SparkSession
from pyspark.sql.functions import date_add, date_sub, datediff, add_months, months_between

spark = SparkSession.builder.appName("DateAndTimeFunction").getOrCreate()

df = spark.createDataFrame([("2023-06-27", "2024-06-27")], ["start_date", "end_date"])

df = df.select(
    date_add("start_date", 10).alias("date_add"),
    date_sub("start_date", 10).alias("date_sub"),
    datediff("end_date", "start_date").alias("datediff"),
    add_months("start_date", 2).alias("add_months"),
    months_between("end_date", "start_date").alias("months_between")
)

df.show()

