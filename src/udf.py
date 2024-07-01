from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("UDF Example").getOrCreate()
data = [
    ("Alice", 29),
    ("Bob", 35),
    ("Charlie", 40)
]

columns = ["name", "age"]

df = spark.createDataFrame(data, columns)
df.show()

def convert_to_uppercase(name):
    return name.upper()

convert_to_uppercase_udf = udf(convert_to_uppercase, StringType())

df = df.withColumn("name_uppercase", convert_to_uppercase_udf(df.name))
df.show()
