from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

spark = SparkSession.builder.appName("from_json_parameters").getOrCreate()

data = [
    ('{"name": "manvi", "age": 30, "contacts": [{"type": "email", "value": "manvi@example.com"}, {"type": "phone", "value": "123-456-7890"}]}',),
    ('{"name": "ishita", "age": 35, "contacts": [{"type": "email", "value": "ishita@example.com"}, {"type": "phone", "value": "098-765-4321"}]}',)
]

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("contacts", ArrayType(StructType([
        StructField("type", StringType(), True),
        StructField("value", StringType(), True)
    ])), True)
])

df = spark.createDataFrame(data, ["json"])

parsed_df = df.withColumn("parsed_json", from_json(col("json"), schema))

result_df = parsed_df.select(
    col("parsed_json.name").alias("name"),
    col("parsed_json.age").alias("age"),
    col("parsed_json.contacts").alias("contacts")
)

result_df.show(truncate=False)

