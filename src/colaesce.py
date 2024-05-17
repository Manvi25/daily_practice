from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col
from pyspark.sql.types import StructType ,StructField,StringType,IntegerType

spark=SparkSession.builder.appName("practice").getOrCreate

schema=StructType([
    StructField("Emp_id",StringType(),True),
    StructField("Name",StringType(),True),
    StructField("age",IntegerType(),True)
])

read_df=spark.read.schema(schema).option("header",True).csv("../../resource/emp_data.csv")

coalesce_df=read_df.select('*',coalesce(col('name'),col('age')))
coalesce_df.show()