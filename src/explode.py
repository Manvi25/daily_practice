from pyspark.sql import SparkSession
from pyspark.sql.functions import posexplode,explode,explode_outer
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType,BooleanType,LongType

spark=SparkSession.builder.appName("Manvi").getOrCreate()


schema=StructType([
    StructField("name",StringType(),True),
    StructField("age",IntegerType(),True),
    StructField("phone_numbers",ArrayType(StringType()),True),
    StructField("is_student",BooleanType(),True),
    StructField("timestamp",LongType(),True)
])

read_df=spark.read.json("../../resource/new_data.json",multiLine=True,schema=schema)
read_df.show()
read_df.printSchema()

print("using posexplode")
# using posexplode
read_df.select(read_df.age,read_df.is_student,read_df.name,posexplode(read_df.phone_numbers),read_df.timestamp).show()

print("using Explode")
# using explode
read_df.select(read_df.age,read_df.is_student,read_df.name,explode(read_df.phone_numbers),read_df.timestamp).show()

print("using explode_outer")
# Using explode_outer
read_df.select(read_df.age,read_df.is_student,read_df.name,explode_outer(read_df.phone_numbers),read_df.timestamp).show()
