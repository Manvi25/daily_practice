from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col
from pyspark.sql.types import StructType ,StructField,StringType,IntegerType

spark=SparkSession.builder.appName("practice").getOrCreate()

schema=StructType([
    StructField("Emp_id",StringType(),True),
    StructField("Name",StringType(),True),
    StructField("age",IntegerType(),True)
])

read_df=spark.read.schema(schema).option("header",True).csv("../../resource/emp_data.csv")

read_df.show()

read_df.distinct().show()

read_df.dropDuplicates().show()

read_df.dropDuplicates(['Emp_id']).show()