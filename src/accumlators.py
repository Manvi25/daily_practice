from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Accumulator").getOrCreate()
sc = spark.sparkContext

data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)

accum = sc.accumulator(0)

def add_to_accum(x):
    global accum
    accum += x

rdd.foreach(add_to_accum)

print(accum.value)

