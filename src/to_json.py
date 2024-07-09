from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("toJSONExample").getOrCreate()

data = [
    {"name": "Manvi", "age": 22, "city": "Jaipur"},
    {"name": "Sakshi", "age": 25, "city": "Mumbai"},
    {"name": "Ishita", "age": 24, "city": "Pune"}
]

df = spark.createDataFrame(data)

json_rdd = df.toJSON()

for json_str in json_rdd.collect():
    print(json_str)

json_rdd_with_sep = df.toJSON(lineSep="|")

for json_str in json_rdd_with_sep.collect():
    print(json_str)
