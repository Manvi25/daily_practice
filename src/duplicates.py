from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark = SparkSession.builder.appName("RemoveDuplicates").getOrCreate()

data = [("Manvi", 23), ("Ishita", 24), ("Sakshi", 21), ("Rishika", 29), ("Manvi", 25), ("Ishita", 30)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

print("Original DataFrame:")
df.show()


df_distinct = df.distinct()
print("Distinct:")
df_distinct.show()

# Drop duplicates based on all columns
df_no_duplicates = df.dropDuplicates()
print("Drop Duplicates (all columns):")
df_no_duplicates.show()


df_no_duplicates_subset = df.dropDuplicates(["Name"])
print("Drop Duplicates (specific column):")
df_no_duplicates_subset.show()

window_spec = Window.partitionBy("Name").orderBy("Age")
df_with_row_number = df.withColumn("row_number", row_number().over(window_spec))
df_deduplicated = df_with_row_number.filter(df_with_row_number.row_number == 1).drop("row_number")
print("Using Window Functions:")
df_deduplicated.show()


