from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("ComplexBroadcastExample").getOrCreate()

sales_data = [
    (1, 101, 5, '2024-06-01'),
    (2, 102, 3, '2024-06-02'),
    (3, 103, 8, '2024-06-03'),
    (4, 101, 6, '2024-06-04'),
    (5, 104, 1, '2024-06-05')
]

product_data = [
    (101, 'ProductA', 100),
    (102, 'ProductB', 200),
    (103, 'ProductC', 300),
    (104, 'ProductD', 400)
]

region_data = [
    (1, 'North America'),
    (2, 'Europe'),
    (3, 'Asia'),
    (4, 'North America'),
    (5, 'Europe')
]

sales_df = spark.createDataFrame(sales_data, ["transaction_id", "product_id", "quantity", "date"])
product_df = spark.createDataFrame(product_data, ["product_id", "product_name", "price"])
region_df = spark.createDataFrame(region_data, ["transaction_id", "region"])

broadcast_region = spark.sparkContext.broadcast(region_df.collect())

region_dict = {row['transaction_id']: row['region'] for row in broadcast_region.value}

def add_region(transaction_id):
    return region_dict.get(transaction_id, "Unknown")


add_region_udf = udf(add_region, StringType())

enriched_sales_df = sales_df.withColumn("region", add_region_udf(sales_df["transaction_id"]))

joined_df = enriched_sales_df.join(broadcast(product_df), "product_id")

analytics_df = joined_df.groupBy("region").sum("quantity").withColumnRenamed("sum(quantity)", "total_quantity")

analytics_df.show()


