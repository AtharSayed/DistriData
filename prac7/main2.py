from pyspark.sql import SparkSession

spark = SparkSession. \
    builder. \
    config("spark.sql.catalogImplementation", "hive"). \
    enableHiveSupport(). \
    master('local[*]'). \
    getOrCreate()

orders_schema = "order_id long, order_date string, customer_id long, order_status string"

orders_df = spark \
.readStream \
.format("json") \
.schema(orders_schema) \
.option("path", "data/inputdir") \
.load()

#Processing Logic
orders_df.createOrReplaceTempView("orders")
completed_orders = spark.sql("SELECT * FROM orders WHERE order_status = 'COMPLETE'")

#Writing to the Sink
query = completed_orders \
.writeStream \
.format("csv") \
.outputMode("append") \
.option("path", "data/outputdir") \
.option("checkpointLocation", "checkpointdir1") \
.start()
query.awaitTermination()



