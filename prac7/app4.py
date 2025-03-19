from pyspark.sql.functions import *
from pyspark.sql import SparkSession
if __name__ == '__main__':
    print("Creating Spark Session ")

spark = SparkSession.builder \
.config('spark.sql.shuffle.partitions',3) \
.appName("streaming application") \
.master("local[2]") \
.getOrCreate()

orders_schema = "order_id long, order_date string, order_customer_id long, order_status string, amount long"


#1 Read the Data
orders_df = spark\
.readStream \
.format("socket") \
.option("host", "localhost") \
.option("port", 9995) \
.load()

value_df = orders_df.select(from_json(col("value"),orders_schema).alias ("value"))

#value_df.printSchema()


refined_orders_df = value_df.select("value.*")


windows_agg_df = refined_orders_df \
    .groupBy(window(col("order_date"),"15 minutes")) \
    .agg(sum("amount").alias("total_invoice")) \
                
windows_agg_df.printSchema()


output_df = windows_agg_df.select("window.start","window.end","total_invoice")

query = output_df \
.writeStream \
.outputMode("update") \
.format("console") \
.option("checkpointLocation","checkpointlocation1") \
.trigger(processingTime = "15 second") \
.start()

query.awaitTermination()