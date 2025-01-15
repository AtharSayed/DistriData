# Aim: To find the words frequency 

from pyspark.sql import SparkSession 

spark = SparkSession.builder.master("local").appName("word_count").getOrCreate()
rdd1 = spark.sparkContext.textFile("C:/Users/sayed/Desktop/DDP/practicals/tempdata.txt")
rdd1.collect()
rdd2= rdd1.flatMap(lambda x : x.split(" "))
rdd3 = rdd2.map(lambda word :(word,1))
rdd4 = rdd3.reduceByKey(lambda x,y : x+y)
result = rdd4.take(10)  # Get only the first 10 word counts
print(result)