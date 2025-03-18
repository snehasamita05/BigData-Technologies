from pyspark.sql import SparkSession

spark = SparkSession. \
	builder. \
	appName("Sneha Spark Session").\
	config("spark.shuffle.useOldFetchProtocol", 'true'). \
	config("spark.sql.warehouse.dir", "/user/itv017244/warehouse"). \
	enableHiveSupport(). \
	master('yarn'). \
	getOrCreate()


order_avro_df = spark.read.format("avro").load("/public/trendytech/datsets/orders_avro")
num_partitions = order_avro_df.getNumPartitions()
print(num_partitions)
order_avro_df.createOrReplaceTempView("order")
spark.sql("select * from order").show()
spark.stop()