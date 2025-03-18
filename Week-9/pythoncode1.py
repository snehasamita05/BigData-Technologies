from pyspark.sql import SparkSession

import getpass

spark = SparkSession. \
	builder. \
	config("spark.shuffle.useOldFetchProtocol", 'true'). \
	config("spark.sql.warehouse.dir", "/user/itv017244/warehouse"). \
	enableHiveSupport(). \
	master('yarn'). \
	getOrCreate()

order_schema = 'order_id long, order_date string, customer_id long, order_status string'

order_df = spark.read.\
format("csv").\
schema(order_schema).\
load("/public/trendytech/orders/orders_1gb.csv")

print(order_df.rdd.getNumPartitions())
order_df.createOrReplaceTempView("orders")

spark.sql("select order_status,count(*) from orders group by order_status").show()
spark.stop()