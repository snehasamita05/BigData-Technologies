from pyspark.sql import SparkSession
import getpass

# Initialize Spark session
spark = SparkSession. \
    builder. \
    appName("Sneha Spark Session").\
    config("spark.sql.warehouse.dir", "/user/itv017244/warehouse"). \
    config("spark.dynamicAllocation.enabled", "false"). \
    enableHiveSupport(). \
    master('yarn'). \
    getOrCreate()

spark.conf.get("spark.dynamicAllocation.enabled")

# Define schema for the orders
order_schema = 'order_id long, order_date string , customer_id long, order_status string'

# Load the CSV file into a DataFrame
order_df = spark.read.format("csv").schema(order_schema).load("/user/itv017244/week11Assignment/orders.csv")

# Register the DataFrame as a temporary view
order_df.createOrReplaceTempView("order")

# Run SQL query
spark.sql("""
SELECT order_status, 
       date_format(to_date(substring(order_date, 1, 10), 'yyyy-MM-dd'), 'MM') AS MonthNum, 
       count(order_id) AS NoOfOrder
FROM order 
GROUP BY order_status, date_format(to_date(substring(order_date, 1, 10), 'yyyy-MM-dd'), 'MM')
""").show()

# Explain the query plan
spark.sql("""
SELECT order_status, 
       date_format(to_date(substring(order_date, 1, 10), 'yyyy-MM-dd'), 'MM') AS MonthNum, 
       count(order_id) AS NoOfOrder
FROM order 
GROUP BY order_status, date_format(to_date(substring(order_date, 1, 10), 'yyyy-MM-dd'), 'MM')
""").explain(True)


# Define schema for the orders
order_schema1 = 'order_id long, order_date date , customer_id long, order_status string'

# Load the CSV file into a DataFrame
order_df1 = spark.read.format("csv").schema(order_schema1).load("/user/itv017244/week11Assignment/orders.csv")

# Register the DataFrame as a temporary view
order_df1.createOrReplaceTempView("order1")

# Run SQL query
spark.sql("""
SELECT order_status, 
       date_format(order_date,'MM') AS MonthNum, 
       count(order_id) AS NoOfOrder
FROM order1 
GROUP BY order_status, date_format(order_date, 'MM')
""").show()

# Explain the query plan
spark.sql("""
SELECT order_status, 
       date_format(order_date,'MM') AS MonthNum, 
       count(order_id) AS NoOfOrder
FROM order1 
GROUP BY order_status, date_format(order_date, 'MM')
""").explain(True)

# Stop the Spark session
spark.stop()
