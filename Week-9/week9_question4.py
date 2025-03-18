from pyspark.sql import SparkSession
import getpass
spark = SparkSession. \
	builder. \
	appName("Sneha Spark Session").\
	config("spark.sql.warehouse.dir", "/user/itv017244/warehouse"). \
	enableHiveSupport(). \
	master('yarn'). \
	getOrCreate()


from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
user_schema = StructType([
                        StructField("user_id", IntegerType(),nullable = False),
                        StructField("user_first_name", StringType(),nullable = False),
                        StructField("user_last_name", StringType(),nullable = False),
                        StructField("user_email", StringType(),nullable = False),
                        StructField("user_gender", StringType(),nullable = False),
                        StructField("user_phone_numbers", ArrayType(StringType()),nullable=True),
                        StructField("user_address", StructType([
                        StructField("street", StringType(), nullable=False),
                        StructField("city", StringType(), nullable=False),
                        StructField("state", StringType(), nullable=False),
                        StructField("postal_code", StringType(), nullable=False),
                        ]), nullable=False)
                        ])
sms_df = spark.read.format("json").schema(user_schema).load("/public/sms/users")
sms_df.rdd.getNumPartitions()
sms_df.count()
sms_df.limit(2)
from pyspark.sql.functions import col, size
sms_df.withColumn("user_street",col("user_address.street"))\
      .withColumn("user_city",col("user_address.city"))\
      .withColumn("user_state",col("user_address.state"))\
      .withColumn("user_postal_code",col("user_address.postal_code"))\
      .withColumn("num_phn_numbers",size(col("user_phone_numbers"))).createOrReplaceTempView("user_vw")
spark.sql("select count(distinct(user_Id)) as user_cnt from user_vw where user_state = 'New York'").show()
spark.sql("select count(distinct(user_postal_code)) as cnt,user_state from user_vw group by user_state order by cnt desc limit(1)")
spark.sql("select count(distinct(user_id)) as cnt, user_city from user_vw limit where user_city is not null group by user_city order by cnt desc limit(1)")
spark.sql("""select count(distinct user_id) as user_cnt from user_vw where user_email like '%bizjournals.com'""")
spark.sql("""select count(distinct user_id) as user_cnt from user_vw where num_phn_numbers = 4""")
spark.sql("""select count(distinct user_id) as user_cnt from user_vw where user_phone_numbers is null""")
sms_df.write.format("parquet").mode("overwrite").option("path","/user/itv017244/week9/assignment").save()
spark.stop()