from pyspark.sql import SparkSession

import getpass

spark = SparkSession. \
	builder. \
	config("spark.shuffle.useOldFetchProtocol", 'true'). \
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

from pyspark.sql.functions import col, size
sms_df.withColumn("user_street",col("user_address.street"))\
      .withColumn("user_city",col("user_address.city"))\
      .withColumn("user_state",col("user_address.state"))\
      .withColumn("user_postal_code",col("user_address.postal_code"))\
      .withColumn("num_phn_numbers",size(col("user_phone_numbers"))).createOrReplaceTempView("user_vw")


spark.sql("""
        select user_state,sum(male_cnt) as male, sum(female_cnt) as female from 
        (select user_state,
        case when user_gender = 'Male' then count(user_id) end as male_cnt,
        case when user_gender = 'Female' then count(user_id) end as female_cnt
        from user_vw where user_state is not null and user_phone_numbers is not null
        group by user_state,user_gender)
        group by user_state
        order by user_state
""").show()
spark.stop()