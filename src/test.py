from pyspark.sql import SparkSession
from pyspark_test import assert_pyspark_df_equal


def create_spark_Session():
    return SparkSession.builder.enableHiveSupport().getOrCreate()


def compare_log_agg_per_device(spark):
    log_per_device_df = spark.sql("select * from log.log_agg_per_device limit 10;")
    cleansed_data_df = spark.sql(
        """select ROW_NUMBER() OVER (ORDER BY dates) - 1 AS row_id, dates as day_hour, client_ip, COUNT(CASE WHEN method = 'GET' then 1 ELSE NULL END) as no_get, COUNT(CASE WHEN method = 'POST' then 1 ELSE NULL END) as no_post, COUNT(CASE WHEN method = 'HEAD' then 1 ELSE NULL END) as no_head from (select * from (select *, substring_index(datetime, ':', 1) as dates from log.cleansed_log_details) as a where dates in (select day_hour from log.log_agg_per_device limit 10)) as b group by dates, client_ip limit 10""")

    print("log agg per device : first 10 ip's --> \n")
    log_per_device_df.show()
    print("\nCleansed Data check --> \n")
    cleansed_data_df.show()

    assert_pyspark_df_equal(log_per_device_df, cleansed_data_df, check_dtype=False)
    print("log per device data Matched")


def compare_log_agg_across_device(spark):
    log_across_device = spark.sql("select * from log.log_agg_across_device limit 1;")
    cleansed_data_df = spark.sql(
        """select ROW_NUMBER() OVER (ORDER BY dates) - 1 AS row_id, dates as day_hour, COUNT(client_ip) as no_of_clients, SUM(no_get) as no_get, SUM(no_post) as no_post, SUM(no_head) as no_head from (select dates, client_ip, COUNT(CASE WHEN method = 'GET' then 1 ELSE NULL END) as no_get, COUNT(CASE WHEN method = 'POST' then 1 ELSE NULL END) as no_post, COUNT(CASE WHEN method = 'HEAD' then 1 ELSE NULL END) as no_head from (select * from (select *, substring_index(datetime, ':', 1) as dates from log.cleansed_log_details) as a where dates in (select day_hour from log.log_agg_per_device limit 10)) as b group by dates, client_ip) as c group by dates""")

    print("\nlog agg across device : first 1 row --> \n")
    log_across_device.show()
    print("\nCleansed Data check --> \n")
    cleansed_data_df.show()

    assert_pyspark_df_equal(log_across_device, cleansed_data_df, check_dtype=False)
    print("log across device data Matched")


if __name__ == "__main__":
    spark = create_spark_Session()
    compare_log_agg_per_device(spark)
    compare_log_agg_across_device(spark)