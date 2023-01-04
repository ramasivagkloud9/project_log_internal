import pyspark
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import regexp_replace
from pyspark.sql.window import Window


def create_rawlayer():
    spark = SparkSession.builder.enableHiveSupport() \
        .config('spark.jars.packages',
                'net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3').getOrCreate()
    spark
    df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(
        r"D:\retail_sales\src\inputs\Retail.csv")

    df.printSchema()
    df.show(truncate=False)
    df.coalesce(1).write.mode("overwrite").format('csv') \
        .option("header", True).save("D:\\retail_sales_project\\src\\modules\\internal_files\\raw_retail_file.csv")

    # sfOptions = {
    #     "sfURL": r"https://hisswyy-qi52071.snowflakecomputing.com/",
    #     "sfAccount": "su57550",
    #     "sfUser": "ramasiva",
    #     "sfPassword": "Rama@2022",
    #     "sfDatabase": "RAMA_DB",
    #     "sfSchema": "PUBLIC",
    #     "sfWarehouse": "COMPUTE_WH",
    #     "sfRole": "ACCOUNTADMIN"
    # }
    # df.coalesce(1).write.format("snowflake").options(**sfOptions) \
    #     .option("dbtable", "{}".format(r"raw_retail_details")).mode("overwrite").options(header=True).save()

    """"" # RAW_DATA HIVE TABLE """""
    df.coalesce(1).write.mode("overwrite").saveAsTable("raw_retail_details")
    df_retail = spark.sql("select * from raw_retail_details")
    df_retail.show()

    df_retail = spark.sql("select count(*) from raw_retail_details")
    df_retail.show()


if __name__ == '__main__':
    create_rawlayer()