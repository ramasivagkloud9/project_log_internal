import pyspark
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import regexp_replace
from pyspark.sql.window import Window


def retail_cleansed_layer():
    spark = SparkSession.builder.enableHiveSupport() \
        .config('spark.jars.packages',
                'net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3').getOrCreate()

    """# #data reading from Log_Details(raw_layer)"""

    # Read CSV File and Write to Table
    cleansed_df = spark.read.option("header", True).csv(
        "D:\\retail_sales_project\\src\\modules\\internal_files\\raw_retail_file.csv")

    cleansed_data = cleansed_df.withColumn("Orderdate", F.to_date("Orderdate", "M/d/yyyy")) \
        .withColumn("Duedate", F.to_date("Duedate", "M/d/yyyy")) \
        .withColumn("Shipdate", F.to_date("Shipdate", "M/d/yyyy"))

    cleansed_data1 = cleansed_data.select(F.col("OrderNumber"),
                                          F.col("ProductName"),
                                          F.col("Color"),
                                          F.col("Category"),
                                          F.col("Subcategory"),
                                          F.col("ListPrice"),
                                          F.col("Orderdate"),
                                          F.col("Duedate"),
                                          F.col("Shipdate"),
                                          F.col("PromotionName"),
                                          F.col("SalesRegion"),
                                          F.col("OrderQuantity"),
                                          F.col("UnitPrice"),
                                          F.col("SalesAmount"),
                                          F.col("DiscountAmount"),
                                          F.col("TaxAmount"),
                                          F.col("Freight"))

    cleansed_retail = cleansed_data1.withColumn('OrderQuantity', F.regexp_replace('OrderQuantity', 'Nan', '1')) \
        .withColumn('OrderQuantity', F.col('OrderQuantity').cast('int')) \
        .withColumn("ListPrice", F.round("ListPrice", 2)) \
        .withColumn("UnitPrice", F.round("UnitPrice", 2)) \
        .withColumn("SalesAmount", F.round("SalesAmount", 2)) \
        .withColumn("DiscountAmount", F.round("DiscountAmount", 2)) \
        .withColumn("TaxAmount", F.round("TaxAmount", 2)) \
        .withColumn("Freight", F.round("Freight", 2)).na.fill("NA")

    cleansed_retail.show()

    cleansed_retail.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save("D:\\retail_sales_project\\src\\modules\\internal_files\\cleansed_retail_file.csv")

    # CLEANSED DATA IN HIVE TABLE
    cleansed_retail.coalesce(1).write.mode("overwrite").saveAsTable("cleansed_retail_details")
    df_retail = spark.sql("select * from cleansed_retail_details")
    df_retail.show()

    df_retail = spark.sql("select count(*) from cleansed_retail_details")
    df_retail.show()

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
    # cleansed_data.coalesce(1).write.format("snowflake").options(**sfOptions) \
    #     .option("dbtable", "{}".format(r"cleansed_retail_details")).mode("overwrite").options(header=True).save()


if __name__ == '__main__':
    retail_cleansed_layer()