import pyspark
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import regexp_replace


def retail_curated_layer():
    spark = SparkSession.builder.enableHiveSupport().config('spark.jars.packages',
                                                            'net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3').getOrCreate()
    spark

    # Read CSV File and Write to Table
    df_curated = spark.read.option("header", True) \
        .csv("D:\\retail_sales_project\\src\\modules\\internal_files\\cleansed_retail_file.csv")

    df_curated.show(truncate=False)

    curated_df = df_curated.withColumn('DiscountAmount', F.when(F.col('DiscountAmount') == 0, "N").otherwise("Y")) \
        .withColumn("salesprice-freight-taxes-promotion",
                    F.col("SalesAmount") - (F.col("TaxAmount") + F.col("Freight"))) \
        .withColumnRenamed('DiscountAmount', 'Discount_present')

    curated_df.show()

    curated_df.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save(
        "D:\\retail_sales_project\\src\\modules\\internal_files\\curated_retail_file.csv")

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
    # curated_df.coalesce(1).write.format("snowflake").options(**sfOptions) \
    #     .option("dbtable", "{}".format(r"curated_log_details")).mode("overwrite").options(header=True).save()

    # CURATED_HIVE TABLE
    curated_df.coalesce(1).write.mode("overwrite").saveAsTable("curated_retail_details")
    curated_hive = spark.sql("select * from curated_retail_details")
    curated_hive.show(truncate=False)

    """# #Data aggregation and reporting """
    """" 1. load_agg_region"""

    agg_region_df = curated_df.groupBy("Category").agg(F.sum("OrderQuantity").alias("OrderQuantity"),
                                                    F.sum("SalesAmount").alias("SalesAmount")).orderBy(
        F.col("OrderQuantity").desc())

    agg_region_df.show()

    agg_region_df.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save(
        "D:\\retail_sales_project\\src\\modules\\internal_files\\load_agg_region_retai.csv")

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
    # agg_region_df.coalesce(1).write.format("snowflake").options(**sfOptions) \
    #     .option("dbtable", "{}".format(r"log_agg_per_devices")).mode("overwrite").options(header=True).save()

    agg_region_df.coalesce(1).write.mode("overwrite").saveAsTable("load_agg_region_retail")
    agg_region_df_hive = spark.sql("select * from load_agg_region_retail")
    agg_region_df_hive.show(truncate=False)

    """## load_agg_category"""

    agg_category_df= agg_region_df.groupBy("Category", "Subcategory").agg(F.sum("OrderQuantity").alias("OrderQuantity"),
                                                                   F.sum("SalesAmount").alias("SalesAmount"))\
        .orderBy(F.col("OrderQuantity").asc())
    w2 = Window.partitionBy("Category").orderBy(F.col("OrderQuantity").desc())
    agg_category_df1 = agg_category_df.withColumn("row", F.row_number().over(w2)) \
        .filter(F.col("row") < 11).drop("row")

    agg_category_df1 .show()

    agg_category_df1.coalesce(1).write.mode("overwrite").format('csv').option("header", True) \
        .save("D:\\retail_sales_project\\src\\modules\\internal_files\\load_agg_category_retail.csv")

    agg_category_df1 .coalesce(1).write.mode("overwrite").saveAsTable("load_agg_category_retail")
    agg_category_df_hive = spark.sql("select * from load_agg_category_retail")
    agg_category_df_hive.show()

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
    #
    # agg_category_df1 .coalesce(1).write.format("snowflake").options(**sfOptions) \
    #     .option("dbtable", "{}".format(r"log_agg_across_devices")).mode("overwrite").options(header=True).save()

    curated_hive = spark.sql("select count(*) from curated_retail_details").show()
    agg_region_df_hive = spark.sql("select count (*) from load_agg_region_retail").show()
    agg_category_df_hive = spark.sql("select count(*) from load_agg_category_retail").show()


if __name__ == '__main__':
    retail_curated_layer()