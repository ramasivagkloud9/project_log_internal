from pyspark.sql import SparkSession as ss


class SparkHelper:
    def get_spark_session(self):
        """
        The get_spark_session function returns a Spark session object.
        It also enables Hive support and configures the Snowflake connector for Spark.

        :param self: Access the class attributes and methods
        :return: A sparksession object
        """
        return ss.builder.enableHiveSupport().config('spark.jars.packages',
                                                     'net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3').getOrCreate()