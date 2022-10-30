import pyspark
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F

def create_cleansed_layer():
	spark = SparkSession.builder.appName("Demo-Project2").config("spark.master","local").enableHiveSupport().getOrCreate()
	spark

	"""# #Log_Details(raw_layer)"""

	# Read CSV File and Write to Table
	# df = spark.read.option("delimiter"," ").csv("C:\\Users\\kaverip\\Downloads\\299999.text")
	df = spark.read.option("delimiter"," ").csv("s3://managed-kafka-kaveri-new/kafka_log_files/file-topic/0/299999.text")
	df.show(truncate = False)

	# Giving col names to each columns

	
	df_col = (df.select(
	    F.monotonically_increasing_id().alias('row_id'),
	    F.col("_c0").alias("client_ip"),
	    F.split(F.col("_c3")," ").getItem(0).alias("datetime"),
	    F.split(F.col("_c5"), " ").getItem(0).alias("method"),
	    F.split(F.col("_c5"), " ").getItem(1).alias("request"),
	    F.col("_c6").alias("status_code"),
	    F.col("_c7").alias("size"),
	    F.col("_c8").alias("referrer"),
	    F.col("_c9").alias("user_agent")
	    ))

	df_col.printSchema()

if __name__=="__main__":
	create_cleansed_layer()
