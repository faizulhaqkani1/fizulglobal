'''To set up spark history server, download the below script and run it in your vm
https://drive.google.com/file/d/1zDpO__vF3YDAWVYyx2TPIV8DhX45ODVX/view?usp=drive_link
bash /home/hduser/config_spark_history.sh
#Also add the below lines of code in the spark code
      .config("spark.eventLog.enabled", "true") \
      .config("spark.eventLog.dir", "file:///tmp/spark-events") \
      .config("spark.history.fs.logDirectory", "file:///tmp/spark-events") \
'''
from pyspark.sql.functions import *
from pyspark.sql.types import *
def main():
   from pyspark.sql import SparkSession
   # define spark configuration object
   spark = SparkSession.builder\
      .appName("HDFS <-> GCS & Hive <-> GCS Read/Write Usecase 1 & 2") \
      .config("spark.eventLog.enabled", "true") \
      .config("spark.eventLog.dir", "file:///tmp/spark-events") \
      .config("spark.history.fs.logDirectory", "file:///tmp/spark-events") \
      .config("spark.jars", "/home/hduser/gcp/gcs-connector-hadoop2-2.2.7.jar")\
      .enableHiveSupport()\
      .getOrCreate()
   #GCS Jar location
   spark.sparkContext.setLogLevel("ERROR")
   conf = spark.sparkContext._jsc.hadoopConfiguration()
   conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
   conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
   conf.set("google.cloud.auth.service.account.json.keyfile","/home/hduser/gcp/fizul-463702-e2b3aa60a1ff.json")

   print("Usecase 1 - Data Transfer between HDFS to GCS and Vice versa")
   hdfs_df=spark.read.csv("hdfs://localhost:54310/user/hduser/datatotransfer/")
   print("HDFS Read Completed Successfully")
   hdfscnt=hdfs_df.count()
   curts = spark.createDataFrame([1], IntegerType()).withColumn("curts", current_timestamp()).select(date_format(col("curts"), "yyyyMMddHHmmSS")).first()[0]
   print(curts)
   hdfs_df.coalesce(1).write.csv("gs://fizul-first-bucket/custdata_"+curts)
   print("GCS Write Completed Successfully")

   gcs_df = spark.read.option("header", "false").option("delimiter", ",")\
      .csv("gs://fizul-first-bucket/custdata_"+curts).toDF("custid","fname","lname","age","profession")
   gcs_df.cache()
   gcs_df.show(2)
   gcscnt=gcs_df.count()
   #Reconcilation
   if (hdfscnt==gcscnt):
      print("GCS Write Completed Successfully including Data Quality/Reconcilation check completed (equivalent to sqoop --validate)")
   else:
      print("Count is not matching - Possibly GCS Write Issue")
      exit(1)

   print("Reading & Writing data into hive table")
   print("Writing data from GCS to Hive table")
   gcs_df.write.mode("overwrite").saveAsTable("retail.custs")
   print("GCS to Hive Write Completed Successfully")

   print("Reading data from hive table")
   df_hive=spark.read.table("retail.txnrecords")
   df_hive.write.json("gs://fizul-first-bucket/txndata_json_"+curts)
   print("Hive to GCS Write Completed Successfully")

main()
