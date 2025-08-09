from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
.appName("CustomerDataProcessing")\
.master("yarn")\
.getOrCreate()

df = spark.read\
.option("header" ,"true")\
.csv("/data/customers_1mb.csv")

output = '/data/old_new'
df1.write.mode('overwrite').parquet(output)

