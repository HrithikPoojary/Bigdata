from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
.appName("CustomerDataProcessing")\
.master("yarn")\
.getOrCreate()

df = spark.read\
.option("header" ,"true")\
.csv("/data/customers_1mb.csv")

customer_df = spark.read.csv(hdfs_path + 'olist_customers_dataset.csv' , header=True,inferSchema=True)
orders_df = spark.read.csv(hdfs_path + 'olist_orders_dataset.csv' , header=True,inferSchema=True)
order_items_df = spark.read.csv(hdfs_path + 'olist_order_items_dataset.csv' , header=True,inferSchema=True)
products_df = spark.read.csv(hdfs_path + 'olist_products_dataset.csv' , header=True,inferSchema=True)
sellers_df = spark.read.csv(hdfs_path + 'olist_sellers_dataset.csv' , header=True,inferSchema=True)
geolocation_df = spark.read.csv(hdfs_path + 'olist_geolocation_dataset.csv' , header=True,inferSchema=True)
order_reviews_df = spark.read.csv(hdfs_path + 'olist_order_reviews_dataset.csv' , header=True,inferSchema=True)
order_payments_df = spark.read.csv(hdfs_path + 'olist_order_payments_dataset.csv' , header=True,inferSchema=True)


def missing_values(df,df_name):
    print(f"Missing Values : {df_name} ")
    df.select([count(when(col(c).isNull(),1)).alias(c) for c in df.columns]).show()

def define_schema(df,df_name):
    print(f"Schema name :  {df_name}")
    return(df.printSchema())


df_lists = {'customer' : customer_df,
            "orders"   : orders_df ,
            "order items" : order_items_df ,
            'products':products_df,
            'Seller' : sellers_df,
            'geolocation' : geolocation_df,
            'order review' : order_reviews_df,
            'order payment' : order_payments_df }


for x,y in df_list.items():
    missing_values(x,y)
  
for i,j in df_lists.items():
    define_schema(j,i)



output = '/data/old_new'
df1.write.mode('overwrite').parquet(output)

