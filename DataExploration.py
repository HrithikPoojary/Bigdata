from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when,count,sum as _sum
from pyspark.sql.functions import datediff


spark = SparkSession.builder\
.appName('DataExploration')\
.getOrCreate()

customer_df = spark.read.csv(hdfs_path + 'olist_customers_dataset.csv' , header=True,inferSchema=True)
orders_df = spark.read.csv(hdfs_path + 'olist_orders_dataset.csv' , header=True,inferSchema=True)
order_items_df = spark.read.csv(hdfs_path + 'olist_order_items_dataset.csv' , header=True,inferSchema=True)
products_df = spark.read.csv(hdfs_path + 'olist_products_dataset.csv' , header=True,inferSchema=True)
sellers_df = spark.read.csv(hdfs_path + 'olist_sellers_dataset.csv' , header=True,inferSchema=True)
geolocation_df = spark.read.csv(hdfs_path + 'olist_geolocation_dataset.csv' , header=True,inferSchema=True)
order_reviews_df = spark.read.csv(hdfs_path + 'olist_order_reviews_dataset.csv' , header=True,inferSchema=True)
order_payments_df = spark.read.csv(hdfs_path + 'olist_order_payments_dataset.csv' , header=True,inferSchema=True)


customer_df.columns

def missing_values(df,df_name):
    print(f"Missing Values : {df_name} ")
    df.select([count(when(col(c).isNull(),1)).alias(c) for c in df.columns]).show()


for x,y in df_list.items():
    missing_values(x,y)

df_lists = {'customer' : customer_df,
            "orders"   : orders_df ,
            "order items" : order_items_df ,
            'products':products_df,
            'Seller' : sellers_df,
            'geolocation' : geolocation_df,
            'order review' : order_reviews_df,
            'order payment' : order_payments_df }

# Duplicate values
customer_df.groupBy('customer_city').count().filter('count>1').show()
customer_df.groupBy('customer_state').count().orderBy('count',ascending= False).show()
customer_df.groupBy('customer_state').count().orderBy(col('count').desc()).show()

orders_df.groupBy('order_status').count().orderBy(col('count').desc()).show()
order_payments_df.groupBy('payment_type').agg(_sum(col('payment_value')).alias('payment')\
                                              ,count('payment_type').alias('count'))\
                  .orderBy(col('payment').desc()).show()

order_items_df.groupBy('product_id').agg(sum('price').alias('Total_sale')).orderBy(col('Total_sale').desc()).show()

orders_df.select('order_id','order_purchase_timestamp','order_delivered_customer_date').show()

rders_df.select(col('order_delivered_customer_date').isNull()).show()
time_orders_df = orders_df.select('order_id','order_purchase_timestamp','order_delivered_customer_date')

time_orders_df.withColumn('delivery_time',datediff(col('order_delivered_customer_date'),col('order_purchase_timestamp')))\
.orderBy(col('delivery_time').desc()).show()
