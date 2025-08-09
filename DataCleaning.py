from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from functools import reduce 
from pyspark.ml.feature import Imputer


spark = SparkSession.builder\
.appName('Data Cleaning')\
.getOrCreate()


null_condition = reduce(lambda x, y: x & y, [col(c).isNull() for c in customer_df.columns])
customer_df.filter(null_condition).show()

order_df_cleaned = orders_df.na.drop(subset=['customer_id','order_id','order_status'])
order_df_cleaned.show(10)

order_df_cleaned = order_df_cleaned.fillna({'order_delivered_carrier_date':'9999-12-31'})
order_df_cleaned.show(10)

imputer = Imputer(inputCols=['payment_value'],outputCols=['payment_value_imputed']).setStrategy("mean")
order_payments_df_cleaned = imputer.fit(order_payments_df).transform(order_payments_df)

order_payments_df.filter(order_payments_df.payment_value.isNull()).show()

payment_with_df = order_payments_df.withColumn('payment_value',when(col('payment_value')!=99.33 , col('payment_value')).otherwise(lit(None)))

payment_with_df_cleaned = payment_with_df.withColumn('payment_type' , when(col('payment_type')=='boleto','Bank Transfer').
                                                                      when(col('payment_type')=='credit_card','Credit Card').
                                                                      otherwise('Other'))

customer_df = customer_df.withColumn('customer_zip_code_prefix', col('customer_zip_code_prefix').cast('string'))

customer_df_cleaned = customer_df.dropDuplicates(['customer_id'])
