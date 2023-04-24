
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import datetime
from datetime import *
import pyspark
import logging
import sys
import pyspark.sql.functions as f
import time
from pyspark import SparkConf, SparkContext
import os
from pyspark.sql.functions import col

logger= logging.getLogger(__name__)
logger.setLevel(logging.INFO)
date = datetime.now().strftime("%Y_%m_%d_%I_%M_%S_%p")
#file_name = 'modified_drill.log'
print(f"filename_{date}")
#logger.info('data reading successfully')

logging.basicConfig(filename="Final_Drill.log", filemode="w",format="%(asctime)s - %(levelname)s - %(message)s")
logger.info('STARTED READING DATA FROM DATABASE')
spark = SparkSession.builder \
        .appName("DBeaver Connection") \
        .config("spark.driver.extraClassPath", "BigDatamysql-connector-java-8.0.11") \
        .getOrCreate()

drill_url = "jdbc:mysql:///phpdemo03.kcspl.in/parth_poc?useSSL=false"
jdbcUsername = "admin"
jdbcPassword = "Krish@123"
#properties = {"driver": 'com.mysql.jdbc.Driver'}

amazon_df= spark.read \
          .format("jdbc") \
          .option("url", "jdbc:mysql:///phpdemo03.kcspl.in/parth_poc") \
          .option("dbtable", "amazon") \
          .option("user", jdbcUsername) \
          .option("password", jdbcPassword) \
          .load()

print("STEP 1 AMAZON COLUMNS COUNT",amazon_df.columns)
logger.info(' FETCHED AMAZON DATA FROM DATABASE')

Flipkart_df = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:mysql://phpdemo03.kcspl.in/parth_poc") \
            .option("dbtable", "flipkart") \
            .option("user",jdbcUsername) \
            .option("password", jdbcPassword) \
            .load()
print(" STEP 2: FETCHED FLIPKART TABLE FROM DATABSE:",Flipkart_df.count())
logger.info('FETCHED FLIPKART DATA FROM DATABASE')
#Flipkart_df.show(3)

ecomm_df= spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://phpdemo03.kcspl.in/parth_poc") \
        .option("dbtable", "ecommerce") \
        .option("user",jdbcUsername) \
        .option("password", jdbcPassword) \
        .load()
print(" STEP 3: FETCHED ECOMMERCE TABLE:",ecomm_df.count())
logger.info('FETCHED ECOMM DATA FROM DATABASE')


none_values_replaced_with_null_amazon=amazon_df.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in amazon_df.columns])

#none_values_replaced_with_null.show()

amazon_check_not_null=none_values_replaced_with_null_amazon.select('order_ID', 'date', 'status', 'fulfilment', 'Sales_Channel', 'ship_service_level', 'style', 'sku', 'category', 'size', 'asin', 'courier_status', 'qty', 'currency', 'amount', 'ship_city', 'ship_state', 'ship_postal_code', 'ship_country', 'promotion_ids', 'b2b', 'fulfilled_by', 'name', 'mobile_number', 'user_id', 'product_id', 'email_id')\
    .where('order_ID is not null and date is not null and status is not null and fulfilment is not null and Sales_Channel is not null and ship_service_level is not null and style is not null and sku is not null and  category is not null and size is not null and asin is not null and courier_status is not null and qty is not null and currency is not null and amount is not null and ship_city is not null and ship_state is not null and ship_postal_code is not null  and ship_country is not null and promotion_ids is not null and b2b is not null and fulfilled_by is not null and name is not null and mobile_number is not null and user_id is not null and product_id is not null and email_id is not null')

#check_not_null.show()
print(" STEP 4: TOTAL NOT NULL RECORDS OF AMAZON",amazon_check_not_null.count())
logger.info('FETCHED NOT NULL RECORDS OF AMAZON')

amazon_check_null=none_values_replaced_with_null_amazon.select('order_ID', 'date', 'status', 'fulfilment', 'Sales_Channel', 'ship_service_level', 'style', 'sku', 'category', 'size', 'asin', 'courier_status', 'qty', 'currency', 'amount', 'ship_city', 'ship_state', 'ship_postal_code', 'ship_country', 'promotion_ids', 'b2b', 'fulfilled_by', 'name', 'mobile_number', 'user_id', 'product_id', 'email_id')\
    .where('order_ID is null or date is null or status is null or fulfilment is  null or Sales_Channel is  null or ship_service_level is  null or style is  null or sku is  null or category is  null or size is  null or asin is  null or courier_status is  null or qty is  null or currency is null or amount is  null or ship_city is  null or ship_state is null or ship_postal_code is null or ship_country is  null or promotion_ids is null or b2b is  null or fulfilled_by is null or name is  null or mobile_number is  null or user_id is  null or product_id is null or email_id is null')

#check_null.show()
print("STEP 5: TOTAL NULL RECORDS OF AMAZON",amazon_check_null.count())
logger.info('FETCHED NULL RECORDS OF AMAZON')

amazon_req_not_null=amazon_check_not_null.select('order_id','date','status','category','size','qty','amount','ship_city','ship_postal_code','ship_country','name','product_id','user_id','email_id','mobile_number')
#req_not_null.show()
print("STEP 6: TOTAL REQUIRED NOT NULL COLUMNS OF AMAZON",amazon_req_not_null.count())
logger.info('CREATED A DATAFRAME FOR REQUIRED COLUMNS IN AMAZON')

amazon_add_col_not_null=amazon_req_not_null.withColumn("Source_System",lit("amazon")).withColumn("current_Date",current_timestamp()).withColumn("country",lit("INDIA")).drop("ship_country")
#amazon_add_col_not_null.show()
print("STEP 7:ADDED COLUMNS TO AMAZON NOT NULL",amazon_add_col_not_null.count())
logger.info('ADDED COLUMNS TO AMAZON NOT NULL')

Amazon_rename_not_null=amazon_add_col_not_null.withColumnRenamed("order_ID","Order_ID").withColumnRenamed("date","Date")\
                            .withColumnRenamed("status","Order_Status").withColumnRenamed("category","Product_Name")\
                            .withColumnRenamed("size","Product_Category").withColumnRenamed("qty","Product_Quantity")\
                            .withColumnRenamed("amount","Product_Price").withColumnRenamed("ship_city","City") \
                            .withColumnRenamed("ship_postal_code","Ship_Postal_Code")\
                            .withColumnRenamed("country","Country").withColumnRenamed("name","User_Name")\
                            .withColumnRenamed("product_id","Product_ID").withColumnRenamed("user_id","User_ID")\
                            .withColumnRenamed("email_id","Email_ID").withColumnRenamed("mobile_number","Phone_NO")
#Amazon_rename_not_null.show(3)
logger.info("AMAZON NOT NULL COLUMNS RENAMED")
print("STEP 8: RENAMED COLUMN OF AMAZON TABLE",Amazon_rename_not_null.count())


amazon_req_null=amazon_check_null.select('order_id','date','status','category','size','qty','amount','ship_city','ship_postal_code','ship_country','name','product_id','user_id','email_id','mobile_number')
#req_null.show()
print("STEP 9:REQUIRED NULL COLUMNS OF AMAZON",amazon_req_null.count())


amazon_add_col_null=amazon_req_null.withColumn("Source_System",lit("amazon")).withColumn("current_Date",current_timestamp()).withColumn("country",lit("INDIA")).drop("ship_country")
#amazon_add_col_null.show()
print("STEP 10 : ADDED REQUIRED COLUMNS TO NULL RECORDS OF AMAZON",amazon_add_col_null.count())
logger.info('ADDED REQUIRED COLUMNS TO AMAZON NULL')

Amazon_rename_null=amazon_add_col_null.withColumnRenamed("order_ID","Order_ID").withColumnRenamed("date","Date")\
                            .withColumnRenamed("status","Order_Status").withColumnRenamed("category","Product_Name")\
                            .withColumnRenamed("size","Product_Category").withColumnRenamed("qty","Product_Quantity")\
                            .withColumnRenamed("amount","Product_Price").withColumnRenamed("ship_city","City") \
                            .withColumnRenamed("ship_postal_code","Ship_Postal_Code")\
                            .withColumnRenamed("country","Country").withColumnRenamed("name","User_Name")\
                            .withColumnRenamed("product_id","Product_ID").withColumnRenamed("user_id","User_ID")\
                            .withColumnRenamed("email_id","Email_ID").withColumnRenamed("mobile_number","Phone_NO")
#Amazon_rename_null.show(3)
print("STEP 11: RENAMED COLUMNS OF AMAZON NULL ",Amazon_rename_null.count())
logger.info('RENAMED COLUMNS OF AMAZON NULL')

amazon_final_not_null=Amazon_rename_not_null.select('Order_ID','Date','Order_Status','Product_Name','Product_Category','Product_Quantity','Product_Price','City','Ship_Postal_Code','User_Name','Product_ID','User_ID','Email_ID','Phone_No','Source_System','Current_Date','Country')
#amazon_final_not_null.show()
print("STEP 12: CREATED DATAFRAME FOR FINAL NOT NULL RECORDS ",amazon_final_not_null.count())
logger.info('CREATED DATAFRAME FOR FINAL NOT NULL')

amazon_final_null=Amazon_rename_null.select('Order_ID','Date','Order_Status','Product_Name','Product_Category','Product_Quantity','Product_Price','City','Ship_Postal_Code','User_Name','Product_ID','User_ID','Email_ID','Phone_No','Source_System','Current_Date','Country')
#amazon_final_null.show()
print("STEP 13:CRETAED DATAFRAME FOR FINAL NULL RECORDS OF AMAZON",amazon_final_null.count())
logger.info('CREATED DATAFRAME FOR FINAL AMAZON NULL')

none_values_replaced_with_null_flipkart=Flipkart_df.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in Flipkart_df.columns])
#none_values_replaced_with_null_flipkart.show()

flipart_check_not_null=none_values_replaced_with_null_flipkart.select('date_', 'city_name', 'order_id', 'cart_id', 'dim_customer_key', 'procured_quantity', 'unit_selling_price', 'total_discount_amount', 'product_id', 'total_weighted_landing_price', 'Order_Status', 'mobile_no', 'user_name', 'email_id', 'product_name', 'unit', 'product_type', 'brand_name', 'manufacturer_name', 'l0_category', 'l1_category', 'l2_category', 'l0_category_id', 'l1_category_id', 'l2_category_id').where('date_ is not null and city_name is not null and order_id is not null and cart_id is not null and dim_customer_key is not null and procured_quantity is not null and unit_selling_price is not null and total_discount_amount is not null and product_id is not null and total_weighted_landing_price is not null and Order_Status is not null and mobile_no is not null and user_name is not null and email_id is not null and product_name is not null and unit is not null and product_type is not null and brand_name is not null and manufacturer_name is not null and l0_category is not null and l1_category is not null and l2_category is not null and l0_category_id is not null and l1_category_id is not null and l2_category_id is not null')
#flipart_check_not_null.show()
print('STEP 14: CHECKED NOT NULL RECORDS OF FLIPKART',flipart_check_not_null.count())
logger.info('CHECKED NOT NULL/VALID RECORDS OF FLIPKART')

required_not_null_flipkart=flipart_check_not_null.select('order_id','date_','city_name','product_id','user_name','unit_selling_price','procured_quantity','dim_customer_key','product_type','product_name','email_id','mobile_no','Order_Status')

Flipkart_add_col=required_not_null_flipkart.withColumn("Country",lit("INDIA")).withColumn("Source_System",lit("FlipKart")).withColumn("Current_Date",current_timestamp()).withColumn("Ship_Postal_Code",lit(431503))
Flipkart_rename_col_not_null=Flipkart_add_col.withColumnRenamed("Order_id","Order_ID").withColumnRenamed("date_","Date")\
                                    .withColumnRenamed("Product_id","Product_ID").withColumnRenamed("user_name","User_Name")\
                                    .withColumnRenamed("unit_selling_price","Product_Price").withColumnRenamed("procured_quantity","Product_Quantity")\
                                    .withColumnRenamed("city_name","City").withColumnRenamed("dim_customer_key","User_ID")\
                                    .withColumnRenamed("product_type","Product_category").withColumnRenamed("product_name","Product_Name")\
                                    .withColumnRenamed("user_name","User_Name").withColumnRenamed("email_id","Email_ID").withColumnRenamed("mobile_no","Phone_NO")\
                                    .withColumnRenamed("ship_postal_code","Ship_Postal_Code")
#Flipkart_rename_col_not_null.show(2)
print(" STEP15:FLIPKART NOT NULL COLUMN RENAMED")
logger.info('FLIPKART NOT NULL COLUMN RENAMED')

final_flipkart_not_null=Flipkart_rename_col_not_null.select('Order_ID','Date','Order_Status','Product_Name','Product_Category','Product_Quantity','Product_Price','City','Ship_Postal_Code','User_Name','Product_ID','User_ID','Email_ID','Phone_No','Source_System','Current_Date','Country')
#final_flipkart_not_null.show()

logger.info('CREATED DATAFRAME FOR FINAL FLIPKART NOT NULL')

check_null_flipkart=none_values_replaced_with_null_flipkart.select('date_', 'city_name', 'order_id', 'cart_id', 'dim_customer_key', 'procured_quantity', 'unit_selling_price', 'total_discount_amount', 'product_id', 'total_weighted_landing_price', 'Order_Status', 'mobile_no', 'user_name', 'email_id', 'product_name', 'unit', 'product_type', 'brand_name', 'manufacturer_name', 'l0_category', 'l1_category', 'l2_category', 'l0_category_id', 'l1_category_id', 'l2_category_id').where('date_ is null or city_name is null or order_id is  null or cart_id is  null or dim_customer_key is  null or procured_quantity is  null or unit_selling_price is  null or total_discount_amount is  null or product_id is  null or total_weighted_landing_price is  null or Order_Status is null or mobile_no is  null or user_name is  null or email_id is  null or product_name is  null or unit is  null or product_type is null or brand_name is  null or manufacturer_name is  null or l0_category is null or l1_category is  null or l2_category is  null or l0_category_id is  null or l1_category_id is  null or l2_category_id is  null')
#check_null_flipkart.show()
print('total null flipkart',check_null_flipkart.count())

req_null_flipkart=check_null_flipkart.select('date_','city_name','order_id','product_id','user_name','unit_selling_price','procured_quantity','dim_customer_key','product_type','product_name','email_id','mobile_no','Order_Status')

Flipkart_add_col=req_null_flipkart.withColumn("Country",lit("INDIA")).withColumn("Source_System",lit("FlipKart")).withColumn("Current_Date",current_timestamp()).withColumn("Ship_Postal_Code",lit(431503))
logger.info('COLUMN ADDED TO FLIPKART NULL')

Flipkart_rename_col_null=Flipkart_add_col.withColumnRenamed("Order_id","Order_ID").withColumnRenamed("date_","Date")\
                                    .withColumnRenamed("Product_id","Product_ID").withColumnRenamed("user_name","User_Name")\
                                    .withColumnRenamed("unit_selling_price","Product_Price").withColumnRenamed("procured_quantity","Product_Quantity")\
                                    .withColumnRenamed("city_name","City").withColumnRenamed("dim_customer_key","User_ID")\
                                    .withColumnRenamed("product_type","Product_category").withColumnRenamed("product_name","Product_Name")\
                                    .withColumnRenamed("user_name","User_Name").withColumnRenamed("email_id","Email_ID").withColumnRenamed("mobile_no","Phone_NO")\
                                    .withColumnRenamed("ship_postal_code","Ship_Postal_Code")

#Flipkart_rename_col_null.show()
print("total null values of flipkart",Flipkart_rename_col_null.count())
logger.info('COLUMNS RENAMED IN FLIPKART NULL')
final_flipkart_null=Flipkart_rename_col_null.select('Order_ID','Date','Order_Status','Product_Name','Product_Category','Product_Quantity','Product_Price','City','Ship_Postal_Code','User_Name','Product_ID','User_ID','Email_ID','Phone_No','Source_System','Current_Date','Country')
final_flipkart_null.show()
logger.info('DATAFRAME FOR FINAL FLIPKART NULL')

#ecommerce ETL

ecomm_add_col=ecomm_df.withColumn("Product_Quantity",lit(1)).withColumn("Source_System",lit("ecomm")).withColumn("Current_Date",current_timestamp()).withColumn("ship_country",lit("INDIA")).drop("country")
logger.info('ADDED COLUMNS TO ECOMMERCE')
ecomm_rename_col=ecomm_add_col.withColumnRenamed("event_time","Date").withColumnRenamed("order_status","Order_Status")\
                              .withColumnRenamed("brand","Product_Name").withColumnRenamed("category_code","Product_Category")\
                              .withColumnRenamed("city","City").withColumnRenamed("ship_country","Country").withColumnRenamed("user_id","User_ID")\
                              .withColumnRenamed("user_name","User_Name").withColumnRenamed("email_id","Email_ID")\
                              .withColumnRenamed("mobile_no","Phone_NO").withColumnRenamed("ship_postal_code","Ship_Postal_Code")\
                              .withColumnRenamed("price","Product_Price").withColumnRenamed("product_id","Product_ID")
# ecomm_rename_col.show(3)


print("ecommerce column renamed")
logger.info('ECOMMERCE COLUMNS RENAMED')

#creating dataframe for required columns in ecommerce table to merge
# ecomm_merge=ecomm_rename_col.select("Order_ID","Date","Product_Name","Product_Category","Product_Price","Order_Status","Product_ID","Product_Quantity","City","Country","User_ID","User_Name","Source_System","Email_ID","Phone_NO","Ship_Postal_Code","Current_Date")

ecomm_none_values_replaced_with_null=ecomm_rename_col.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in ecomm_rename_col.columns])
#ecomm_none_values_replaced_with_null.show(10)

# fill_not_null=ecomm_none_values_replaced_with_null.na.fill(" ",["Order_ID","Order_Status"])
# fill_not_null.show()
# print("replaced none value with null")

#ecom_check_not_null_trial=fill_not_null.select('Order_ID','Date','Order_Status','Product_Name','Product_Category','Product_Quantity','Product_Price','City','Ship_Postal_Code','User_Name','Product_ID','User_ID','Email_ID','Phone_NO','Source_System','current_Date', 'Country') .where('Order_ID is not null and Date is not null and Order_Status is not null and Product_Name is not null and Product_Category is not null and Product_Quantity is not null and  Product_Price is not null and  City is not null and  Ship_Postal_Code is not null and  User_Name is not null and  Product_ID is not null and User_ID is not null and Email_ID is not null and Phone_NO is not null and Source_System is not null and current_Date is not null and Country is not null')
# new_df = ecomm_none_values_replaced_with_null.na.fill(" ", ["Order_ID"]).na.fill(" ",["order_status"])
# fill_not_null=ecomm_none_values_replaced_with_null.na.fill(" ",["Order_ID","Order_Status"])

ecomm_check_not_null_value=ecomm_none_values_replaced_with_null.select("Order_ID","Date","Product_ID","Product_Name","Product_Category","Product_Price","Product_Quantity","Order_Status","City","Ship_Postal_Code","Country","User_ID","User_Name","Source_System","Email_ID","Phone_NO","Current_Date")\
     .where(" Order_ID is not NULL AND Date is not NULL AND Product_ID is not NULL AND Product_Name is not NULL AND Product_Category is not NULL AND Product_Price is not NULL AND Product_Quantity is not NULL AND Order_Status is not NULL AND City is not NULL AND Ship_Postal_Code is not NULL AND Country is not NULL AND User_ID is not NULL AND User_Name is not NULL AND Source_System is not NULL AND Email_ID is not NULL AND Phone_NO is not NULL AND Current_Date is not NULL")
print("all valid data",ecomm_check_not_null_value.count())
#ecomm_check_not_null_value.show()
#ecom_check_not_null_trial.show()
# fill_not_null=ecomm_none_values_replaced_with_null.na.fill(" ",["Order_ID","Order_Status"])
#fill_not_null.show()
logger.info('CHECKED ECOMMERCE NOT NULL/VALID RECORDS')
final_ecomm_not_null=ecomm_check_not_null_value.select('Order_ID','Date','Order_Status','Product_Name','Product_Category','Product_Quantity','Product_Price','City','Ship_Postal_Code','User_Name','Product_ID','User_ID','Email_ID','Phone_No','Source_System','Current_Date','Country')
final_ecomm_not_null.show()
logger.info('DATAFRAME FOR FINAL NOT NULL WITH REQUIRED COLUMNS')
# final_ecomm_not_null=fill_not_null.select('Order_ID','Date','Order_Status','Product_Name','Product_Category','Product_Quantity','Product_Price','City','Ship_Postal_Code','User_Name','Product_ID','User_ID','Email_ID','Phone_No','Source_System','Current_Date','Country')
# final_ecomm_not_null.show()

ecomm_check_null_value=ecomm_none_values_replaced_with_null.select("Order_ID","Date","Product_ID","Product_Name","Product_Category","Product_Price","Product_Quantity","Order_Status","City","Country","User_ID","User_Name","Source_System","Email_ID","Phone_NO","Ship_Postal_Code","Current_Date")\
      .where(" Order_ID is NULL OR Date is NULL OR Product_ID is NULL OR Product_Name is NULL or Product_Category is NULL OR Product_Price is NULL OR Product_Quantity is  NULL OR Order_Status is NULL OR City is  NULL OR Country is  NULL OR User_ID is NULL OR User_Name is NULL OR Source_System is NULL OR Email_ID is  NULL OR Phone_NO is NULL OR Ship_Postal_Code is NULL OR Current_Date is NULL")

#
#ecomm_check_null_value.show()
# print("displaying fill_null",ecomm_check_null_value.count())
logger.info('CHECKED ECOMMERCE NULL RECORDS')

final_ecomm_null=ecomm_check_null_value.select('Order_ID','Date','Order_Status','Product_Name','Product_Category','Product_Quantity','Product_Price','City','Ship_Postal_Code','User_Name','Product_ID','User_ID','Email_ID','Phone_No','Source_System','Current_Date','Country')
final_ecomm_null.show()
logger.info('DATAFRAME FOR FINAL NULL RECORDS OF ECOMMERCE')

#adding global_id column to each dataframe of valid data

Global_amazon_valid=amazon_final_not_null.withColumn("Global_ID", concat_ws("-", lit("AZ"), (monotonically_increasing_id() + 10000).cast("string")))
#Global_amazon_valid.show()
Global_flipkart_valid=final_flipkart_not_null.withColumn("Global_ID", concat_ws("-", lit("FLIP"), (monotonically_increasing_id() + 10000).cast("string")))
#Global_flipkart_valid.show()
Global_ecomm_valid=final_ecomm_not_null.withColumn("Global_ID", concat_ws("-", lit("ECOMM"), (monotonically_increasing_id() + 10000).cast("string")))
#Global_ecomm_valid.show()

#merging valid data

union_amazon_flipkart_ecommerce_not_null=Global_amazon_valid.union(Global_flipkart_valid).union(Global_ecomm_valid)
union_amazon_flipkart_ecommerce_not_null.show()
print("total merged valid records of 3 tables",union_amazon_flipkart_ecommerce_not_null.count())
logger.info('MERGED ALL NOT NULL/VALID RECORDS OF ALL THE TABLES TOGETHER')


#changing schema of required column

date=union_amazon_flipkart_ecommerce_not_null.withColumn("Date",to_timestamp(col("Date"),"yyyy-MM-dd HH:mm:ss"))
date.printSchema()
date.show()
logger.info('CHANGED SCHEMA OF COLUMN')
#union_amazon_flipkart_ecommerce_not_null.show()
# print("total not null",union_amazon_flipkart_ecommerce_not_null.count())

#merging data of three tables that holds null records
union_amazon_flipkart_ecommerce_null=amazon_final_null.union(final_flipkart_null).union(final_ecomm_null)
union_amazon_flipkart_ecommerce_null.show()
print("total merged null values of 3 tables",union_amazon_flipkart_ecommerce_null.count())
logger.info('MERGED ALL NULL/ERROR DATA OF 3 TABLES TOGETHER')

#credentials to connect database

drill_url1 = "jdbc:mysql://phpdemo03.kcspl.in/parth_poc?useSSL=false"
jdbcUsername = "admin"
jdbcPassword = "Krish@123"
mode = "overwrite"
properties = {"driver": 'com.mysql.jdbc.Driver'}

#writing final/valid data to database
date.write.format("jdbc").mode(mode) \
    .option("properties", properties) \
    .option("url",drill_url1) \
    .option("dbtable", "merged_final_data") \
    .option("user", jdbcUsername) \
    .option("password", jdbcPassword)\
    .save()
print("total valid data of 3 tables saved successfully on database")

#writing error data to database
union_amazon_flipkart_ecommerce_null.write.format("jdbc").mode(mode) \
    .option("properties", properties) \
    .option("url",drill_url1) \
    .option("dbtable", "merged_error_data") \
    .option("user", jdbcUsername) \
    .option("password", jdbcPassword)\
    .save()

print("total error data of three tables saved successfully on database")