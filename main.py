import os 
import datetime 

import findspark
findspark.init()
 
from pyspark.sql import SparkSession
from pyspark.sql.window import Window 
from pyspark.sql.functions import * 

def convert_to_datevalue(value):
	date_value = datetime.datetime.strptime(value,"%Y%m%d").date()
	return date_value

def date_range(start_date,end_date):
	date_list = []
	current_date = start_date
	while current_date <= end_date:
		date_list.append(current_date.strftime("%Y%m%d"))
		current_date += datetime.timedelta(days=1)
	return date_list

def generate_date_range(from_date,to_date):                        
	from_date = convert_to_datevalue(from_date)
	to_date = convert_to_datevalue(to_date)
	date_list = date_range(from_date,to_date)
	return date_list

def process_log_search(df):
    df = df.select('user_id','keyword')
    df = df.groupBy('user_id','keyword').count()
    df = df.withColumnRenamed('count','TotalSearch')
    df = df.orderBy('user_id',ascending = False )
    window = Window.partitionBy('user_id').orderBy(col('TotalSearch').desc())
    df = df.withColumn('Rank',row_number().over(window))
    df = df.filter(col('Rank') == 1)
    df = df.withColumnRenamed('keyword','Most_Search')
    df = df.select('user_id','Most_Search')
    return df

def transform_list_file(list_file,month):
    file_name1 = spark.read.parquet(path+list_file[0])
    for i in list_file[1:]:
        file_name2 = spark.read.parquet(path+i)
        file_name1 = file_name1.union(file_name2)
        file_name1 = file_name1.cache()
    
    df = process_log_search(file_name1)
    df = df.withColumnRenamed('Most_Search','Most_Search'+month)
    return df  

def compare_t6_t7(list_file_1, list_file_2):
   
    list_file_1 = transform_list_file(list_file_1, 't6')
    list_file_2 = transform_list_file(list_file_2, 't7')

    joined_user = list_file_1.join(list_file_2, on='user_id', how='inner')

    keyword_search =  spark.read.option("header",True).csv("C:\\Users\\quoctoan1512\\Desktop\\Project2\\key_search.csv")

    user_limit_100 = joined_user.limit(100)
    
    Most_Search_t6  = user_limit_100.select("user_id","Most_Search_t6")
    Most_Search_t6 = Most_Search_t6.withColumnRenamed('Most_Search_t6','Most_Search')
    category_t6 = Most_Search_t6.join(keyword_search, on='Most_Search', how='inner')
    category_t6 = category_t6.withColumnRenamed('Most_Search', 'Most_Search_T6')\
                                .withColumnRenamed('Category', 'Category_T6')
    category_t6= category_t6.select("user_id", "Most_Search_T6","Category_T6")

    Most_Search_t7  = user_limit_100.select("user_id","Most_Search_t7")
    Most_Search_t7 = Most_Search_t7.withColumnRenamed('Most_Search_t7','Most_Search')
    category_t7 = Most_Search_t7.join(keyword_search, on='Most_Search', how='inner')
    category_t7 = category_t7.withColumnRenamed('Most_Search', 'Most_Search_T7')\
                                .withColumnRenamed('Category', 'Category_T7')
    category_t7= category_t7.select("user_id", "Most_Search_T7","Category_T7")

    category_t6_t7 =category_t6.join(category_t7, on='user_id', how='inner')

    return category_t6

spark = SparkSession.builder.config("spark.driver.memory","2g").config("spark.jars.packages","com.mysql:mysql-connector-j:8.0.25").getOrCreate()
path = "D:\\Data\\log_search\\"
jdbc_url = "jdbc:mysql://localhost:3306/project2"
connection_properties = {
    "user": "root",
    "password": "1",
    "driver": "com.mysql.jdbc.Driver"
    }

def main():
    list_file_1 = generate_date_range('20220601','20220614')
    list_file_2 = generate_date_range('20220701','20220714') 

    category_t6_t7 = compare_t6_t7(list_file_1, list_file_2)

    category_t6_t7.write.jdbc(url=jdbc_url, table="Most_Search_T6_7", mode="overwrite", properties=connection_properties)

main()