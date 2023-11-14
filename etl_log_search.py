from pyspark.sql import SparkSession 
from pyspark.sql.functions import * 
import pyspark.sql.functions as sf 
from pyspark.sql.window import Window 
import os 
from datetime import datetime, timedelta
from pyspark.sql import Window, types
import pandas as pd

spark = SparkSession.builder.config("spark.driver.memory", "8g").config("spark.jars.packages","com.mysql:mysql-connector-j:8.0.33").getOrCreate()

def convert_to_datevalue(value):
    date_value = datetime.strptime(value,"%Y-%m-%d").date()
    return date_value

def date_range(start_date,end_date):
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime("%Y%m%d"))
        current_date += timedelta(days=1)
    return date_list 

def generate_date_range(from_date,to_date):
    from_date = convert_to_datevalue(from_date)
    to_date = convert_to_datevalue(to_date)
    date_list = date_range(from_date,to_date)
    return date_list

def convert_to_file_names():
    first_date_6 = '2022-06-01'
    last_date_6 = '2022-06-14'
    first_date_7 = '2022-07-01'
    last_date_7 = '2022-07-14'   
    file_names_6 = generate_date_range(first_date_6,last_date_6)
    file_names_7 = generate_date_range(first_date_7,last_date_7)   
    return file_names_6, file_names_7

# Đọc dữ liệu
def read_1_file(path, file_names):
    print('------------------------')
    print('Read data from file')
    print('------------------------')   
    df = spark.read.parquet(path+file_names)
    data_value = datetime.strptime(file_names,"%Y%m%d").date()
    df = df.withColumn('date', sf.lit(data_value))
    print('------------------------')
    print('Showing data')
    print('------------------------')
    df.show() 
    print('------------------------')
    print('Showing data value')
    print('------------------------')
    df.show(10)
    print('------------------------')
    print('Read data from file successfully')
    print('------------------------') 
    return df

# Output được xử lý ngoài để map 
def output_map():
    output_map_6 = pd.read_excel('/Users/thanhnhanak16/Documents/BIG DATA/Project/thang6_output.xlsx',sheet_name='Sheet1')
    output_map_7 = pd.read_excel('/Users/thanhnhanak16/Documents/BIG DATA/Project/thang7_output.xlsx',sheet_name='Sheet1')
    output_6 = spark.createDataFrame(output_map_6)
    output_7 = spark.createDataFrame(output_map_7)
    return output_6,output_7

#Xử lý và tìm most search tháng 6
def process_log_search_t6(df):
    df = df.filter((col('action') == 'search') & (col('user_id').isNotNull()) 
                    & (col('keyword').isNotNull()))
    df = df.groupBy('user_id','keyword').count()
    df = df.withColumnRenamed('count','TotalSearch')
    df= df.orderBy('user_id',ascending = False )
    output_6,output_7 = output_map()
    df = df.join(output_6, on = ['user_id','keyword'],how='inner').select(df['user_id'], df['keyword'], output_6['related content'], output_6['Category'], output_6['Type'], df['TotalSearch'])
    df = df.withColumn('month', lit('2022-06'))
    most_search_6 = df.filter(col('related content')!='Unknown')\
                        .groupBy('user_id','related content','Category','Type').agg(sum('TotalSearch').alias('total_search'))\
                        .withColumn('rank_total_search', row_number().over(Window.partitionBy('user_id').orderBy(col('total_search').desc())))\
                        .filter(col('rank_total_search')==1)
    most_search_6 = most_search_6.drop(col('total_search'),col('rank_total_search'))\
                .withColumnRenamed('related content', 'T6_most_search_content')\
                .withColumnRenamed('Category','T6_Category')\
                .withColumnRenamed('Type','T6_Type')
    return df, most_search_6

#Xử lý và tìm most search tháng 7
def process_log_search_t7(df):
    df = df.filter((col('action') == 'search') & (col('user_id').isNotNull()) 
                    & (col('keyword').isNotNull()))
    df = df.groupBy('user_id','keyword').count()
    df = df.withColumnRenamed('count','TotalSearch')
    df= df.orderBy('user_id',ascending = False )
    output_6,output_7 = output_map()
    df = df.join(output_7,on = ['user_id','keyword'],how='inner').select(df['user_id'], df['keyword'], output_7['related content'], output_7['Category'], output_7['Type'], df['TotalSearch'])
    df = df.withColumn('month', lit('2022-07'))
    most_search_7 = df.filter(col('related content')!='Unknown')\
                        .groupBy('user_id','related content','Category','Type').agg(sum('TotalSearch').alias('total_search'))\
                        .withColumn('rank_total_search', row_number().over(Window.partitionBy('user_id').orderBy(col('total_search').desc())))\
                        .filter(col('rank_total_search')==1)
    most_search_7 = most_search_7.drop(col('total_search'),col('rank_total_search'))\
                .withColumnRenamed('related content','T7_most_search_content')\
                .withColumnRenamed('Category','T7_Category')\
                .withColumnRenamed('Type','T7_Type')
    return df, most_search_7

def import_data_t6_to_mysql(result):
    jdbc_url = "jdbc:mysql://localhost:3306/etl_logs"
    driver = "com.mysql.jdbc.Driver"
    user = "root"
    password = ""
    result.write.format('jdbc').option('url',jdbc_url).option('driver',driver).option('dbtable','data_t6').option('user',user).option('password',password).mode('overwrite').save()
    return print("Data Import Successfully")

def import_data_t7_to_mysql(result):
    jdbc_url = "jdbc:mysql://localhost:3306/etl_logs"
    driver = "com.mysql.jdbc.Driver"
    user = "root"
    password = ""
    result.write.format('jdbc').option('url',jdbc_url).option('driver',driver).option('dbtable','data_t7').option('user',user).option('password',password).mode('overwrite').save()
    return print("Data Import Successfully")

def import_most_search_trend_to_mysql(result):
    jdbc_url = "jdbc:mysql://localhost:3306/etl_logs"
    driver = "com.mysql.jdbc.Driver"
    user = "root"
    password = ""
    result.write.format('jdbc').option('url',jdbc_url).option('driver',driver).option('dbtable','most_search_trend').option('user',user).option('password',password).mode('overwrite').save()
    return print("Data Import Successfully")

def maintask():
    start_time = datetime.now()
    path = '/Users/thanhnhanak16/Documents/BIG DATA/Buổi 4/log_search/'
    file_names_6, file_names_7 = convert_to_file_names()
    #most search thang 6
    file_names_6 = [file for file in file_names_6 if not file.startswith('.DS_Store')]
    file_name_6a = file_names_6[0]
    result_6a = read_1_file(path,file_name_6a)
    for i in file_names_6[1:]:
        file_name_6b = i 
        result_6b = read_1_file(path,file_name_6a)
        result_6a = result_6a.union(result_6b)
        result_6a = result_6a.cache()
    data_t6, most_search_t6 = process_log_search_t6(result_6a)
    most_search_t6.show(10)
    print('------------------------')
    print('Process most search t6 successfully')
    print('------------------------')
    #most search thang 7 
    file_names_7 = [file for file in file_names_7 if not file.startswith('.DS_Store')]
    file_name_7a = file_names_7[0]
    result_7a = read_1_file(path,file_name_7a)
    for i in file_names_7[1:]:
        file_name_7b = i 
        result_7b = read_1_file(path,file_name_7a)
        result_7a = result_7a.union(result_7b)
        result_7a = result_7a.cache()
    data_t7, most_search_t7 = process_log_search_t7(result_7a)
    most_search_t7.show(10)
    print('------------------------')
    print('Process most search t7 successfully')
    print('------------------------')
    #Join most_search t6 và t7 để tìm most search trend
    user_most_search_trend = most_search_t6.join(most_search_t7,'user_id','inner')
    user_most_search_trend = user_most_search_trend\
            .withColumn('search_category_trend',sf.when(col('T6_Category') == col('T7_Category'),'Unchanged').otherwise('Changed'))
    user_most_search_trend = user_most_search_trend.withColumn('category_previous',sf.when(col('search_category_trend')=='Changed',concat_ws('->', col('T6_Category'), col('T7_Category'))).otherwise('Unchanged'))
    user_most_search_trend = user_most_search_trend\
            .withColumn('search_type_trend',sf.when(col('T6_Type') == col('T7_Type'),'Unchanged').otherwise('Changed'))
    user_most_search_trend = user_most_search_trend.withColumn('type_previous',sf.when(col('search_type_trend')=='Changed',concat_ws('->', col('T6_Type'), col('T7_Type'))).otherwise('Unchanged'))
    user_most_search_trend.show()
    print('------------------------')   
    print('Loading data to MySQL')
    print('------------------------')
    import_data_t6_to_mysql(data_t6)
    import_data_t7_to_mysql(data_t7)
    import_most_search_trend_to_mysql(user_most_search_trend)
    end_time = datetime.now()
    time_processing = (end_time-start_time).total_seconds()
    print('It took {} to process the data'.format(time_processing))
    return print('ETL Data Successfully')
maintask()


