from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, lit, round, percent_rank, concat
from pyspark.sql.functions import when, max, greatest, concat_ws, expr, regexp_replace, size, split
import pyspark.sql.functions as sf 
import os 
from datetime import datetime, timedelta
from pyspark.sql import Window, types

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
    first_date = '2022-04-01'
    last_date = '2022-04-30'
    file_names = generate_date_range(first_date,last_date)
    file_names = [name + '.json' for name in file_names]
    return file_names

def read_1_file(path, file_names):
    print('------------------------')
    print('Read data from file')
    print('------------------------')   
    df = spark.read.json(path+file_names)
    data_value = datetime.strptime(file_names.replace('.json',''),"%Y%m%d").date()
    df = df.withColumn('Date', sf.lit(data_value))
    print('------------------------')
    print('Showing data value')
    print('------------------------')
    df = df.select('_source.*','Date')
    df.show(5)
    print('------------------------')
    print('Read data from file successfully')
    print('------------------------') 
    return df

def etl_process(df):
    print('------------------------')
    print('Start ETL Process')
    print('------------------------')
    df = df.withColumn('Type',when((col('AppName')=='KPLUS')
                                   |(col('AppName')=='CHANNEL'),'TV')
                                   .when((col('AppName')=='APP')
                                    |(col('AppName')=='BHD')|(col('AppName')=='VOD')
                                    |(col('AppName')=='FIMS'),'Movie')
                                    .when((col('AppName')=='CHILD'),'Child')
                                    .when((col('AppName')=='RELAX'),'Relax')
                                    .when((col('AppName')=='SPORT'),'Sport')
                                    .otherwise('Error'))
    print('------------------------')
    print('Showing new data structure')
    print('------------------------')
    df = df.select('Contract','Type','TotalDuration','Date')
    df = df.filter(df.Type != 'Error')
    df.printSchema()
    print('------------------------')
    print('Showing new data')
    print('------------------------')
    latest_date = df.withColumn('LatestDate', sf.max('Date').over(Window.partitionBy('Contract')))
    active = df.withColumn('Active', sf.count('Date').over(Window.partitionBy('Contract'))).distinct()
    columns_to_drop = ['Type','TotalDuration','Date']
    latest_date = latest_date.drop(*columns_to_drop)
    active = active.drop(*columns_to_drop)
    df = df.groupBy('Contract','Type').sum('TotalDuration')
    df = df.withColumnRenamed('sum(TotalDuration)','TotalDuration')
    df = df.cache()
    print('------------------------')
    print('Pivot data')
    print('------------------------')
    result = df.groupBy('Contract').pivot('Type').sum('TotalDuration')
    print('------------------------')
    print('Showing the pivoting result')
    print('------------------------')
    result.show(5)
    print('Finished ETL Processing')
    return result, latest_date, active

def most_watch(df):
    print('------------------------')
    print('Finding most watch type')
    print('------------------------')
    most_watch = greatest(col('TVDuration'), col('MovieDuration'), col('ChildDuration')
                        , col('RelaxDuration'), col('SportDuration'))
    df = df.withColumn('most_watch',when(col('TVDuration') == most_watch,'TV')\
                       .when(col('MovieDuration')== most_watch,'Movie')\
                        .when(col('ChildDuration')== most_watch,'Child')\
                        .when(col('RelaxDuration') == most_watch,'Relax')\
                        .when(col('SportDuration') == most_watch,'Sport')\
                        .otherwise("Unknown"))
    return df 

def customer_taste(df):
    print('------------------------')
    print('Finding customer taste')
    print('------------------------')
    df = df.withColumn('customer_taste',
                    concat_ws('-',
                             when(col('TVDuration') > 0, 'TV'),
                             when(col('MovieDuration') > 0, 'Movie'),
                             when(col('ChildDuration') > 0, 'Child'),
                             when(col('RelaxDuration') > 0, 'Relax'),
                             when(col('SportDuration') > 0, 'Sport')
                             )
                   )
    return df

def engagement_assessment(df):
    print('------------------------')
    print('Assess user engagement')
    print('------------------------')
    df = df.withColumn('total_duration', col('TVDuration') + col('MovieDuration') 
                        + col('ChildDuration') + col('RelaxDuration')
                          + col('SportDuration'))
    windowSpec = Window.orderBy("total_duration")
    df = df.withColumn('total_duration_percentiles',percent_rank().over(windowSpec))
    df = df.withColumn('engagement_assessment', when(col('total_duration_percentiles') < 0.25,'Very Low Engagement')
                       .when((col('total_duration_percentiles') >= 0.25) & (col('total_duration_percentiles') < 0.5),'Low Engagement')
                       .when((col('total_duration_percentiles') >= 0.5) & (col('total_duration_percentiles') < 0.75),'Normal Engagement')
                       .otherwise('High Engagement'))
    df = df.withColumn('total_duration_rank', when(col('total_duration_percentiles') < 0.25,'1')
                       .when((col('total_duration_percentiles') >= 0.25) & (col('total_duration_percentiles') < 0.5),'2')
                       .when((col('total_duration_percentiles') >= 0.5) & (col('total_duration_percentiles') < 0.75),'3')
                       .otherwise('4'))
    return df

def import_to_mysql(result1):
    jdbc_url = "jdbc:mysql://localhost:3306/etl_logs"
    driver = "com.mysql.jdbc.Driver"
    user = "root"
    password = ""
    result1.write.format('jdbc').option('url',jdbc_url).option('driver',driver).option('dbtable','output_etl').option('user',user).option('password',password).mode('overwrite').save()
    return print("Data Import Successfully")

def maintask():
    start_time = datetime.now()
    path = '/Users/thanhnhanak16/Documents/BIG DATA/Buổi 3/log_content/'
    list_file = convert_to_file_names()
    list_file = [file for file in list_file if not file.startswith('.DS_Store')]
    file_name = list_file[0]
    result1 = read_1_file(path,file_name)
    for i in list_file[1:]:
        file_name2 = i
        result2 = read_1_file(path,file_name2)
        result1 = result1.union(result2)
        result1 = result1.cache()
    result1, latest_date, active = etl_process(result1)
    result1 = result1.drop('Date')
    result1 = result1.withColumnRenamed('TV','TVDuration')\
                    .withColumnRenamed('Movie','MovieDuration')\
                    .withColumnRenamed('Child','ChildDuration')\
                    .withColumnRenamed('Relax','RelaxDuration')\
                    .withColumnRenamed('Sport','SportDuration')
    result1 = result1.fillna(0)
    result1 = most_watch(result1)
    result1 = customer_taste(result1)
    result1 = engagement_assessment(result1)
    result1 = result1.join(latest_date,['Contract'],'left')
    result1 = result1.join(active,['Contract'],'left')
    result1 = result1.distinct()
    report_date = "2022-05-01"
    result1 = result1.withColumn('frequency',round(col('Active')*100.0/30,1))
    result1 = result1.withColumn('recency',datediff(lit(report_date),col('LatestDate')))
    result1 = result1.cache()
    print('------------------------')
    print('Calculating frequency and recency rank')
    print('------------------------')
    windowSpecFrequency = Window.orderBy("frequency")
    result1 = result1.withColumn('frequency_percentiles', percent_rank().over(windowSpecFrequency))
    result1 = result1.withColumn('frequency_rank', when(col('frequency_percentiles') < 0.25, '1')
                   .when((col('frequency_percentiles') >= 0.25) & (col('frequency_percentiles') < 0.5), '2')
                   .when((col('frequency_percentiles') >= 0.5) & (col('frequency_percentiles') < 0.75), '3')
                   .otherwise('4'))
    windowSpecRecency = Window.orderBy("recency")
    result1 = result1.withColumn('recency_percentiles', percent_rank().over(windowSpecRecency))
    result1 = result1.withColumn('recency_rank', when(col('recency_percentiles') < 0.25, '1')
                   .when((col('recency_percentiles') >= 0.25) & (col('recency_percentiles') < 0.5), '2')
                   .when((col('recency_percentiles') >= 0.5) & (col('recency_percentiles') < 0.75), '3')
                   .otherwise('4'))
    print('------------------------')
    print('User segmentation using RFM')
    print('------------------------')
    #r is recency_rank, f is frequency_rank, m is total_duration_rank
    result1 = result1.withColumn('rfm_score', concat(col('recency_rank')
                            , col('frequency_rank'), col('total_duration_rank')))
    result1 = result1.cache()
    result1 = result1.withColumn('user_segmentation',
            when((col('recency_rank') <= 2) & 
                (col('frequency_rank') >= 3) &
                (col('total_duration_rank') >= 3), 'Loyal')
            .when((col('recency_rank') <= 2) & 
                (col('frequency_rank') >= 3) &
                (col('total_duration_rank') <= 2), 'Potential')
            .when((col('recency_rank') <=2) & 
                  (col('frequency_rank') <=2) & 
                  (col('total_duration_rank') >= 3), 'Promising')
            .when((col('recency_rank') <= 2) &
                  (col('frequency_rank') <= 2) &
                  (col('total_duration_rank') <= 2), 'Recent')
            .when((col('recency_rank') >= 3) & 
                (col('frequency_rank') >= 3) &
                (col('total_duration_rank') >= 3), 'Cannot Lose')
            .when((col('recency_rank') >= 3) & 
                (col('frequency_rank') >= 3) &
                (col('total_duration_rank') <= 2), 'Need Attention')
            .when((col('recency_rank') >= 3) & 
                  (col('frequency_rank') <= 2) & 
                  (col('total_duration_rank') >= 3), 'At Risk')
            .when((col('recency_rank') >= 3) &
                  (col('frequency_rank') <= 2) &
                  (col('total_duration_rank') <= 2), 'Lost'))
    print('------------------------')
    print('The final ETL Result')
    print('------------------------')
    result1 = result1.select('Contract','most_watch','customer_taste','recency','frequency','total_duration','rfm_score','engagement_assessment','user_segmentation')
    result1.show(5)
    print('------------------------')   
    print('Loading data to MySQL')
    print('------------------------')
    import_to_mysql(result1)
    end_time = datetime.now()
    time_processing = (end_time-start_time).total_seconds()
    print('It took {} to process the data'.format(time_processing))
    return print('ETL Data Successfully')
maintask()

