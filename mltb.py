from pyspark import SparkFiles
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession, SQLContext, Window
import numpy as np


def get_df():
	file_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
	spark.sparkContext.addFile(file_url)

	input_df = spark.read.csv(SparkFiles.get("time_series_covid19_confirmed_global.csv"), header=True)

	input_df = input_df.withColumnRenamed('Province/State','province').withColumnRenamed('Country/Region','country')

	index_columns = ['country','province', 'Lat', 'Long']

	columns, dtypes = zip(*((c, t) for (c, t) in input_df.dtypes if c not in index_columns))

	transpose_columns = explode(array([struct(lit(c).alias("date"), col(c).alias("cases")) for c in columns])).alias("transpose_columns")

	input_df = input_df.select(index_columns + [transpose_columns]).select(index_columns + ["transpose_columns.date", "transpose_columns.cases"])
	input_df = input_df.withColumn('date', to_date(input_df.date, 'M/d/yy'))

	continent_df = spark.read.format("csv").option("header", "true").load('/home/gowthaman/git/covid-analytics/continent_mapping.csv')
	input_df = input_df.join(continent_df, on=['country'], how='inner')
	
	window_spec = Window.partitionBy("country", "province").orderBy("date")
	input_df = input_df.withColumn("daily_cases", input_df.cases - when(F.lag(input_df.cases).over(window_spec).isNull(),input_df.cases).otherwise(F.lag(input_df.cases).over(window_spec)))
	input_df = input_df.withColumn('province',coalesce('province','country'))
	input_df = input_df.withColumn("week", weekofyear(input_df.date)).withColumn("day", dayofweek(input_df.date)).withColumn('month', month(input_df.date))
	
	return input_df

def find_monthly_avg_cases(df):
	monthly_df = df.withColumn('month', date_format(df.date,'yyyy-MM'))
	monthly_df = monthly_df.groupBy('country','month').agg(avg('daily_cases').alias('avg_cases')).orderBy('country', 'month')
	return monthly_df

def get_slope(x,y,order=1):
    coeffs = np.polyfit(x, y, order)
    slope = coeffs[-2]
    return float(slope)

def join_continent_data(df):
    spark = SparkSession.builder.getOrCreate()
    
    return continent_merge_df

def find_week_nums(df):    
    """Assigns a week number to each row based on the date
    Input: merged dataset with continents
    Output: dataset with the week numbers
    """
    weekly_df = df.withColumn('week_no', weekofyear(df.date)).select("Continent", "country", "province", "date", "cases", "daily_cases", "week_no")
    weekly_df = weekly_df.withColumn('week_no', weekly_df.week_no - (weekly_df.collect()[0]['week_no'] - 1))
    return weekly_df

def get_stats_continents(df):
	get_slope_udf = F.udf(get_slope, returnType=DoubleType())
	shift_days_udf = F.udf(shift_days, returnType=ArrayType(IntegerType()))
	df_array_values = df.orderBy('continent','province','week','date').groupBy('continent','province','week').agg(collect_list('daily_cases').alias('daily_cases'), collect_list('day').alias('days'))
	df_array_values = df_array_values.withColumn('days', shift_days_udf(F.col('days')))

	stats_df = df_array_values.withColumn('slope', get_slope_udf(F.col('days'), F.col('daily_cases')))
    
	window = Window.partitionBy( stats_df['week']).orderBy(stats_df['slope'].desc())
	stats_df = stats_df.select('continent', 'week', 'daily_cases', rank().over(window).alias('rank')).filter(col('rank') <= 100).orderBy(stats_df.week)
	stats_df = stats_df.withColumn('daily_cases', explode(stats_df.daily_cases))
	stats_df = stats_df.groupBy('continent','week').agg(avg('daily_cases').alias('average'), stddev('daily_cases').alias('deviation'), min('daily_cases').alias('minimum'), max('daily_cases').alias('maximum'))

	return stats_df


def shift_days(days):
    shifted_days = [ day - 1 if day - 1 > 0 else 7 for day in days ]
    return shifted_days


df = get_df()
df.cache()

monthly_average = find_monthly_avg_cases(df)
continents_stats = get_stats_continents(df)