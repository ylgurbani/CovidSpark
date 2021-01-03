from pyspark import SparkFiles
from pyspark.sql.functions import array, col, explode, struct, lit
from pyspark.sql.functions import *
import pyspark.sql.functions as f
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
	
	window_spec = Window.partitionBy("country", "province").orderBy("date")
	input_df = input_df.withColumn("daily_cases", input_df.cases - when(F.lag(input_df.cases).over(window_spec).isNull(),input_df.cases).otherwise(F.lag(input_df.cases).over(window_spec)))
	
	return input_df

def find_monthly_avg_cases(df):
	monthly_df = df.withColumn('month', date_format(df.date,'yyyy-MM'))
	monthly_df = monthly_df.groupBy('country','month').agg(avg('daily_cases').alias('avg_cases')).orderBy('country', 'month')
	monthly_df.show()

def get_slope(x,y,order=1):
    coeffs = np.polyfit(x, y, order)
    slope = coeffs[-2]
    return float(slope)

def join_continent_data(df):
    spark = SparkSession.builder.getOrCreate()
    continent_df = spark.read.format("csv").option("header", "true").load('continent_mapping.csv')
    continent_merge_df = df.join(continent_df, on=['country'], how='inner')
    return continent_merge_df

def get_slope_df(df):
	get_slope_udf = F.udf(get_slope, returnType=DoubleType())
	shift_days_udf = F.udf(shift_days, ArrayType(IntegerType()))
	df_with_week_days = df.withColumn("week", weekofyear(df.date)).withColumn("day", dayofweek(df.date)).withColumn('province_or_country',coalesce('province','country'))
	df_with_week_days = df_with_week_days.withColumn("week", df_with_week_days.week - (df_with_week_days.collect()[0]['week'] - 1))
	df_array_values = df_with_week_days.orderBy('province_or_country','week','date').groupBy('province_or_country','week').agg(collect_list('daily_cases').alias('daily_cases'), collect_list('day').alias('days'))
	df_array_values = df_array_values.withColumn('days', shift_days_udf(F.col('days')))
	return df_array_values.withColumn('slope', get_slope_udf(F.col('days'), F.col('daily_cases')))

def shift_days(days):
    shifted_days = [ day - 1 if day - 1 > 0 else 7 for day in days ]
    return shifted_days

test_df = get_df()
find_monthly_avg_cases(test_df)
test_merge = join_continent_data(test_df)
