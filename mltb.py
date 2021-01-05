from pyspark import SparkFiles
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession, SQLContext, Window
import numpy as np

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

def get_df():
	
	file_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
	spark.sparkContext.addFile(file_url)
	input_df = spark.read.csv(SparkFiles.get("time_series_covid19_confirmed_global.csv"), header=True)

	input_df = input_df.withColumnRenamed('Province/State','province')\
	                   .withColumnRenamed('Country/Region','country')
	index_columns = ['country','province', 'Lat', 'Long']

	columns, dtypes = zip(*((c, t) for (c, t) in input_df.dtypes if c not in index_columns))
	transpose_columns = explode(array([struct(lit(c).alias("date"), col(c).alias("cases")) for c in columns])).alias("transpose_columns")

	input_df = input_df.select(index_columns + [transpose_columns])\
	                   .select(index_columns + ["transpose_columns.date", "transpose_columns.cases"])
	input_df = input_df.withColumn('date', to_date(input_df.date, 'M/d/yy'))

	continent_df = spark.read.format("csv").option("header", "true")\
	                         .load('/home/gowthaman/git/covid-analytics/continent_mapping.csv')
	input_df = input_df.join(continent_df, on=['country'], how='inner')\
	                   .select('continent', 'country', 'province', 'Lat', 'Long', 'date', 'cases')
	
	window_spec = Window.partitionBy("country", "province").orderBy("date")
	input_df = input_df.withColumn("daily_cases", input_df.cases - when(F.lag(input_df.cases).over(window_spec).isNull(),input_df.cases)\
				                                       .otherwise(F.lag(input_df.cases).over(window_spec)))
	input_df = input_df.withColumn('province',coalesce('province','country'))
	input_df = input_df.withColumn("week", weekofyear(input_df.date))\
	                   .withColumn("day", dayofweek(input_df.date))\
	                   .withColumn("day_of_month", dayofmonth(input_df.date))\
	                   .withColumn('month', month(input_df.date))\
	                   .withColumn('year', year(input_df.date))

	return input_df

def find_monthly_avg_cases(df):
    """Finds the daily average per month
    Input: original dataset
    """
    count_udf = F.udf(count_rows, returnType=DoubleType())
    
    monthly_df = df.withColumn('month', date_format(df.date,'yyyy-MM'))
    monthly_df = monthly_df.withColumn('daily_cases', when(col('daily_cases') < 0, 0)\
				                      .otherwise(col('daily_cases')))
    
    coal_df = monthly_df.orderBy('country', 'province', 'month', 'year')\
	                .groupBy('country', 'province', 'month', 'year')\
	                .agg(collect_list('daily_cases').alias('daily_cases'))
    coal_df = coal_df.withColumn('days_in_month', count_udf(F.col('daily_cases')))
    coal_df = coal_df.withColumn('daily_cases', explode(coal_df.daily_cases))
    
    window1 = Window.partitionBy('country', 'province', 'month', 'year')\
	            .orderBy('country', 'province', 'month', 'year')
    grouped_df = coal_df.groupBy('country', 'province', 'month', 'year', 'days_in_month')\
                        .agg(sum('daily_cases').alias('sum1'))
    final_df = grouped_df.groupBy('country', 'month', 'year', 'days_in_month')\
	                 .agg(sum('sum1').alias('total_monthly_cases'))
    final_df = final_df.withColumn('daily_avg_per_month', col('total_monthly_cases') / col('days_in_month'))\
                       .select('country', 'month', 'year', 'total_monthly_cases', 'daily_avg_per_month')
    
    return final_df.orderBy('country', 'month', 'year')

def count_rows(input):
    return float(len(input))

def get_slope(x,y,order=1):
    coeffs = np.polyfit(x, y, order)
    slope = coeffs[-2]
    return float(slope)

def get_stats_continents(df):
	get_slope_udf = F.udf(get_slope, returnType=DoubleType())
	replace_negatives_udf = F.udf(replace_negatives, returnType=ArrayType(DoubleType()))
	shift_days_udf = F.udf(shift_days, returnType=ArrayType(IntegerType()))

	df_array_values = df.orderBy('continent','province', 'year','week','date')\
			    .groupBy('continent','province', 'year','week')\
			    .agg(collect_list('daily_cases').alias('daily_cases'), collect_list('day').alias('days'))
	df_array_values = df_array_values.withColumn('daily_cases', replace_negatives_udf(F.col('daily_cases')))

	df_array_values = df_array_values.withColumn('days', shift_days_udf(F.col('days')))
	stats_df = df_array_values.withColumn('slope', get_slope_udf(F.col('days'), F.col('daily_cases')))
	
	window = Window.partitionBy('week', 'year')\
		       .orderBy(stats_df['slope'].desc())
	stats_df = stats_df.select('continent', 'year', 'week', 'days', 'daily_cases', rank().over(window).alias('rank'))\
			   .filter(col('rank') <= 100).orderBy(stats_df.week)
	stats_df = stats_df.withColumn('zip_cols', explode(arrays_zip(stats_df.daily_cases, stats_df.days)))\
			   .select('continent', 'year','week','zip_cols.days','zip_cols.daily_cases')
	stats_df = stats_df.groupBy('continent', 'year', 'week', 'days')\
			   .agg(avg('daily_cases').alias('daily_cases'))
	stats_df = stats_df.groupBy('continent','week', 'year')\
			   .agg(avg('daily_cases').alias('average'), stddev('daily_cases').alias('deviation'), min('daily_cases').alias('minimum'), max('daily_cases').alias('maximum'))

	return stats_df

def shift_days(days):
    shifted_days = [ day - 1 if day - 1 > 0 else 7 for day in days ]
    return shifted_days

def replace_negatives(input):
    non_negatives = [ num for num in input if num >= 0]
    sum = 0
    for num in non_negatives:
        sum = sum + num
        
    if len(non_negatives) == 0:
       non_negatives.insert(0,0)    

    average = sum / len(non_negatives)
    return [num if num >= 0 else average for num in input]

def cluster_top_provinces(df):

    get_slope_udf = F.udf(get_slope, returnType=DoubleType())
    replace_negatives_udf = F.udf(replace_negatives, returnType=ArrayType(DoubleType()))

    df_array_values = df.orderBy('province', 'year','month','date')\
			.groupBy('province','month', 'year')\
			.agg(collect_list('daily_cases').alias('daily_cases'), collect_list('day_of_month').alias('days'), collect_list('date').alias('date'))
    df_array_values = df_array_values.withColumn('daily_cases', replace_negatives_udf(F.col('daily_cases')))

    slope_df = df_array_values.withColumn('slope', get_slope_udf(F.col('days'), F.col('daily_cases')))
    slope_df = slope_df.filter(slope_df.slope != 0)
    window = Window.partitionBy('year', 'month')\
		   .orderBy(slope_df['slope'].desc())
    slope_df = slope_df.select('province', 'year', 'month', 'days', 'daily_cases', 'slope', rank().over(window).alias('rank'))\
		       .filter(col('rank') <= 50).orderBy('month', 'year', 'rank')
    
    month_year = slope_df.select('month', 'year').dropDuplicates().collect()
    
    #Uncomment below line to run few months and make code faster for debugginhg
    #month_year = month_year[1:3]

    cluster_df_list = []
    
    for row in month_year:
        filtered_df = slope_df.filter((slope_df.month == row.month) & (slope_df.year == row.year))
        cluster_df_list.append(kmeans(filtered_df))
        
    from functools import reduce
    from pyspark.sql import DataFrame
    clusters = reduce(DataFrame.unionAll, cluster_df_list)

    return clusters

def kmeans(df):
    assembler = VectorAssembler(inputCols=["slope"],outputCol="features")
    output = assembler.transform(df)

    kmeans = KMeans() \
      .setK(4) \
      .setFeaturesCol("features") \
      .setPredictionCol("cluster")

    model = kmeans.fit(output)
    predictions = model.transform(output)

    return predictions

df = get_df()
df.cache()

monthly_average = find_monthly_avg_cases(df)
continents_stats = get_stats_continents(df)
cluster_top_provinces = cluster_top_provinces(df)
