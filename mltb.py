from pyspark import SparkFiles
from pyspark.sql.functions import array, col, explode, struct, lit
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql import SparkSession, SQLContext, Window

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
	input_df = input_df.withColumn("daily_cases", input_df.cases - when(F.lag(input_df.cases).over(window_spec).isNull(),0).otherwise(F.lag(input_df.cases).over(window_spec)))
	
	return input_df


def find_monthly_avg_cases(df):
	monthly_df = df.withColumn('month', date_format(df.date,'yyyy-MM'))
	monthly_df = monthly_df.groupBy('month','country').agg(avg('daily_cases').alias('avg_cases')).orderBy('avg_cases', ascending=False)
	monthly_df.show()


test_df = get_df()

find_monthly_avg_cases(test_df)

test_df.createOrReplaceTempView("Pre_Dataset")

df_with_null = spark.sql('SELECT *, cases - LAG(cases) OVER(PARTITION BY (CASE WHEN province IS NOT NULL THEN province \
                                                                               ELSE country END) \
                                                            ORDER BY (CASE WHEN province IS NOT NULL THEN province \
                                                                           ELSE country END)) AS Null_daily_Cases \
                          FROM Pre_Dataset')

df_with_null.createOrReplaceTempView("Null_Case_DF")

daily_case_df = spark.sql('SELECT *, CASE WHEN Null_Daily_cases IS NOT NULL THEN Null_Daily_Cases ELSE 0 END AS Daily_Cases \
                           FROM Null_Case_DF')

daily_case_df.createOrReplaceTempView("Daily_Case_DF")

#test to see if UK province daily cases show correctly
spark.sql('SELECT * \
           FROM Daily_Case_DF \
           WHERE country ="United Kingdom" AND province IS NULL').show()
