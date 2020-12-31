from pyspark import SparkFiles
from pyspark.sql.functions import array, col, explode, struct, lit
from pyspark.sql.window import Window

def get_df():

	file_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
	spark.sparkContext.addFile(file_url)


	input_df = spark.read.csv(SparkFiles.get("time_series_covid19_confirmed_global.csv"), header=True)

	input_df = input_df.withColumnRenamed('Province/State','province').withColumnRenamed('Country/Region','country')

	index_columns = ['country','province', 'Lat', 'Long']

	columns, dtypes = zip(*((c, t) for (c, t) in input_df.dtypes if c not in index_columns))

	transpose_columns = explode(array([struct(lit(c).alias("date"), col(c).alias("cases")) for c in columns])).alias("transpose_columns")

	input_df = input_df.select(index_columns + [transpose_columns]).select(index_columns + ["transpose_columns.date", "transpose_columns.cases"])
	return input_df

test_df = get_df()

w =  Window.partitionBy(test_df.country).orderBy(test_df.country)

daily_case_df = test_df.withColumn("diff", col("cases") - when((lag("cases", 1).over(w)).isNull(), test_df.head().cases)
                   .otherwise(lag("cases", 1).over(w)))
daily_case_df.show()
