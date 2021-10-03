import pyspark
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark.sql.functions import month, col
import ConfigParser 

def main(spark, argv):

	# Assign variables from input

	print("*****"+argv[0]+"************")
	
	config = ConfigParser.ConfigParser()
	config.readfp(open(argv[0]))
	inputPath = config.get('BaseConfig', 'inputPath')
	outputpath = config.get('BaseConfig', 'ouputPath')
	databaseName = config.get('BaseConfig', 'databaseName')
	reportTable = config.get('BaseConfig', 'reportTable')
	inputFile = inputPath + "COVID-19_Vaccinations_in_the_United_States_County-1.xlsx"
	
	schema = StructType([ \
    StructField("Date",TimestampType(),True), \
    StructField("State",StringType(),True), \
    StructField("vaccinated_total",IntegerType(),True), \
    StructField("vaccinated_12+", IntegerType(), True), \
    StructField("vaccidnated_18+", IntegerType(), True), \
    StructField("vaccinated_65+", IntegerType(), True), \
	StructField("Metro_status", StringType(), True) \
	])
	
	  
	covid19_vaction_Rawdf = spark.read.format("com.crealytics.spark.excel") \
        .schema(schema) \
        .option("useHeader", "true") \
        .option("treatEmptyValuesAsNulls", "true") \
        .option("addColorColumns", "False") \
        .option("maxRowsInMey", 2000) \
        .option("sheetName", "COVID-19_Vaccinations_in_the_Un") \
        .load(inputFile)
		
	
	vaccinatedCleanData = covid19_vaction_Rawdf.filter('State != "null" and vaccinated_total > 0 and Metro_status != "null" ')
	vaccinatedCleanData.write.mode('overwrite').format('parquet').save(outputpath+databaseName)
	
	summarizedata =   vaccinatedCleanData.withColumn('quarter',(((month(covid19_vaction_Rawdf.Date)-1)/3)+1).cast('integer')) \
								.withColumn('vactinatedbtw_12_to_18', \
								( covid19_vaction_Rawdf['vaccinated_12+'] - covid19_vaction_Rawdf['vaccidnated_18+'].cast('integer')))
	summarizedata.write.mode('overwrite').format('parquet').save(outputpath+"summerized_data")
	
	StatewisevaccinatedByQuarter = summarizedata.groupby('State','quarter').sum("vactinatedbtw_12_to_18") \
										.select('State','quarter',col('sum(vactinatedbtw_12_to_18)').alias('vaccinated_12_18'))
	
	StatewisevaccinatedByQuarter.write.mode('overwrite').format('parquet').save(outputpath+"statewisevaccinatedbyquarter")
	
	vaccinatedByQuarter = StatewisevaccinatedByQuarter.groupby('quarter').sum('vaccinated_12_18') \
													.select('quarter',col('sum(vaccinated_12_18)').alias('vaccinated_12_18_by_quarter'))
	
	vaccinatedByQuarter.write.mode('overwrite').format('parquet').save(outputpath+reportTable)

	spark.sparkContext.stop()

if __name__ == "__main__":

	print 'Number of arguments:', len(sys.argv), 'arguments passed'
	print 'Argument List:', str(sys.argv)

	# Setting the configuration for the SparkContext
	spark = SparkSession.builder.enableHiveSupport() \
		.appName("COVID-19_Vaccinations_Report_in_the_United_States_County") \
		.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
		.config("spark.sql.parquet.compression.codec", "snappy") \
		.config("hive.exec.dynamic.partition", "true") \
		.config("hive.exec.dynamic.partition.mode", "nonstrict") \
		.getOrCreate()

	main(spark, sys.argv[1:])