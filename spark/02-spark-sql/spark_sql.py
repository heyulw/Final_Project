from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .master("spark://spark:7077") \
        .appName("Spark SQL") \
        .getOrCreate()

    surveyDf = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(path="/data/spark-sql/sample.csv")

    surveyDf.printSchema()

    surveyDf.createOrReplaceTempView("survey_view")
    countDf = spark.sql("select Country, count(1) as count from survey_view where Age < 40 group by Country")

    countDf.show()

    genderDf = surveyDf.select('Gender', 'Country') \
        .withColumn('num_male', f.when(col('Gender') == 'Male', 1).otherwise(0)) \
        .withColumn('num_female', f.when(col('Gender') == 'Female', 1).otherwise(0)) \
        .groupBy('Country', 'Gender') \
        .agg(f.sum('num_male').alias('num_male'), f.sum('numJ_female').alias('num_female'))

    genderDf.show()
