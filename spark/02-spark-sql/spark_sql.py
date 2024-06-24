from pyspark.sql import SparkSession

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
