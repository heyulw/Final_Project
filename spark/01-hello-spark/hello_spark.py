from pyspark.sql import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Hello Spark") \
        .master("spark://spark:7077") \
        .getOrCreate()

    data_list = [("Phuong", 31),
                 ("Huy", 31),
                 ("Bee", 25)]

    df = spark.createDataFrame(data_list).toDF("Name", "Age")
    df.show()
