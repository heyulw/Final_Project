import time

from pyspark.sql import *
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from user_agents import parse

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("TestExternalPythonLib") \
        .master("spark://spark:7077") \
        .getOrCreate()

    print("Start TestExternalPythonLib")

    data_list = [
        [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
        ],
        [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/92.0.4515.107 Safari/537.36"
        ]
    ]

    df = spark.createDataFrame(data_list).toDF("user_agent")

    df.printSchema()

    def parse_browser(ua):
        user_agent = parse(ua)
        return user_agent.browser.family


    parse_browser_udf = udf(parse_browser, returnType=StringType())

    df = df.withColumn("browser", parse_browser_udf("user_agent"))

    df.printSchema()

    df.show()
    time.sleep(3600)

    print("Stop TestExternalPythonLib")
    spark.stop()
