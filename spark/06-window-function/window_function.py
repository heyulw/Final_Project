import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f

import util.config as conf
from util.logger import Log4j

if __name__ == '__main__':
    working_dir = os.getcwd()
    print(f"working_dir: {working_dir}")

    spark_conf = conf.get_spark_conf()

    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    summary_df = spark.read.parquet("/data/window-function/summary.parquet")

    log.info("summary_df schema:")
    summary_df.printSchema()

    log.info("summary_df:")
    summary_df.show()

    running_total_window = Window.partitionBy("Country") \
        .orderBy("WeekNumber") \
        .rowsBetween(-2, Window.currentRow)

    running_total_df = summary_df.withColumn("RunningTotal",
                                             f.sum("InvoiceValue").over(running_total_window))

    log.info("running_total_df schema:")
    running_total_df.printSchema()

    log.info("running_total_df:")
    running_total_df.show()
