import os

import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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

    # flight_time_df = spark.read.parquet("/data/source-and-sink/flight-time.parquet")
    #
    # flight_time_df.printSchema()
    #
    # flight_time_df.show()
    #
    # log.info(f"Num Partitions before: {flight_time_df.rdd.getNumPartitions()}")
    # flight_time_df.groupBy(spark_partition_id()).count().show()
    #
    # partitioned_df = flight_time_df.repartition(5)
    # log.info(f"Num Partitions after: {partitioned_df.rdd.getNumPartitions()}")
    # partitioned_df.groupBy(spark_partition_id()).count().show()
    #
    # partitioned_df.write \
    #     .format("avro") \
    #     .mode("overwrite") \
    #     .option("path", "/data/sink/avro/") \
    #     .save()
    #
    # flight_time_df.write \
    #     .format("json") \
    #     .mode("overwrite") \
    #     .partitionBy("OP_CARRIER", "ORIGIN") \
    #     .option("path", "/data/sink/json/") \
    #     .option("maxRecordsPerFile", 10000) \
    #     .save()

    flight_time_df = spark.read.json(path="/data/sink/json/")
    flight_time_df.printSchema()

    flight_time_df.show()

    flight_time_df.filter(col("CANCELLED") == 1) \
        .withColumn("FL_DATE", f.to_date("FL_DATE", "yyyy-MM-dd")) \
        .withColumn("YEAR", f.year("FL_DATE")) \
        .filter(col("YEAR") == 2000) \
        .filter(col("DEST") == "ATL") \
        .select("DEST", "DEST_CITY_NAME", "FL_DATE", "ORIGIN", "ORIGIN_CITY_NAME", "CANCELLED") \
        .orderBy("FL_DATE") \
        .show()

    flight_time_avro = spark.read.format("avro").option("path", "/data/sink/avro/").load()

    flight_time_avro.filter(col("CANCELLED") == 1) \
        .withColumn("FL_DATE", f.to_date("FL_DATE", "yyyy-MM-dd")) \
        .withColumn("YEAR", f.year("FL_DATE")) \
        .groupBy("OP_CARRIER", "ORIGIN") \
        .agg(f.count("*").alias("NUM_CANCELLED_FLIGHT")) \
        .orderBy("OP_CARRIER", "ORIGIN") \
        .show()

    spark.stop()
