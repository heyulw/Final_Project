from pyspark.sql import SparkSession

from util.config import Config
from util.logger import Log4j

if __name__ == '__main__':
    conf = Config()
    spark_conf = conf.spark_conf
    nc_conf = conf.nc_conf

    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    log.info(f"nc_conf: {nc_conf}")

    socketDF = spark \
        .readStream \
        .format("socket") \
        .option("host", nc_conf.host) \
        .option("port", nc_conf.port) \
        .load()

    socketDF.isStreaming()  # Returns True for DataFrames that have streaming sources

    socketDF.printSchema()