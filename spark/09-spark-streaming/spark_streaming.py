from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

import util.config as conf
from util.logger import Log4j

if __name__ == "__main__":
    spark_conf = conf.get_spark_conf()

    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    # Create a local StreamingContext with two working thread and batch interval of 1 second
    sc = spark.sparkContext
    ssc = StreamingContext(sc, 5)

    # Create a DStream that will connect to hostname:port, like localhost:9999
    lines = ssc.socketTextStream("netcat", 9999)

    # Split each line into words
    words = lines.flatMap(lambda line: line.split(" "))

    # Count each word in each batch
    pairs = words.map(lambda word: (word, 1))
    wordCounts = pairs.reduceByKey(lambda x, y: x + y)

    # Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.pprint()

    ssc.start()  # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate
