docker container stop kafka-streaming || true &&
docker container rm kafka-streaming || true &&
docker run -ti --name kafka-streaming \
--network=streaming-network \
-p 4040:4040 \
-v ./:/spark \
-v spark_lib:/opt/bitnami/spark/.ivy2 \
-v spark_data:/data \
-v /home/tailephuc/Desktop/de-coaching-lab/spark/11-kafka-streaming/checkpoint_kafka:/checkpoint_kafka \
-e POSTGRES_HOST=postgres_alternative \
-e POSTGRES_PORT=5432 \
-e POSTGRES_DB=postgres \
-e POSTGRES_USER=postgres \
-e POSTGRES_PASSWORD=UnigapPostgres@123 \
-e KAFKA_BOOTSTRAP_SERVERS='kafka-0:9092,kafka-1:9092,kafka-2:9092' \
-e KAFKA_SASL_JAAS_CONFIG='org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="Unigap@2024";' \
-e PYSPARK_DRIVER_PYTHON='python' \
-e PYSPARK_PYTHON='./environment/bin/python' \
unigap/spark:3.5 bash -c "python -m venv pyspark_venv &&
source pyspark_venv/bin/activate &&
pip install -r /spark/requirements.txt &&
venv-pack -o pyspark_venv.tar.gz &&
spark-submit \
--py-files /spark/util.zip  \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
--archives pyspark_venv.tar.gz#environment \
/spark/11-kafka-streaming/kafka_streaming.py"