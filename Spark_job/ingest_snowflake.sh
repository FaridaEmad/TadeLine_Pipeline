# Run with spark-submit

nohup spark-submit \
  --master local[2] \
  --conf spark.jars=/opt/spark3/jars/spark-sql-kafka-0-10_2.12-3.1.2.jar,/opt/spark3/jars/kafka-clients-2.8.0.jar,/opt/spark3/jars/spark-sql-kafka-0-10_2.12-3.1.2.jar \
  --conf spark.executor.extraClassPath=/opt/spark3/jars/* \
  --conf spark.driver.extraClassPath=/opt/spark3/jars/* \
  /cluster/data_processing/stream/snowflake_ingest.py \
  > /dev/null 2>&1 &

  
  nohup spark-submit \
  --master local[2] \
  --conf spark.driver.host=127.0.0.1 \
  --conf spark.jars=/opt/spark-2.4.8-bin-hadoop2.7/jars/spark-sql-kafka-0-10_2.11-2.4.8.jar,/opt/spark-2.4.8-bin-hadoop2.7/jars/kafka-clients-2.0.0.jar \
  --conf spark.executor.extraClassPath=/opt/spark-2.4.8-bin-hadoop2.7/jars/* \
  --conf spark.driver.extraClassPath=/opt/spark-2.4.8-bin-hadoop2.7/jars/* \
  /spark_job/ingest_snowflake.py \
  > /dev/null 2>&1 &