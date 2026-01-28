
start-dfs.sh
start-yarn.sh

hive --service hiverserver2
hive --service metastore

zookeeper-server-start.sh zookeeper.properties
kafka-server-start.sh server.properties

start-master.sh

spark-submit orders_producer.py --master local[2]
spark-submit orders_save_consumer.py --master local[2]
spark-submit silver_consumer.py --master local[2]


beeline -u jdbc:hive2://localhost:10000/orders

spark-submit \
  order_producer.py
  --master local[2] \
  --conf spark.executor.cores=2 \
  --conf spark.task.cpus=1 \

spark-submit \
  order_save_consumer.py
  --master local[2] \
  --conf spark.executor.cores=2 \
  --conf spark.task.cpus=1 \

spark-submit \
  silver_consumer.py
  --master local[2] \
  --conf spark.executor.cores=2 \
  --conf spark.task.cpus=1 \