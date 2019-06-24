mvnw install

docker cp spark-stream/target/cart-stream-processing-jar-with-dependencies.jar spark-master:data

docker exec -ti spark-master sh -c  "cd data && /spark/bin/spark-submit --class com.mekong.streaming.hive.CartStreamingHiveApp --master spark://spark-master:7077 cart-stream-processing-jar-with-dependencies.jar"