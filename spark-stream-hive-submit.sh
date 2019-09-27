#!/bin/bash

./gradlew shadowJar

docker cp cart-stream-processing/build/cart-stream-processing-1.0-SNAPSHOT.jar spark-master:data

docker exec -ti spark-master sh -c  "cd data && /spark/bin/spark-submit --class com.bd.streaming.hive.CartStreamingHiveApp --master spark://spark-master:7077 cart-stream-processing-jar-with-dependencies.jar"