This project contains the demo of the big data technologies such as Hadoop, Spark, Hbase,
Hive, etc.  Instead of using the Cloudera quickstart distribution, which contains the built-in
Hadoop, HBase, etc. and most of them are outdated versions. For example, with the latest Cloudera quickstart
version that released 3 years ago, and its software versions:

* Hadoop: 2.6.0

* HBase: 1.2.0-cdh5.7.0

* Hive: 1.1.0-cdh5.7.0

...

I want to build a distributed standalone environment, which each software run lonely in its own container
and all of them can run well together similar than they work in the production environment. We can upgrade
each service easier by upgrade its image version. This project would be the base toolkit for any of my big data project
later. I hope you find it is useful as well and I am willing to support when you have any problem of running the examples in this project, feel free to submit your ticket!

Requirements
============

* You must install [Docker](https://www.docker.com/) on your machine.

* You should allocate at least 4 GB for your docker. I recommend 6 GB if you have enough resource. Go to Docker Desktop > Preferences > Advanced to configure the available RAM for docker.

I develop the entire project uses Scala, Java on IntelliJ. If you want to enhance this project, I recommend to use IntelliJ as well.

In addition, Java 8 and Scala 2.12 is required.


The technology stack
====================

* Hadoop 2.7.0
* Spark 2.4
* Kafka 2.0.2
* Cassandra 3.0.4
* Hive 2.3.2
* ZooKeeper 3.4.14
* Jupyter latest

Business Model 
===================

It simulates the real e-commerce business 

![Business model](Big-Data-Demo.png)

How to run the demo
===================


## Start all services

Run the script 'run-all-services.sh' 

## Run kafka producer

## Deploy and run Spark streaming job

## Verify data on HDFS

## Run report on Jupyter

Credits
=======

The first version of this project starts with the base project [https://github.com/big-data-europe/docker-hbase](https://github.com/big-data-europe/docker-hbase) when I need to set up the 
couple services of HBase, Hadoop, ZooKeeper. It was fitted to my needs though the HBase is 1.2.6 while I like to work with HBase 2.0.5 so I created the new Dockerfile base on the old ones. Later, 
I added more images like Kafka, Hive and demonstrate my works with Hadoop, Kafka, Spark, Hive, ZooKeeper and more.  


