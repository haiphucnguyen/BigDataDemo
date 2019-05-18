#!/usr/bin/env bash

docker build -t hbase-base base
docker build -t hbase-master hmaster
docker build -t hbase-regionserver hregionserver
