#!/bin/bash

docker build -t mycollab/jupyter-bigdata:latest .
docker push mycollab/jupyter-bigdata:latest