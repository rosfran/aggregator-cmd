#!/bin/sh

docker build . -f Dockerfile_tests -t csv_aggr-container

docker run -it csv_aggr-container

#mvn clean compile test
