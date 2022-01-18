#!/bin/sh

docker build . -t csv_aggr-container

docker run -it csv_aggr-container
