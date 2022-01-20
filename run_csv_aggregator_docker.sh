#!/bin/sh

docker build . -t csv_aggr_cmd-container

docker run -it csv_aggr_cmd-container
