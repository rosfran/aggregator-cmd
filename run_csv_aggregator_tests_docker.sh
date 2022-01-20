#!/bin/sh

docker build . -f Dockerfile_tests -t csv_aggr_cmd-container

docker run -it csv_aggr_cmd-container

