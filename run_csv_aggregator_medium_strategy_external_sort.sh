#!/bin/sh

java -jar target/aggregator-cmd-0.0.1.jar strategy=external fromDir=src/main/resources/medium_example/ toFile=src/main/resources/merged_file.dat

