#!/bin/sh

java -jar target/aggregator-cmd-0.0.1.jar strategy=parallel fromDir=src/main/resources/small_example/ toFile=src/main/resources/merged_file.dat

