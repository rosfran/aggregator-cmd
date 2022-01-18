#!/bin/sh

docker-compose --file docker-compose-scaling.yaml up -d --build --scale app=5
