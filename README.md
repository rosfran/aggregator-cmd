# CSV Aggregator

A solution to merge in a sorted way a list of CSV files. 

# Description

    The main feature is implemented by the service CSVAggregatorService. It reads a list of CSV files asynchronously, 
    sort all this files in increasing order (based on the only column value), write all these ordered values into
    a unique CSV file. The CSV aggregator procedure uses Java 8 parallel stream for reading and sorting files.

# Test cases

    The class com.mendix.csv.aggregator.AggregatorApplicationTests, in the test Maven profile, uses JUnit
    to implement some interesting test cases.
* aggregateCSVMediumFiles
* aggregateCSVSmallFiles

# Command-line (shell) utilities

## run_csv_aggregator.sh
    This shell script runs the JAR file directly (doesn't uses the Docker image)

## run_csv_aggregator_docker.sh
    Build and run a Docker image created for this CSV Aggregator service.

## run_csv_aggregator_docker_compose.sh
    Build and run a Docker image created for this CSV Aggregator service, 
    but using the docker compose structure (docker-compose.yaml).

## run_csv_aggregator_docker_scale.sh
    Build and run a Docker image created for this CSV Aggregator service (using scalability features from docker compose), 
    but using the docker compose structure (docker-compose-scaling.yaml).