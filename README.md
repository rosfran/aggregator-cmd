# CSV Aggregator

A solution to merge in a sorted way a list of CSV files. 

# Description

    The main feature is implemented by the service CSVAggregatorService. It reads a list of CSV files asynchronously, 
    sort all this files in increasing order (based on the only column value), write all these ordered values into
    a unique CSV file. The CSV aggregator procedure uses Java 8 parallel stream for reading and sorting files.

# Compiling and packaging

    This application uses Java 11 with Maven version 3.8. You can build it with: mvn compile package
    It will creates the file target/aggregator-cmd-0.0.1.jar

# Running it

    Refer to the shell files below, to see examples on using the command line arguments:
    
    run_csv_aggregator_medium_strategy_external_sort.sh
    run_csv_aggregator_small_strategy_parallel.sh

# Reading, sorting and merging strategies
    
    The application implements 4 different ways for reading, sorting and merging the CSV entries.

### external - Implements an External Sort algorithm. The main difference from the others is that it
###           splits off all file paths in groups, which are processed in groups by the Task (Worker Thread)
###           Implemented by class: com.mendix.csv.aggregator.parallel.impl.ParallelProcessingExternalSortImpl

### parallel - Implements a parallel algorithm using Java Parallel Streams. The main difference from the others is that
###           the decision on the Task's distribution and concurrency are given to the internal mechanisms inside Java Streams.
###           Implemented by class: com.mendix.csv.aggregator.parallel.impl.ParallelProcessingStreamImpl

### sequential - Implements a sequantial algorithm using Java Streams. The main difference from the others is that
###           it uses sequential Java Streams.
###           Implemented by class: com.mendix.csv.aggregator.parallel.impl.ParallelProcessingNoParallelImpl

### forkjoin - Implements a parallel algorithm running on a ForkJoin pool. The main difference from the others is that 
###           each individuals Tasks runs only one file entry. 
###           Implemented by class: com.mendix.csv.aggregator.parallel.impl.ParallelProcessingForkJoinImpl
    
# Running it

    The application runs through the command line. There are 3 arguments that may be passed to the application:

### fromDir  - Points to the directory where all CSV files are located
### toFile   - It is the filename where all the CSV entries will be stored and sorted
### strategy - It will define the strategy that is being used to read, sort and merge all CSV files. 
###            This argument is optional. If it is not informed, the default strategy is ForkJoin
###            Valid values to the strategy argument: external, parallel, sequential and forkjoin

# Example

java -jar target/aggregator-cmd-0.0.1.jar strategy=external fromDir=src/main/resources/medium_example/ toFile=src/main/resources/merged_file.dat

# Test cases

    The class com.mendix.csv.aggregator.AggregatorApplicationTests, in the test Maven profile, uses JUnit
    to implement some interesting test cases.

* testMediumFilesProcessingExternalSorting
* testMediumFilesProcessingForkJoin
* testMediumFilesProcessingParallelStream
* testSmallFilesProcessingExternalSorting
* testSmallFilesProcessingForkJoin
* testSmallFilesProcessingParallelStream

# Command-line (shell) utilities

## run_csv_aggregator.sh
    This shell script runs the JAR file directly (doesn't uses the Docker image). It is possible to pass arguments
    through the command line

## run_csv_aggregator_medium_strategy_external_sort.sh
    Runs the External Sort strategy for the directory with medium CSV entries.

## run_csv_aggregator_small_strategy_parallel.sh
    Runs the Parallel strategy for the directory with small CSV entries.

## run_csv_aggregator_tests.sh
    Runs the Unit test cases.