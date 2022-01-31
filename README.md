# CSV Aggregator

**A solution to merge in a sorted way a list of CSV (DAT) files. Each file has a word, and these words can be sorted.
The main purpose of the solution given here is to use the multi-threading facilites found on the Java language
to optimize some tasks like reading and sorting the file, using Java Parallel Streams and the Thread/Runnable
constructs and, in some cases, using the Fork/Join Pool (a kind of parallizing strategy that works with some 
advanced Reactive Java implementations, like Futures, Promises and CompletableFutures)**

# Description

**I decided to implement the solution of sorting a collection of text files**

**The main feature is implemented by the service CSVAggregatorService. It is a wrapper for 4 strategies of reading and 
sorting the DAT files. For example, the method processFileUsingExternalSortStrategy uses the External Sort strategy, which
consists on reading a list of CSV files asynchronously, sort all this files in increasing order (based on the only column value), 
write all these ordered values intoa unique CSV file.**

**All the 4 strategies for sorting text files uses Java 8 parallel stream 
for reading and sorting files.**


# Compiling and packaging

**_This application uses Java 11 with Maven version 3.8. You can build it with:_** 

```
mvn compile package
```
**_It will creates the file target/aggregator-cmd-0.0.1.jar_**

# Running it

**_Refer to the shell files below, to see examples on using the command line arguments:_**
    
    run_csv_aggregator_medium_strategy_external_sort.sh
    run_csv_aggregator_small_strategy_parallel.sh
    run_csv_aggregator_medium_fork_join.sh

# Reading, sorting and merging strategies
    
    The application implements 4 different ways for reading, sorting and merging the CSV entries.

### external 
Implements an External Sort algorithm. The main difference from the others is that it
splits off all file paths in groups, which are processed in groups by the Task (Worker Thread),
and inside each of these tasks we have individual sorting tasks for each chunk of file
Implemented by class: ParallelProcessingExternalSortImpl

### parallel 
Implements a parallel algorithm using Java Parallel Streams. The main difference from the others is that
the decision on the Task's distribution and concurrency are given to the internal mechanisms inside Java Streams.
Implemented by class: ParallelProcessingStreamImpl

### sequential 
Implements a sequantial algorithm using Java Streams. The main difference from the others is that
it uses sequential Java Streams.
Implemented by class: ParallelProcessingNoParallelImpl

### forkjoin
Implements a parallel algorithm running on a ForkJoin pool. The main difference from the others is that 
each individuals Tasks runs only one file entry. 
Implemented by class: ParallelProcessingForkJoinImpl
    
**The best performance is for the external strategy. It has almost the double of perfomance when compared with
the forkjoin strategy. But it will depends on the input size (size of each file and the number of files)**

**The usage of Fork Join framework is awesome on recursive tasks, where you have tasks running a lot of 
subtasks. Another great advantage of using Fork Join is the "work stealing" mechanisms, which 
allows that one task may use the task from another task, just in case a given task doesn't have
tasks available.**

# Running it

**The application runs through the command line. There are 3 arguments that may be passed to the application:**

#### _fromDir  - Points to the directory where all CSV files are located_
#### _toFile   - It is the filename where all the CSV entries will be stored and sorted_
#### _strategy - It will define the strategy that is being used to read, sort and merge all CSV files._ 
This last argument is optional. If it is not informed, the default strategy is ForkJoin.
Valid values to the strategy argument: **external, parallel, sequential and forkjoin**

# Example

java -jar target/aggregator-cmd-0.0.1.jar strategy=external fromDir=src/main/resources/medium_example/ toFile=src/main/resources/merged_file.dat

# Test cases

    The class AggregatorApplicationTests, in the test Maven profile, uses JUnit
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


# References

### https://developer.ibm.com/articles/j-java-streams-1-brian-goetz/
### https://en.wikipedia.org/wiki/Fork%E2%80%93join_model
### Doug Lea explaining Fork Join - http://gee.cs.oswego.edu/dl/papers/fj.pdf
