package org.bendable.csv.aggregator.parallel;

/**
 * Defines the 4 strategy types for reading and sorting files.
 */
public enum ParallelStrategyType
{

    PARALLEL_STREAM     ("Parallel Java Stream"),
    EXTERNAL_SORT       ("External Sort"),
    SEQUENTIAL_STREAM   ("Sequential Java Stream"),
    FORK_JOIN           ("Fork/Join")
    ;

    private final String description;

    ParallelStrategyType(String description) {
        this.description = description;
    }

    public String getDescription() {
        return this.description;
    }

    static public ParallelStrategyType fromArgument( String str )
    {
        str = str.toLowerCase();
        if ( str.indexOf("parallel") != -1 )
        {
            return PARALLEL_STREAM;
        } else if ( str.indexOf("external") != -1 )
        {
            return EXTERNAL_SORT;
        } else if ( str.indexOf("sequential") != -1 )
        {
            return SEQUENTIAL_STREAM;
        }

        return FORK_JOIN;

    }

}
