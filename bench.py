#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''A simple pyspark benchmarking utility'''

import time

def benchmark(spark, n_trials, query_maker, *args, **kwargs):
    '''Benchmark a spark query.

    To use this benchmark, you will need to write a function that
    constructs and returns a spark DataFrame object, but does not execute
    any actions.

    The benchmark will then repeatedly call this function to create a new
    DataFrame some `n_trials` times, take a `count()` action on that dataframe,
    and calculate how long the operation took to run.

    The return value of the benchmark is the running time (in seconds)
    for each trial.

    Example usage
    -------------

    >>> def make_query(spark, file_path):
    ...     df = spark.read.csv(file_path)
    ...     df.createOrReplaceTempView('my_table')
    ...     query = spark.sql('SELECT count(distinct SOME_COLUMN) FROM my_table')
    ...     return query
    >>> times = bench.benchmark(spark, 5, make_query,
    ...                         'hdfs:/path/to/my_file.csv')



    Parameters
    ----------
    spark : spark Session object
        This will probably just be the `spark` object from your pyspark environment.

    n_trials : integer >= 1
        The number of times to run the query

    query_maker : function
        This function must take as its first argument the `spark` session object.
        Any additional parameters to this function can be passed through to
        `benchmark()` as in the example given above.

    *args, **kwargs : optional parameters to `query_maker`

    Returns
    -------
    times : list of float
        The running time (in seconds) of each of the `n_trials` timing trials.
    '''
    times = []

    for _ in range(n_trials):
        dataframe = query_maker(spark, *args, **kwargs)

        # Just in case anything is cached, let's purge that
        spark.catalog.clearCache()

        # And make sure the dataframe is not persisted
        dataframe.unpersist()

        # Start the timer
        start = time.time()
        dataframe.count()  # This forces a calculation
        end = time.time()

        # Append the result to our time count
        times.append(end - start)

    return times


def stats(times):
    '''Helper function to get the min, median, and max of a timing list.
    
    Parameters
    ----------
    times : iterable list object with floats
    
    Returns
    -------
    nothing... but it prints out the min, median, and max of times.
    '''
    import statistics
    print(f'min: {min(times):.3f}')
    print(f'med: {statistics.median(times):.3f}')
    print(f'max: {max(times):.3f}')
    
def csv_table(spark, benchmark_cmd, runtype=False, *args, **kwargs):
    '''Helper procedure to make csv formatted table statistics.
    
    Parameters
    ----------
    benchmark_cmd : string
        The benchmark command you want to execute.
    
    runtype : string | None, 'csv', 'pq', 'both'
    
        Optional parameter, if not set only benchmark_cmd results are printed.
        
        Attempts to run all queries of test type and returns results.
        'csv' uses the bm106 csv filepath.
        'pq' uses the filepath provided in the benchmark_cmd.
        'both' attempts both, defaulting to 'csv' behavior if no 'pq' path provided
    
    Returns
    -------
    nothing... but prints out a csv formatted table of statistics with header:
        'query,file,trials,minimum,mediam,maximum'
    '''
    from statistics import median as med
    import queries
    
    _, runs, query, file = benchmark_cmd.split(',')
    runs = int(runs.strip())
    query = query.strip()
    queryname = query.split('.')[-1]
    file = file.strip()[1:-2]
    filename = file.split('/')[-1].split('.')[0]
    
    output = 'query,file,trials,minimum,median,maximum\n'
    if runtype:
        csv_path = 'hdfs:/user/bm106/pub/'
        pq_path = file
        f_list = ['people_small', 'people_medium', 'people_large']
        q_list = ['csv_avg_income', 'csv_max_income', 'csv_anna', 
                   'pq_avg_income', 'pq_max_income', 'pq_anna']
        
        csv = True if runtype=='csv' or runtype=='both' else False
        pq = True if (runtype=='pq' or runtype=='both') and pq_path[-1]=='t' else False
        
        for q in q_list:
            query = query.replace(queryname, q)
            queryname = q
            for f in f_list:
                if csv and q[0]=='c':
                    file = csv_path+filename+'.csv'
                elif pq and q[0]=='p':
                    file = pq_path
                else:
                    continue
                file = file.replace(filename, f)
                filename = f
                t = benchmark(spark, runs, eval(query), file)
                output += f'{q},{f},{runs},{min(t):.3f},{med(t):.3f},{max(t):.3f}\n' 

    else:
        t = benchmark(spark, runs, eval(query), file)
        output += f'{queryname},{filename},{runs},{min(t):.3f},{med(t):.3f},{max(t):.3f}'
    
    print(output)




