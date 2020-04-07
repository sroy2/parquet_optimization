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

def whoami():
    '''Helper function to return user netid
    
    Returns
    -------
    netid : string
    '''
    import getpass
    return getpass.getuser()

def copy(spark):
    '''Helper function to copy the lab4 csv files to parquet.
    
    Returns
    -------
    nothing... but it saves the three parquet files to your hdfs.
    '''
    netid = whoami()
    for x in ['people_small', 'people_medium', 'people_large']:
        df = spark.read.csv('hdfs:/user/bm106/pub/'+x+'.csv', header=True, schema='first_name STRING, last_name STRING, income FLOAT, zipcode INT')
        try:
            df.write.parquet('hdfs:/user/'+netid+'/'+x+'.parquet')
        except:
            print(f"Are you sure {x}.parquet isn't already on your hdfs?")
            pass
    
def csv_table(spark, benchmark_cmd, runtype=None, *args, **kwargs):
    '''Helper procedure to make csv formatted table statistics.
    
    Parameters
    ----------
    benchmark_cmd : string
        The benchmark command you want to execute.
    
    runtype : string | None, 'single', 'all', 'csv', 'pq'
    
        Optional parameter, if not set only benchmark_cmd results are printed.
        
        'all'    run all pq|csv queries on all file sizes
        'csv'    runs csv queries with all bm106 csv filepath sizes
        'pq'     runs pq queries with all provided pq filepath sizes
        'both'   runs both, csv and pq queries as stated above
    
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
        
        csv = True if 'csv' in runtype or 'both' in runtype else False
        pq = True if ('pq' in runtype or 'both' in runtype) and pq_path[-1]=='t' else False
        _all = True if 'all' in runtype else False
        
        for q in q_list:
            if not _all and q not in query:
                continue
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


def transform(spark, t_list, name, benchmark_cmd=None, runtype=False, *args, **kwargs):
    '''Helper procedure to transform and write parquet files.
    
    Parameters
    ----------
    t_list : list of tuples
        list of transformations to apply:
            
        ('query','sort','value')
        ('query','repartition','value')
        
        where query = 'pq_avg_income'|0, 'pq_max_income'|1, 'pq_anna'|2
        
    name : string
        string appended to base filename when adding to hdfs
        f'people_small.parquet' -> f'{query}_people_small_{name}.parquet'

    benchmark_cmd : string
        automatically chains csv_table(benchmark) once files are written

    Returns
    -------
    nothing... but it saves the three parquet files to your hdfs
    '''
    import re
    netid = whoami()
    f_list = ['people_small', 'people_medium', 'people_large']
    lookup = {'pq_avg_income':0, 
              'pq_max_income':1, 
              'pq_anna':2}
    dfs = {}
    for f in f_list:
        df = spark.read.parquet('hdfs:/user/'+netid+'/'+f+'.parquet')
        for q in lookup:
            dfs[(f,q)]=df
            for t in t_list:
                if t[0] != q and t[0] != lookup[q]:
                    continue
                elif t[1]=='sort':
                    dfs[(f,q)]=dfs[(f,q)].sort([i.strip() for i in t[2].split(',')])
                elif t[1]=='repartition':
                    dfs[(f,q)]=dfs[(f,q)].partition([i.strip() for i in t[2].split(',')])
                else:
                    print(f'malformed transformation:{t}')
                    
    transformed = list(zip(*t_list))[0]
    for f in f_list: 
        for q in lookup:        
            try:
                if q in transformed or lookup[q] in transformed:
                    dfs[(f,q)].write.parquet('hdfs:/user/'+netid+'/'+q+'_'+f+'_'+name+'.parquet')
                else:
                    _ = dfs.pop((f,q))
                    print(f"no transformations applied to {q}; skipping")
            except:
                print(f"Are you sure {f}_{name}.parquet isn't already on your hdfs?")
                print(f"skipping: {t}")
                pass
        
    if benchmark_cmd:
        for f, q in dfs:
            if q in transformed or lookup[q] in transformed:
                if f in benchmark_cmd:
                    cmd = re.sub("'.*?'", 
                                 f"'hdfs:/user/{netid}/{q}_{f}_{name}.parquet'",
                                 benchmark_cmd)
                    try:
                        print(f"{q}_{name}")
                        csv_table(spark, cmd, runtype)
                    except:
                        print(f"{cmd} broke; going to next")
                        pass

