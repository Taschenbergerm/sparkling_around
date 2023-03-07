# Themen:


## Functional 
- PySpark - connect to a cluster 

- withColumn
- pyspark.sql.functions
- select where/filter show drop 
- udf
- Cleaning
  - deduplication 
  - imputation 
  - dropping 

## Stats and Summaries  

- build in: 
    • avg(): Calculates the mean of a column. We can also
    use the mean() function in place of avg().
    • max(): Finds the maximum value for a given column.
    • Mmn(): Finds the minimum value in a given column.
    • sum(): Performs summation on the values of a column.
    • count(): Counts the number of elements in a column.
    • var_samp(): Calculates sample variance. We can use
    the variance() function in place of the var_samp()
    function.
    • var_pop(): If you want to calculate population
    variance, the var_pop() function will be used.
    • stddev_samp(): The sample standard deviation can
    be calculated using the stddev() or stddev_samp()
    function.
    • stddev_pop(): Calculates the population standard
    deviation.
    agg()
    meanVal = corrData.agg({"iv1":"avg","iv2":"avg","iv3":"avg"})
- sampling
  - simple 
  - conditional
  - frequency
- Crunshing
- normal sorting 
- partition sorting

## SQL-Like operations  

- group by 
  - single key 
  - multi key 
- crosstab 
- joins 
  - inner 
  - outer
- merging
- stacking 
- id_generation 

## SparkSQL

- create tables 
- Repeat actions done functional before 
  - select 
  - describe 
- using functions in SQL
  - using aliases -> error 
- UDFs 
- Aggregations -> select where join group by 
- Window functions 
  - rank over 
  - rank over partition
  - Exercise: Student Ranking partitioned by subject order by Rank 
    - getting top two students 
  - distribute vs sort / Shuffling 