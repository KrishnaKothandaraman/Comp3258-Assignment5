# Comp3258-Assignment5

**Project structure**
```tree
.
├── README.md
└──  src
    ├── Preprocessor.java
    ├── Spark
    │   └── spark_tasks.py
    ├── in
    │   └── search_data.sample
    └── out
        └── part-r-00000
```

## Src

In this directory you will find all the source code for this project.

### Preprocessor.java

This is the implementation of task 1 which performs MapReduce to preprocess the data using Hadoop

### Spark/spark_tasks.py

This file contains the implementation of task 2 and 3 of this project using Apache Spark

### in/search_data.sample

The input file

### out/part-r-00000

The output file after Running Task 1

## Run instructions

*NOTE*: This project assumes you have hadoop and Apache Spark installed locally

### Run Preprocessor.java

1. `javac -cp /path/to/hadoop/share/hadoop/common/hadoop-common-3.3.1.jar:/path/to/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.3.1.jar:/path/to/hadoop/share/hadoop/common/lib/ Preprocessor.java`
2. `jar cf preprocessor.jar Preprocessor*.class`
3. `/path/to/hadoop/bin/hadoop jar preprocessor.jar Preprocessor /path/to/input /path/to/output`

### Run spark tasks
1. `export PYSPARK_PYTHON=/path/to/python-executable`
2. `export PYSPARK_DRIVER_PYTHON=/path/to/python-executable`
3. `/path/to/spark-submit spark_tasks.py -t <task 2,3> -i path/to/input/file`

May the processing speed be with you
