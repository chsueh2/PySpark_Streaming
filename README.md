# PySpark Streaming

Analyze, transform and aggregate streaming data with and without overlapping windows by using Spark Structured Streaming.

[Project report](https://htmlpreview.github.io/?https://raw.githubusercontent.com/chsueh2/PySpark_Streaming/main/streaming.html)

Key features:

- PySpark
- Read and Write Streaming Data
- Overlapping Windows
- Data Transform and Aggregation

Modules used:

- `numpy`: Python library used for working with arrays
- `pandas`: data manipulation and analysis. In particular, it offers data structures and operations for manipulating numerical tables and time series
- `time`: time-related functions
- `shutil`: high-level operations on files and collections of files
- `os`: a portable way of using operating system dependent functionality
- `sys`: a portable way of using operating system dependent functionality
- `pyspark.sql`: Important classes of Spark SQL and DataFrames

## Project Report

[Project report](https://htmlpreview.github.io/?https://raw.githubusercontent.com/chsueh2/PySpark_Streaming/main/streaming.html) ([Jupyter Notebook](./streaming.ipynb))

The analysis results with theoretical backgrounds are included.

Chien-Lan Hsueh (chienlan.hsueh at gmail.com)

## Overview and Project Goal

Set up data stream to read data with and without overlapping windows. Add a trigger to slow down how often we do the computations. Perform data transform and aggregation before we write streaming data back.

## Workflow

1. Preparation and Load module
2. Streaming Data 
   - Without Overlapping Windows
   - With Overlapping Windows
3. Example: Streaming Accelerometer Data
   - Define helper functions (in execution order):
     - setup_stream(): Set up stream data and folders
     - start_query(): Create and start a query to read from input streams, perform calculation and write the output
     - start_stream(): Start streaming data
   - Set up steaming and start the queries
     1. Set up stream data and folders for each selected pid
     1. Start 2 querries to monitor the stream folders. If any data come in, do the calculation and output the results
     1. Check the query status
     1. Start the data streaming to send in data
     1. Output teh results to a single csv file
     1. Stop the queries