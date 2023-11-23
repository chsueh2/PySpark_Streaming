# PySpark Streaming

Analyze, transform and aggregate streaming data with and without overlapping windows by using Spark Structured Streaming.



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

[Project report](https://htmlpreview.github.io/?https://raw.githubusercontent.com/chsueh2/NFL_pandas-on-Spark/main/NFL.html) ([Jupyter Notebook](./NFL.ipynb))

The analysis results with theoretical backgrounds are included.

Chien-Lan Hsueh (chienlan.hsueh at gmail.com)

## Overview and Project Goal

Set up data stream to read data with and without overlapping windows.Add a trigger to slow down how often we do the computations. Perform data transform and aggregation before we write streaming data back.

## Workflow

1. Preparation and Load module
2. Streaming Data Without Overlapping Windows
3. Streaming Data With Overlapping Windows
4. Example: Streaming Accelerometer Data
