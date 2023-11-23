# %% [markdown]
# Chien-Lan Hsueh

# %% [markdown]
# ## Preparation
# Load in needed modules, set up evironment to run PySpark and create a Spark session.

# %%
# import modules used in this assignment
import numpy as np
import pandas as pd
import time
import shutil

# set up to use Spark
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType
from pyspark.sql.functions import window, col

# %%
# create a spark session
spark = SparkSession.builder.getOrCreate()

# %% [markdown]
# ## Task 1: Streaming Data **Without** Overlapping Windows

# %% [markdown]
# We first start an input stream using the `Rate` format and set up the query for the aggregated sum with the following watermark, window and trigger settings:
# - watermark: 5 seconds 
# - windows: 30 seconds long **with no overlap**
# - triggered: every 20 seconds

# %%
# create an input stream with rate of 1 row per second
# execute a query for the aggregated sum:
spdf = spark \
    .readStream \
    .format("rate") \
    .option("rowsPerSecond", 1) \
    .load()

# set watermarking and 30-second windows without overlap
myquery = spdf \
    .withWatermark("timestamp", "5 seconds") \
    .groupBy(window(spdf.timestamp, "30 seconds")) \
    .sum() \
    .writeStream.outputMode("update").format("memory") \
    .trigger(processingTime = "20 seconds") \
    .queryName("task1") \
    .start()

# %% [markdown]
# We can check the results by sending them to the console or save them into a JSON file:

# %%
# view the task 1 results on console
spark.sql("SELECT * FROM task1").show()

# %%
# save the task 1 results in json file in the folder `task1`
# make sure there is no `task1` folder before running the following codes
spark.sql("SELECT * FROM task1") \
    .coalesce(1) \
    .write.format("json") \
    .option("header", "false") \
    .save("task1")

# %%
# stop the query
myquery.stop()

# %% [markdown]
# ## Task 2: Streaming Data **With** Overlapping Windows

# %% [markdown]
# We start another query with an overlapping windows:
# - watermark: 5 seconds 
# - windows: 30 seconds long **with 15 seconds overlap**
# - triggered: every 20 seconds

# %%
# create an input stream with rate of 1 row per second
# execute a query for the aggregated sum:
spdf = spark \
    .readStream \
    .format("rate") \
    .option("rowsPerSecond", 1) \
    .load()

# use the same watermarking and 30-second windows but with a 15-second overlap
myquery2 = spdf\
    .withWatermark("timestamp", "5 seconds") \
    .groupBy(window(spdf.timestamp, "30 seconds", "15 seconds")) \
    .sum() \
    .writeStream.outputMode("update").format("memory") \
    .trigger(processingTime = "20 seconds") \
    .queryName("task2") \
    .start()

# %% [markdown]
# Check the results on console and save into a JSON file:

# %%
# view the task 2 results on console
spark.sql("SELECT * FROM task2").show()

# %%
# save the task 2 results in json file in the folder `task2`
# make sure there is no `task2` folder before running the following codes
spark.sql("SELECT * FROM task2") \
    .coalesce(1) \
    .write.format("json") \
    .option("header", "false") \
    .save("task2")

# %%
# stop the query
myquery2.stop()

# %% [markdown]
# ## Task 3: Streaming Accelerometer Data

# %% [markdown]
# ### Prepartion
# Before we start this task, we will do some preparation:
# 1. Read in raw csv data
# 2. Define helper functions (in execution order):
#     - 2.1 setup_stream(): Set up stream data and folders
#     - 2.2 start_query(): Create and start a query to read from input streams, perform calculation and write the output
#     - 2.3 start_stream(): Start streaming data   

# %% [markdown]
# #### Prepartion 1: Read in the raw csv file
# Read in the raw csv file, check it and list all unique values of pid:

# %%
# specify working folder
TaskDir = "task3"

# raw data file
CSVFile = "all_accelerometer_data_pids_13.csv"

# read in the raw csv data
df_raw = pd.read_csv(CSVFile)

# look at the data
display(df_raw.info())
display(df_raw.head())

# list of unique pid
print(f"The unique values of pid: {df_raw['pid'].unique()}")

# %% [markdown]
# #### Prepartion 2.1: Define a helper function to set up streams
# This function will subset raw data for each stream, create folders for streaming and checkpoints and return those paths:

# %%
# set up stream data and folders
def setup_stream(df_raw, column, lst_stream, dir):
    """
    Subset raw data for each stream, create folders for streaming and checkpoints and return those paths.
    
    df_raw: raw data in dataframe
    column: column in the dataframe df_raw to specify types of data stream
    lst_stream: lst of data streams 
    dir: working folder
    
    Return a zipped list of the following setup for each stream
    dir_stream: list of stream folders for each stream
    dir_checkpoint: list of checkpoint folders for each stream
    lst_df: list of dataframes for each stream
    """
    
    # number of data dumps (data streams)
    n = len(lst_stream)
    
    # extract the data from the raw data for the selected pids
    lst_df = [df_raw[df_raw[column] == _]  for _ in lst_stream]

    # stream data folders for each stream: dir/<stream>/data
    dir_stream = [os.path.join(dir, _, "stream") for _ in lst_stream]
    dir_checkpoint = [os.path.join(dir, _, "checkpoint") for _ in lst_stream]

    # create folders
    ## (1) working folder
    ## (2) in working folder, one data folder for each stream source
    for x in [dir] + dir_stream + dir_checkpoint:
        # create an empty data folder
        try:
            # if not exists, create one
            os.makedirs(x)
        except:
            # if exists, delete and then create one
            shutil.rmtree(x)
            os.makedirs(x)   

    # check if the setup is correct: stream data folders and data
    [display(f"{lst_stream[_]} stream data in folder '{dir_stream[_]}' (checkpoint: {dir_checkpoint[_]})", lst_df[_]) for _ in range(n)]
        
    return list(zip(dir_stream, dir_checkpoint, lst_df))

# %% [markdown]
# #### Prepartion 2.2: Define a helper function to start a spark strucctured streaming query
# This function starts a query to 
# - read a streaming data from a specified data folder (location)
# - cast the `x`, `y` and `z` variables to `double` numeric type
# - calculate the magnitude of acceleration data
# - write the output via a writestream with `append` output mode

# %%
# Create and start a query to read from input streams, perform calculation and write the output
def start_query(spark, stream_setup, dir):
    """
    Start a query to read streaming data, calculate the magnitude of acceleration data and write the output.
    
    spark: a spark session
    stream_setup: a list of the following setup for each stream
        dir_stream: list of stream folders for each stream
        dir_checkpoint: list of checkpoint folders for each stream
        lst_df: list of dataframes for each stream     
    dir: working folder
    """    
    
    # unpack the list of stream setup
    dir_stream, dir_checkpoint, _ = stream_setup
    
    print(f"Set up and start a query for the streaming data from the folder: '{dir_stream}'.", end ="")
    
    # define schema for streaming data to read in
    userSchema = StructType() \
        .add("time", "string").add("pid", "string") \
        .add("x", "string").add("y", "string").add("z", "string")
    
    # read in stream and convert data types
    # calculate mag and select columns for output
    # set up output sink and start the query
    query = spark \
        .readStream \
        .option("sep", ",") \
        .schema(userSchema) \
        .csv(dir_stream) \
        .select(col('time').cast("timestamp"), 'pid', col('x').cast("double"), col('y').cast("double"), col('z').cast("double")) \
        .withColumn('mag', (col('x')**2 + col('y')**2 + col('z')**2)**0.5 ) \
        .select(col('time'), col('pid'), col('mag')) \
        .writeStream \
        .outputMode("append") \
        .format("csv") \
        .option("path", dir) \
        .option("checkpointlocation", dir_checkpoint) \
        .start()
    
    # Make sure we have enough time to get the query up and ready to wait for streaming data
    # (Needs improvement - not a good way to do this, but good enough for now)
    status = query.status
    while status['message'] != "Waiting for data to arrive":
        time.sleep(1)        
        print(".", end ="")      
        status = query.status
    
    print()
    print(query.status)
    print()
    
    # return the query for further operations
    return query

# %% [markdown]
# #### Prepartion 2.3: Define a helper function to start streams
# This function starts generating csv files as streaming data into the specified folders for each stream, The default batch size is 500 rows of data with 10 seconds delay between batches of the streaming data.

# %%
# Start streaming data
def start_stream(stream_setup, batch_size = 500, batches = None, delay = 10):
    """
    For each stream, start generating csv files into the stream folders
    
    stream_setup: a list of the following setup for each stream
        dir_stream: list of stream folders for each stream
        dir_checkpoint: list of checkpoint folders for each stream
        lst_df: list of dataframes for each stream 
    batch_size: number of data rows to dump in each batch
    batches: number of batches to dump. By default, use all of the available data rows
    delay: delay time (in seconds) befor dumping next batch
    """
    
    # unpack the list of stream setup
    dir_stream, dir_checkpoint, lst_df = list(zip(*stream_setup))
    
    # number of data streams
    n = len(stream_setup)
    
    # decide the maximum number of batches without running out of data
    batches_max = min([_.shape[0] // batch_size for _ in lst_df])
    
    # decide number of batches to stream
    batches = batches_max if batches is None else min(batches, batches_max)
    print(f"{batches} batches to stream:")

    # for each pid, generate streaming data and save as csv files in their own stream data folder
    for i in range(batches):
        print(i+1, end = " ")
        for _ in range(n):
            # get the next batch of rows        
            temp = lst_df[_][i*batch_size : (i+1)*batch_size].copy()
            # write to csv file
            temp.to_csv(os.path.join(dir_stream[_], str(i+1) + ".csv"), index = False, header = False)
            
        # delay before sending next batch
        time.sleep(delay)
    
    print("Done!")  

# %% [markdown]
# ### Set up steaming and start the queries
# With all the preparation done, we are ready for task 3. Here are the steps:
# 1. Set up stream data and folders for each selected pid
# 2. Start 2 querries to monitor the stream folders. If any data come in, do the calculation and output the results
# 3. Check the query status
# 4. Start the data streaming to send in data
# 5. Output teh results to a single csv file
# 6. Stop the queries

# %% [markdown]
# #### Step 1
# Set up two streaming data for the personr with pid 'SA0297', 'PC6771':

# %%
# pid of interest
lst_pid = ['SA0297', 'PC6771']

# set up streams for each pid
stream_setup = setup_stream(df_raw, column = "pid", lst_stream = lst_pid, dir = TaskDir)
print(f"There are totally {len(stream_setup)} streams being set up.")

# %% [markdown]
# #### Step 2
# Next we will start 2 queries, one for each data stream, to calulcate the magnitude of acceleration:

# %%
# get queries for each pid and start them
queries = [start_query(spark, _, dir = TaskDir) for _ in stream_setup]
print(f"There are totally {len(queries)} queries started.")

# %% [markdown]
# #### Step 3
# Check the stauts of the queries to confirm they are started and active:

# %%
# report status of the queries
[print(_.status) for _ in queries]

# list active streams
spark.streams.active

# %% [markdown]
# #### Step 4
# Once the queries are up running and ready for data, we can start the data streams:

# %%
# here we use all data available (maximum number of batches)
# in case a quick test is prefered: 
# `start_stream(stream_setup, batch_size = 500, batches = 20, delay = 10)`
start_stream(stream_setup)

# %% [markdown]
# #### Step 5
# Get one single csv file for the results:

# %%
# Use PySpark to read in all "part" files
allfiles = spark.read.option("header","false").csv(os.path.join(TaskDir, "part-*.csv"))

# set up folder for the single csv file
dir_single_csv = os.path.join(TaskDir, "single_csv_file")
try:
    # if exists, delete it
    shutil.rmtree(dir_single_csv)
except:
    # otherwise do nothing
    pass

# Output as CSV file
allfiles \
    .coalesce(1) \
    .write.format("csv") \
    .option("header", "false") \
    .save(dir_single_csv)

# %% [markdown]
# #### Step 6
# Stop the queries:

# %%
# stop all queries
[_.stop() for _ in queries]

# %% [markdown]
# ### Bonus: Queries with More Streams
# The following is a workflow for another test with more streams. (My computer is too slow so I didn't run it...)

# %%
# specify working folder
TaskDir = "task3-2nd"

# pid of interest
lst_pid = ['BK7610', 'DC6359', 'MC7070', 'MJ8002', 'BU4707']

# set up streams for each pid
stream_setup = setup_stream(df_raw, column = "pid", lst_stream = lst_pid, dir = TaskDir)
print(f"There are totally {len(stream_setup)} streams being set up.")

# report status of the queries
[print(_.status) for _ in queries]

# list active streams
spark.streams.active

# a quicker and shorter test:
start_stream(stream_setup, batch_size = 500, batches = 20, delay = 5)

# Use PySpark to read in all "part" files
allfiles = spark.read.option("header","false").csv(os.path.join(TaskDir, "part-*.csv"))

# set up folder for the single csv file
dir_single_csv = os.path.join(TaskDir, "single_csv_file")
try:
    # if exists, delete it
    shutil.rmtree(dir_single_csv)
except:
    # otherwise do nothing
    pass

# Output as CSV file
allfiles \
    .coalesce(1) \
    .write.format("csv") \
    .option("header", "false") \
    .save(dir_single_csv)

# stop all queries
[_.stop() for _ in queries]


