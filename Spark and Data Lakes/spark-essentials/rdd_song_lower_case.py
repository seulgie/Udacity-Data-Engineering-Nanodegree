
## 
# This is part of the exercises on the Udacity Data Engineering Nanodegree.
##

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Maps and Lazy Evaluation Example") \
    .getOrCreate()

# Starting off with a regular python list
log_of_songs = [
    "Despacito",
    "Nice for what",
    "No tears left to cry",
    "Despacito",
    "Havana",
    "In my feelings",
    "Nice for what",
    "despacito",
    "All the stars"
]

# parallelize the log_of_songs to use with Spark
distributed_song_log_rdd = spark.sparkContext.parallelize(log_of_songs)

# show the original input data is preserved
distributed_song_log_rdd.foreach(print)

# create a python function to convert strings to lowercase
def convert_song_to_lowercase(song):
  return song.lower()


# use the map function to transform the list of songs with the python function that converts strings to lowercase
lower_case_songs = distributed_song_log_rdd.map(convert_song_to_lowercase)
lower_case_songs.foreach(print)

# Use lambda functions instead of named functions to do the same map operation
distributed_song_log_rdd.map(lambda song: song.lower()).foreach(print)
distributed_song_log_rdd.map(lambda x: x.lower()).foreach(print)






