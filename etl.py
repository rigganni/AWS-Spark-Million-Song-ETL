import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

aws_access_key = config.get("AWS", "AWS_ACCESS_KEY_ID")
aws_secret_key = config.get("AWS", "AWS_SECRET_ACCESS_KEY")
aws_region = config.get("AWS", "AWS_REGION")
os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key
os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_key

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file

    #song_data = input_data + "song_data/*/*/*/*.json"
    song_data = input_data + "song_data/A/A/A/*.json"
    print("start song_data")
    print(song_data)
    print("end song_data")

    # read song data file
    df = spark.read.json(song_data)

    df.printSchema()
    print(df.count())
    df.show(5)
    df.createOrReplaceTempView("song_analysis")
    print(spark.sql("""
    select count(1) as cnt
    from song_analysis
    """).toPandas()['cnt'])
    df.write.parquet("s3a://million-song/output/song-data/test.json", mode="overwrite")
    # extract columns to create songs table
    songs_table = ""

    # write songs table to parquet files partitioned by year and artist
    songs_table

    # extract columns to create artists table
    artists_table = ""

    # write artists table to parquet files
    artists_table


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = ""

    # read log data file
    df = ""

    # filter by actions for song plays
    df = ""

    # extract columns for users table
    artists_table = ""

    # write users table to parquet files
    artists_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = ""

    # create datetime column from original timestamp column
    get_datetime = udf()
    df = ""

    # extract columns to create time table
    time_table = ""

    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = ""

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = ""

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()

    # Set log level to error only to reduce information printed to stdout
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    input_data = "s3a://udacity-dend/"
    output_data = "s3://million_song/output/"

    process_song_data(spark, input_data, output_data)
#   process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
