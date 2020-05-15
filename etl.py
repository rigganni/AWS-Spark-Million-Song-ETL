import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, types
from pyspark import SparkContext
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import subprocess

config = configparser.ConfigParser()
config.read('dl.cfg')

aws_access_key = config.get("AWS", "AWS_ACCESS_KEY_ID")
aws_secret_key = config.get("AWS", "AWS_SECRET_ACCESS_KEY")

# Used us-west-2 since million song files located in s3 bucket there
aws_region = config.get("AWS", "AWS_REGION")

# Private address; obtain from AWS EMR 
hdfs_address_port = config.get("AWS", "AWS_HDFS_ADDRESS_PORT")
os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key
os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_key



def create_spark_session():
    """
    Create SparkSession object

    Parameters:
    None

    Returns:
    spark (pyspark.sql.SparkSession): Spark session
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .appName("Million Song Data Mart Creation") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Extract data from million song data in S3 bucket and create songs & artists tables

    Parameters:
    spark (SparkSession): Handle to Spark session
    input_data (string): S3 bucket location for JSON files to process
    ouput_data (string): S3 bucket location to write parquet files

    Returns:
    None
    """

    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # Uncomment following line to use reduced set of data for testing
    #song_data = input_data + "song_data/A/A/A/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # create view for PySpark SQL
    df.createOrReplaceTempView("song_analysis")

    # extract columns to create songs table
    songs_table = spark.sql("""
    SELECT DISTINCT
           song_id,
           title,
           artist_id,
           year,
           duration
    FROM song_analysis
    """)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet("hdfs://" + hdfs_address_port + "/output/songs", mode="overwrite")
    # cp from hdfs to s3 since much quicker than writing directly to s3
    subprocess.check_output(['s3-dist-cp', '--src', 'hdfs://' + hdfs_address_port + '/output/songs', '--dest', output_data + 'songs'])

    # extract columns to create artists table
    artists_table = spark.sql("""
    SELECT DISTINCT 
           artist_id,
           artist_name,
           artist_location AS location,
           artist_latitude AS lattitude,
           artist_longitude AS longitude
    FROM song_analysis
    """)

    # write artists table to parquet files
    artists_table.write.parquet("hdfs://" + hdfs_address_port + "/output/artists", mode="overwrite")
    # cp from hdfs to s3 since much quicker than writing directly to s3
    subprocess.check_output(['s3-dist-cp', '--src', 'hdfs://' + hdfs_address_port + '/output/artists', '--dest', output_data + 'artists'])


def process_log_data(spark, input_data, output_data):
    """
    Extract data from logs and create users, time, & songplay tables.

    Parameters:
    spark (SparkSession): Handle to Spark session
    input_data (string): S3 bucket location for JSON files to process
    ouput_data (string): S3 bucket location to write parquet files

    Returns:
    None
    """

    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # Uncomment following line to used reduced set of data for testing
    #log_data = input_data + "log_data/2018/11/*.json"

    # read log data file
    df = spark.read.json(log_data)

    # filter by page == "NextSong"
    df = df.where(col("page") == "NextSong")

    # Create SQL view to utilize Spark SQL
    df.createOrReplaceTempView("logs")

    # extract columns for users table
    users_table = spark.sql("""
    SELECT userId AS user_id,
           firstName AS first_name,
           lastName AS last_name,
           gender,
           level
    FROM logs
    """)

    # write users table to parquet files
    users_table.write.parquet("hdfs://" + hdfs_address_port + "/output/users", mode="overwrite")
    # cp from hdfs to s3 since much quicker than writing directly to s3
    subprocess.check_output(['s3-dist-cp', '--src', 'hdfs://' + hdfs_address_port + '/output/users', '--dest', output_data +  'users'])

    # extract columns to create time table
    # Adapted from: https://knowledge.udacity.com/questions/141592
    # Adapted from: https://stackoverflow.com/questions/39088473/pyspark-dataframe-convert-unusual-string-format-to-timestamp
    time_table = spark.sql("""
    SELECT DISTINCT 
           to_timestamp(ts/1000.0) AS start_time,
           hour(to_timestamp(ts/1000.0)) AS hour,
           dayofmonth(to_timestamp(ts/1000.0)) AS day,
           weekofyear(to_timestamp(ts/1000.0)) AS week,
           month(to_timestamp(ts/1000.0)) AS month,
           year(to_timestamp(ts/1000.0)) AS year,
           dayofweek(to_timestamp(ts/1000.0)) AS weekday
    FROM logs
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet("hdfs://" + hdfs_address_port + "/output/time", mode="overwrite")
    # cp from hdfs to s3 since much quicker than writing directly to s3
    subprocess.check_output(['s3-dist-cp', '--src', 'hdfs://' + hdfs_address_port + '/output/time', '--dest', output_data + 'time'])

    songs_table_df = spark.read.parquet("hdfs://" + hdfs_address_port + "/output/songs")
    # cp from hdfs to s3 since much quicker than writing directly to s3
    subprocess.check_output(['s3-dist-cp', '--src', 'hdfs://' + hdfs_address_port + '/output/songs', '--dest', output_data + 'songs'])
    # Create SQL view to utilize Spark SQL
    songs_table_df.createOrReplaceTempView("songs")

    # load artists table created previously from hdfs to use in SQL queries below
    artists_table_df = spark.read.parquet("hdfs://" + hdfs_address_port + "/output/artists")
    # Create SQL view to utilize Spark SQL
    artists_table_df.createOrReplaceTempView("artists")

    # Create view to obtain needed songs & artists fields for songplays table generation
    songs_with_artists_info_df = spark.sql("""
    SELECT songs.song_id, songs.title AS song_title, artists.artist_name, artists.artist_id
    FROM songs
    INNER JOIN artists
    ON songs.artist_id = artists.artist_id
    """)

    # Create SQL view to utilize Spark SQL
    songs_with_artists_info_df.createOrReplaceTempView("songs_with_artists_info")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql("""
    SELECT sequence(1,1) AS songplay_id,
           to_timestamp(ts/1000.0) AS start_time,
           userId as user_id,
           level as level,
           song_id,
           artist_id,
           sessionId as session_id,
           location,
           userAgent as user_agent,
           month(to_timestamp(ts/1000.0)) AS month,
           year(to_timestamp(ts/1000.0)) AS year
    FROM logs
    INNER JOIN songs_with_artists_info sa
    ON logs.artist = sa.artist_name
      AND
       logs.song = sa.song_title
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet("hdfs://" + hdfs_address_port + "/output/songplays", mode="overwrite")
    subprocess.check_output(['s3-dist-cp', '--src', 'hdfs://' + hdfs_address_port + '/output/songplays', '--dest', output_data + 'songplays'])


def main():
    """
    Process song & log json files to create million song data mart tables.

    Parameters:
    None

    Returns:
    None
    """
    spark = create_spark_session()

    # Set log level to error only to reduce information printed to stdout
    global sc
    sc = spark.sparkContext
    # Reduce verbosity of output
    sc.setLogLevel("ERROR")

    # Speed up S3 writing. Reduced total time by 50%.
    # Source: https://knowledge.udacity.com/questions/73278
    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")

    input_data = "s3a://udacity-dend/"
    output_data = "s3a://million_song/output/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
