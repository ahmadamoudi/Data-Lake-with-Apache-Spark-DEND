import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ Scanning Spark Components """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Processing song and artist data by the JSON given by S3,
        after data normalization these data are wrote as parquet files """
    
    # get filepath to song data file
    song_data = str(input_data) + 'song-data/*/*/*/*.json'
    
    # read song data file
    #df = spark.read.json(song_data)
    kdf = ks.read_json(song_data)

    
    """ extract columns to create songs table by selecting just 
    the columns needed """
    
    # extract columns to create songs table
    songs_table = (ks.sql('''
               SELECT 
               DISTINCT
               row_number() over (ORDER BY year,title,artist_id) id,
               title,
               artist_id,
               year,
               duration
               FROM 
                   {kdf}''')
              )
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + "songs/")

    
    """ extract columns to create artists table by selecting just 
    the columns needed """
    
    # extract columns to create artists table
    artists_table = (ks.sql('''
               SELECT 
               DISTINCT
               artist_id,
               artist_name,
               artist_location,
               artist_latitude,
               artist_longitude
               FROM 
                   {kdf}''')
              )
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/")


def process_log_data(spark, input_data, output_data):
    """ Processing log data (users, time table, songplay) by the JSON given by S3,
        after data normalization and transformation
        these data are wrote as parquet files """ 
    
    # get filepath to log data file
    log_data = str(input_data) + 'log-data/2018-11-*.json'

    # read log data file
    #df = spark.read.json(log_data)
    kdf = ks.read_json(log_data)
    
    # extract columns for users table and filter by actions for song plays
    users_table = (ks.sql('''
                SELECT
                DISTINCT
                userId,
                firstName,
                lastName,
                gender,
                level
                FROM
                    {kdfLog}
                WHERE page = 'NextSong'
                ''')
              )
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users/")

    # create timestamp column from original timestamp column
    ts = ks.to_datetime(kdfLog.ts, unit='ms') 
    
    # create datetime column from original timestamp column
    # extract columns to create time table
    time_table = (ks.sql('''
                SELECT
                ts as start_time,
                HOUR(ts) as hour,
                DAY(ts) as day,
                EXTRACT(week FROM ts) as week,
                MONTH(ts) as month,
                YEAR(ts) as year,
                WEEKDAY(ts) as weekday
                FROM
                    {ts}
                ''')
              )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data + "time/")

    # read in song data to use for songplays table
    song_data = str(input_data) + 'song-data/*/*/*/*.json'
    song_df = ks.read_json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = (ks.sql('''
                SELECT
                row_number() over (ORDER BY userId) AS songplay_id,
                to_timestamp(ts / 1000) AS start_time,
                YEAR(to_timestamp(ts / 1000)) AS year,
                MONTH(to_timestamp(ts / 1000)) AS month,
                userId AS user_id,
                dfl.level,
                sdf.song_id,
                sdf.artist_id,
                sessionId AS session_id,
                location,
                userAgent AS user_agent
                FROM {kdfLog} dfl JOIN {song_df} sdf
                ON dfl.artist = sdf.artist_name
                WHERE page = 'NextSong' 
                ''')
                )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(output_data + "songplays/")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://project4dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
