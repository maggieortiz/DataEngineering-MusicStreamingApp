import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # Funtion Purpose: load song_data from S3 and processes it. Extracts songs and artist table and loades it back into S3
    
    # get filepath to song data file
    #song_data = os.path.json(input_data, "song-data/A/*/*/*.json")
    song_data = input_data + 'song-data/A/A/A/*.json'
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'duration', 'year']
    songs_table = songs_table.dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artists_table = artists_table.dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artist.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    # Funtion Purpose: load log_data from S3 json files and processes it. Extracts songs and artist table and loades it back into S3
    
    # get filepath to log data file
    #log_data = os.path.json(input_data, "log_data/2018/*/*.json")
    log_data = input_data + 'log_data/2018/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
       #song_plays = df['songplay_id', 'start_time', 'user_id', 'level','song_id', 'artist_id', 'session_id', 'location', 'user_agent' ]

    # extract columns for users table    
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table.dropDuplicates(['userId'])
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'user.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x)/1000)))
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    #https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.dayofweek.html
    time_table = df.select(
        col('datetime').alias('start_time'), 
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day'), 
        weekofyear('datetime').alias('week'), 
        dayofweek('datetime').alias('weekday'),
        year('datetime').alias('year'),
        month('datetime').alias('month')
    )
    time_table.dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_d =  input_data + 'song-data/A/A/A/*.json'
    song_df = spark.read.json(song_d)
    
    df_join = df.join(song_df, song_df.title == df.song)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df_join.select( 
       # 'songplay_id', 
        col('datetime').alias('start_time'), 
        col('userId').alias('user_id'), 
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'), 
        col('sessionId').alias('session_id'), 
        col('location').alias('location'), 
        col('userAgent').alias('user_agent'),
        year('datetime').alias('year'),
        month('datetime').alias('month')
    )
    songplays_table.dropDuplicates()
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://mydatalakeproject2/"  #put your s3a address 
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
