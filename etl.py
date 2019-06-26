import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import udf,to_timestamp,hour,dayofmonth,weekofyear,month,year,dayofweek,date_format,row_number
from pyspark.sql import Window
import datetime


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """  
    This Function connects to AWS and creates a Spark Session.  
    Paramaters:     
       None  
    Returns:
       None
    """
    spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    This Function reads the song data file path from S3 Data Lake using spark.read.json API, Extracts the Table columns and writes them back to S3 in Parquet file format.  
    Paramaters:     
       spark: Spark Session connection.
       input data : Udacity S3 Data Lake
       output data : User created S3 Data Lake.    
    Returns:
       None
    """
    # get filepath to song data file
    song_data = input_data+"song_data/*/*/*/*"
    
    # read song data file
    global song_df
    song_df = spark.read.json(song_data)

    # extract columns to create songs table
    filcols = ["song_id", "title", "artist_id", "year", "duration"]
    songs_table = song_df[filcols].dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.partitionBy("year","artist_id").parquet(output_data+"songs","overwrite")

    # extract columns to create artists table
    filartistcols = ["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]
    artists_table = song_df[filartistcols].dropDuplicates()
    
    # write artists table to parquet files
    artists_table = artist_table.write.parquet(output_data+"artists","overwrite")


def process_log_data(spark, input_data, output_data):
    """
    This Function reads the log data file path from S3 Data Lake using spark.read.json API, Extracts the Table columns and writes them back to S3 in Parquet file format.  
    Paramaters:     
       spark: Spark Session connection.
       input data : Udacity S3 Data Lake
       output data : User created S3 Data Lake.    
    Returns:
       None
    """
    # get filepath to log data file
    log_data = input_data+"log_data/*/*/*"

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df = log_df[log_df['page']=='NextSong']

    # extract columns for users table 
    filusercols = ["userId","firstName","lastName","gender","level"]
    users_table = log_df[filusercols].dropDuplicates()
    
    # write users table to parquet files
    users_table = users_table.write.parquet(output_data+"users","overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x/1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    
    log_df = log_df.withColumn('start_time',to_timestamp(get_timestamp(log_df.ts)))\
       .withColumn('hour',hour(to_timestamp(get_timestamp(log_df.ts))))\
       .withColumn('day',dayofmonth(to_timestamp(get_timestamp(log_df.ts))))\
       .withColumn('week',weekofyear(to_timestamp(get_timestamp(log_df.ts))))\
       .withColumn('month',month(to_timestamp(get_timestamp(log_df.ts))))\
       .withColumn('year',year(to_timestamp(get_timestamp(log_df.ts))))\
       .withColumn('weekday',dayofweek(to_timestamp(get_timestamp(log_df.ts))))
    
    # extract columns to create time table
    time_table = log_df[['start_time','hour','day','week','month','year','weekday']].dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table = time_table.write.partitionBy("year","month").parquet((output_data+"time","overwrite"))

    # read in song data to use for songplays table
    splay_df = log_df.join(song_df,(log_df.song == song_df.title)&(log_df.artist == song_df.artist_name)&(log_df.length == song_df.duration)&(log_df.page == 'NextSong'),"inner")
    
    splay_df = splay_df.withColumn('Auto',lit(0))\
                        .withColumn('start_time',to_timestamp(get_timestamp(songplay_df.ts)))\
                        .withColumn('user_id',songplay_df.userId)\
                        .withColumn('level',songplay_df.level)\
                        .withColumn('song_id',songplay_df.song_id)\
                        .withColumn('artist_id',songplay_df.artist_id)\
                        .withColumn('session_id',songplay_df.sessionId)\
                        .withColumn('location',songplay_df.location)\
                        .withColumn('userAgent',songplay_df.userAgent)\
                        .withColumn('songplays_month',month(to_timestamp(get_timestamp(songplay_df.ts))))\
                        .withColumn('songplays_year',year(to_timestamp(get_timestamp(songplay_df.ts))))
    
    splay_df = splay_df.withColumn('songplay_id',row_number().over(Window.partitionBy(songplay_df.Auto).orderBy(splay_df.user_id)))\
                        .withColumn('start_time',to_timestamp(get_timestamp(splay_df.ts)))\
                        .withColumn('user_id',splay_df.userId)\
                        .withColumn('level',splay_df.level)\
                        .withColumn('song_id',splay_df.song_id)\
                        .withColumn('artist_id',splay_df.artist_id)\
                        .withColumn('session_id',splay_df.sessionId)\
                        .withColumn('location',splay_df.location)\
                        .withColumn('userAgent',splay_df.userAgent)\
                        .withColumn('songplays_month',month(to_timestamp(get_timestamp(splay_df.ts))))\
                        .withColumn('songplays_year',year(to_timestamp(get_timestamp(splay_df.ts))))

    # extract columns from joined song and log datasets to create songplays table 
    splays = splay_df[['songplay_id','start_time','user_id','level','song_id',
                            'artist_id','session_id','location','userAgent',
                            'songplays_month','songplays_year']].dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table = splays.write.partitionBy("songplays_year","songplays_month").parquet(output_data+"songplays","overwrite")


def main():
    """
    This Main Function Creates a Spark Session from Pyspark API, Executes the process_song_data and process_log_data functions with the respective parameters provided.
    Paramaters:     
     None    
    Returns:
     None
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
