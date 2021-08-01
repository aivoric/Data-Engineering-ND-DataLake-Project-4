import os
import configparser
import logging
from schema import log_data_schema, song_data_schema
from spark_setup import create_spark_session
import pyspark.sql.functions as F

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages=org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'
logging.basicConfig(level=logging.INFO)

class ETL:
    def __init__(self):
        config = configparser.ConfigParser()
        config.read_file(open('conf.cfg'))

        self.KEY         = config.get('AWS', 'key')
        self.SECRET      = config.get('AWS', 'secret')
        self.LOG_DATA_S3_PATH    = config.get('S3', 'log_data')
        self.SONG_DATA_S3_PATH   = config.get('S3', 'song_data')

        self.spark = create_spark_session(self.KEY, self.SECRET)
        
        self.log_parquet_location = os.path.join(os.getcwd(), 'parquet-raw', 'log_data')
        self.song_parquet_location = os.path.join(os.getcwd(), 'parquet-raw', 'song_data')
        
        self.song_table_location = os.path.join(os.getcwd(), 'parquet-processed', 'song_table')
        self.artist_table_location = os.path.join(os.getcwd(), 'parquet-processed', 'artist_table')
        self.user_table_location = os.path.join(os.getcwd(), 'parquet-processed', 'user_table')
        self.songplay_table_location = os.path.join(os.getcwd(), 'parquet-processed', 'songplay_table')
        self.time_table_location = os.path.join(os.getcwd(), 'parquet-processed', 'time_table')
        
    def s3logs_to_parquet(self):
        df_logs = self.spark.read.option("recursiveFileLookup", "true").json(
            path = self.LOG_DATA_S3_PATH, 
            schema = log_data_schema)
        self._local_parquet_writer(df_logs, self.log_parquet_location)
    
    def s3songs_to_parquet(self):
        df_songs = self.spark.read.option("recursiveFileLookup", "true").json(
            path = self.SONG_DATA_S3_PATH, 
            schema = song_data_schema)        
        self._local_parquet_writer(df_songs, self.song_parquet_location)
            
    def create_songs_table(self):
        df_songs = self.spark.read.format("parquet").load(self.song_parquet_location)
        songs_table = df_songs.select(["song_id","title", "artist_id", "year", "duration"]).distinct()
        self._local_parquet_writer(songs_table, self.song_table_location)
        songs_table.show(10)
        
    def create_artists_table(self):
        df_songs = self.spark.read.format("parquet").load(self.song_parquet_location)
        artists_table = df_songs.select(["artist_id", "artist_name", "artist_location", 
                                        "artist_latitude", "artist_longitude"]).distinct()
        self._local_parquet_writer(artists_table, self.artist_table_location)
        artists_table.show(10)
        
    def create_user_table(self):
        df_logs = self.spark.read.format("parquet").load(self.log_parquet_location)
        df_logs.createOrReplaceTempView("logs")
        
        user_table = self.spark.sql("""
            SELECT
                a.userId
                , a.firstName
                , a.lastName
                , a.gender
                , a.level
            FROM logs a
            INNER JOIN (
                SELECT userId, MAX(ts) as ts
                FROM logs
                GROUP BY userId
            ) b ON a.userId = b.userId AND a.ts = b.ts
        """)
        self._local_parquet_writer(user_table, self.user_table_location)
        user_table.show(10)
        
    def create_songplay_table(self):
        df_songs = self.spark.read.format("parquet").load(self.song_parquet_location)
        df_songs.createOrReplaceTempView("songs")
        df_logs = self.spark.read.format("parquet").load(self.log_parquet_location)
        df_logs.createOrReplaceTempView("logs")
        songplay_table = self.spark.sql("""
            SELECT
                logs.ts
                , logs.userId
                , logs.level
                , songs.song_id
                , songs.artist_id
                , logs.sessionId
                , logs.location
                , logs.userAgent
            FROM logs
            LEFT JOIN songs ON songs.title = logs.song
            WHERE page = 'NextSong'
        """)
        self._local_parquet_writer(songplay_table, self.songplay_table_location)
        songplay_table.show(10)
        
    def create_time_table(self):
        df_logs = self.spark.read.format("parquet").load(self.log_parquet_location)
        df_logs.createOrReplaceTempView("logs")
        time_table = self.spark.sql("""
            SELECT
                DISTINCT ts AS start_time
                , CAST(CAST(ts/1000 as TIMESTAMP) AS DATE) AS date
                , EXTRACT(hour FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS hour
                , EXTRACT(day FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS day
                , EXTRACT(week FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS week
                , EXTRACT(month FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS month
                , EXTRACT(year FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS year
            FROM logs
        """)
        time_table_with_weekday = time_table.select([
            "start_time"
            , "hour"
            , "day"
            , "week"
            , "month"
            , "year"
            , (((F.dayofweek("date")+5)%7)+1).alias("weekday")
            ]).distinct()
        self._local_parquet_writer(time_table_with_weekday, self.time_table_location)
        time_table_with_weekday.show(10)

    def _local_parquet_writer(self, df, save_location):
        df.write.format("parquet")\
            .mode("overwrite")\
            .option("compression", "snappy")\
            .save(save_location)
        
    
etl = ETL()
#etl.s3logs_to_parquet()
#etl.s3songs_to_parquet()
etl.create_songs_table()
etl.create_artists_table()
etl.create_user_table()
etl.create_songplay_table()
etl.create_time_table()