import os
import configparser
import logging
from schema import log_data_schema, song_data_schema
from spark_setup import create_spark_session
import pyspark.sql.functions as F

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages=org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'
logging.basicConfig(level=logging.INFO)

class ETL:
    """
    A class used to handle the entire ETL process. Reading S3 data, transforming it, storing in
    parquet format, uploading back to S3, and running a few tests.

    ...


    Methods
    -------
    s3logs_to_parquet()
        Downloads S3 data logs from udacity-dend aws s3 bucket and stores in parquet format locally.
    s3songs_to_parquet()
        Downloads S3 songs data from udacity-dend aws s3 bucket and stores in parquet format locally.
    create_songs_table()
        Processes local parquet files to create the song table and stores in parquet format locally.
    create_artists_table()
        Processes local parquet files to create the artists table and stores in parquet format locally.
    create_user_table()
        Processes local parquet files to create the user table and stores in parquet format locally.
    create_songplay_table()
        Processes local parquet files to create the songplay table and stores in parquet format locally.
    create_time_table()
        Processes local parquet files to create the time table and stores in parquet format locally.
    _local_parquet_writer(df, save_location)
        Used for writing a Spark dataframe to parquet when no partioning is required.
    empty_s3_bucket()
        Empties the target bucket where files will be written to later.
    upload_to_s3_batch()
        Uploads the output parquet files to an S3 output bucket.
    datalake_read_test()
        Reads parquet files from the S3 output bucket for all 5 tables and displays the first 10 results.
    """
    
    def __init__(self):
        """
        Configures the ETL process with the right AWS credentials, read and write data locations, and
        creates a spark session which is then used across by most of the class methods.
        """
        config = configparser.ConfigParser()
        config.read_file(open('conf.cfg'))

        self.KEY                 = config.get('AWS', 'key')
        self.SECRET              = config.get('AWS', 'secret')
        self.REGION              = config.get('AWS', 'region')
        self.AWS_PROFILE         = config.get('AWS', 'profile')
        self.LOG_DATA_S3_PATH    = config.get('S3', 'log_data')
        self.SONG_DATA_S3_PATH   = config.get('S3', 'song_data')
        self.OUTPUT_S3_BUCKET    = config.get('S3', 'output_bucket')
        
        # Folder name where the output parquet files for the 5 tables should be stored:
        self.PARQUET_OUTPUT      = "parquet-processed"  

        self.spark = create_spark_session(self.KEY, self.SECRET)
        
        self.log_parquet_location = os.path.join(os.getcwd(), 'parquet-raw', 'log_data')
        self.song_parquet_location = os.path.join(os.getcwd(), 'parquet-raw', 'song_data')
        
        self.song_table_location = os.path.join(os.getcwd(), self.PARQUET_OUTPUT , 'song_table')
        self.artist_table_location = os.path.join(os.getcwd(), self.PARQUET_OUTPUT , 'artist_table')
        self.user_table_location = os.path.join(os.getcwd(), self.PARQUET_OUTPUT , 'user_table')
        self.songplay_table_location = os.path.join(os.getcwd(), self.PARQUET_OUTPUT , 'songplay_table')
        self.time_table_location = os.path.join(os.getcwd(), self.PARQUET_OUTPUT , 'time_table')
        
        # Used for testing the parquet data in the S3 output bucket:
        self.s3tables = ["song_table", "artist_table", "user_table", "songplay_table", "time_table"]
        
    def s3logs_to_parquet(self):
        """
        Recursively read through all the json data and use the log_data_schema to generate a dataframe
        with all the logs data. Then write the dataframe to a local folder for further processing.
        """
        df_logs = self.spark.read.option("recursiveFileLookup", "true").json(
            path = self.LOG_DATA_S3_PATH, 
            schema = log_data_schema)
        self._local_parquet_writer(df_logs, self.log_parquet_location)
    
    def s3songs_to_parquet(self):
        """
        Recursively read through all the json data and use the song_data_schema to generate a dataframe
        with all the songs data. Then write the dataframe to a local folder for further processing.
        """
        df_songs = self.spark.read.option("recursiveFileLookup", "true").json(
            path = self.SONG_DATA_S3_PATH, 
            schema = song_data_schema)        
        self._local_parquet_writer(df_songs, self.song_parquet_location)
            
    def create_songs_table(self):
        """
        Create the songs table from the raw local parquet data.
        Write the table to parquet while partioning it. Display a sample result after writing.
        """
        df_songs = self.spark.read.format("parquet").load(self.song_parquet_location)
        songs_table = df_songs.select(["song_id","title", "artist_id", "year", "duration"]).distinct()
        
        songs_table.write.format("parquet")\
            .partitionBy("year","artist_id")\
            .mode("overwrite")\
            .option("compression", "snappy")\
            .save(self.song_table_location)
        songs_table.show(10)
        
    def create_artists_table(self):
        """
        Create the artists table from the raw local parquet data.
        Write the table to parquet without partioning it. Display a sample result after writing.
        """
        df_songs = self.spark.read.format("parquet").load(self.song_parquet_location)
        artists_table = df_songs.select(["artist_id", "artist_name", "artist_location", 
                                        "artist_latitude", "artist_longitude"]).distinct()
        self._local_parquet_writer(artists_table, self.artist_table_location)
        artists_table.show(10)
        
    def create_user_table(self):
        """
        Create the user table from the raw local parquet data.
        Exclude duplicates and only read the latest user data by using an inner join with
        a MAX() on the timestamp.
        Write the table to parquet without partioning it. Display a sample result after writing.
        """
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
        """
        Create the songplay table from the raw local parquet data.
        First create temp views of the songs and logs dataframes. Then use spark SQL to perform
        a join on them in order to get the song_id and artist_id data.
        The year and month are also extracted from the timestamp for partioning the data.
        Write the table to parquet with ear and month partions. Display a sample result after writing.
        """
        df_songs = self.spark.read.format("parquet").load(self.song_parquet_location)
        df_songs.createOrReplaceTempView("songs")
        df_logs = self.spark.read.format("parquet").load(self.log_parquet_location)
        df_logs.createOrReplaceTempView("logs")
        songplay_table = self.spark.sql("""
            SELECT
                logs.ts
                , EXTRACT(year FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS year
                , EXTRACT(month FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS month
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
        songplay_table.write.format("parquet")\
            .partitionBy("year","month")\
            .mode("overwrite")\
            .option("compression", "snappy")\
            .save(self.songplay_table_location)
        songplay_table.show(10)
        
    def create_time_table(self):
        """
        Create the time table from the raw local parquet data.
        Create a temp view of the logs dataframe in order to use spark sql.
        Using spark sql create the necessary hour, day, week etc. data points.
        To generate the weekday first extract the data using spark sql, and then
        using sparks F.dayofweek() function on the extract date to extract the
        dayofweek integer. To also make the day of week start on monday use the 
        following full function: (((F.dayofweek("date")+5)%7)+1).alias("weekday")
        """
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
        time_table_with_weekday.write.format("parquet")\
            .partitionBy("year","month")\
            .mode("overwrite")\
            .option("compression", "snappy")\
            .save(self.time_table_location)
        time_table_with_weekday.show(10)

    def _local_parquet_writer(self, df, save_location):
        """
        Writes a dataframe to a local filepath.

        Parameters
        ----------
        df : Spark DataFrame, required
        save_location : str, local file path where to store the write the parquet output, required
        """
        df.write.format("parquet")\
            .mode("overwrite")\
            .option("compression", "snappy")\
            .save(save_location)
            
    def empty_s3_bucket(self):
        """
        Empties a S3 bucket by running a system command with a specified AWS profile.
        The profile needs to be setup locally.
        """
        os.system(f"aws s3 rm s3://{self.OUTPUT_S3_BUCKET}/ --recursive --profile {self.AWS_PROFILE}")
                            
    def upload_to_s3_batch(self):
        """
        Uploads all the local parquet output data to an S3 bucket by running a system command with a specified AWS profile.
        The profile needs to be setup locally.
        """
        os.system(f"aws s3 cp {self.PARQUET_OUTPUT }/ s3://{self.OUTPUT_S3_BUCKET}/ --recursive --profile {self.AWS_PROFILE}")     
        
    def datalake_read_test(self):
        """
        Reads the data for the 5 remote tables on S3 into a Spark dataframe.
        Shows the first 10 results of the dataframe for each table for testing purposes.
        """
        for table in self.s3tables:
            df = self.spark.read.format("parquet").load(f"s3n://{self.OUTPUT_S3_BUCKET}/{table}/")
            df.show(10)
        

def main():
    """
    1. Creates an instance of the ETL class.
    2. Calls each class method to perform different stages of the ETL process.
    3. Methods can be commented out in order to skip certain stages of the ETL process.
    """
    etl = ETL()
    # etl.s3logs_to_parquet()
    # etl.s3songs_to_parquet()
    # etl.create_songs_table()
    # etl.create_artists_table()
    # etl.create_user_table()
    # etl.create_songplay_table()
    # etl.create_time_table()
    # etl.empty_s3_bucket()
    # etl.upload_to_s3_batch()
    etl.datalake_read_test()
    
if __name__ == "__main__":
    main()