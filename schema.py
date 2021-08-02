"""
Contains the schemas for the log_data and the song_data.
"""

log_data_schema = """
    `artist` STRING
    , `auth` STRING
    , `firstName` STRING
    , `gender` STRING
    , `itemInSession` INT
    , `lastName` STRING
    , `length` FLOAT
    , `level` STRING
    , `location` STRING
    , `method` STRING
    , `page` STRING
    , `registration` FLOAT
    , `sessionId` INT
    , `song` STRING
    , `status` INT
    , `ts` BIGINT
    , `userAgent` STRING
    , `userId` STRING
"""

song_data_schema = """
    `num_songs` INT
    , `artist_id` STRING
    , `artist_latitude` FLOAT
    , `artist_longitude` FLOAT
    , `artist_location` STRING
    , `artist_name` STRING
    , `song_id` STRING
    , `title` STRING
    , `duration` FLOAT
    , `year` INT
"""