import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_drop = "DROP TABLE IF EXISTS staging_songs"

songplays_drop = "DROP TABLE IF EXISTS songplays CASCADE"
users_drop = "DROP TABLE IF EXISTS users CASCADE"
songs_drop = "DROP TABLE IF EXISTS artists CASCADE"
artists_drop = "DROP TABLE IF EXISTS songs CASCADE"
time_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES
staging_songs_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs          INTEGER,
        artist_id          VARCHAR(64),
        artist_latitude    DOUBLE PRECISION,
        artist_longitude   DOUBLE PRECISION,
        artist_location    VARCHAR(256),
        artist_name        VARCHAR(256),
        song_id            VARCHAR,
        title              VARCHAR,
        duration           DOUBLE PRECISION,
        year               INTEGER,
        id			       INTEGER IDENTITY(1,1)
    );
""")


staging_events_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist          VARCHAR,
        auth            VARCHAR,
        firstName       VARCHAR,
        gender          VARCHAR,
        itemInSession   INTEGER,
        lastName        VARCHAR,
        length          DOUBLE PRECISION,
        level           VARCHAR,
        location        VARCHAR,
        method          VARCHAR,
        page            VARCHAR,
        registration    BIGINT,
        sessionId       INTEGER,
        song            VARCHAR,
        status          INTEGER,
        ts              TIMESTAMP,
        userAgent       VARCHAR,
        userId          INTEGER,
        id			    INTEGER IDENTITY(1,1)
    );
""")


songplays_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        PRIMARY KEY (start_time, user_id),
        start_time  TIMESTAMP,
        user_id     INTEGER,
        level       VARCHAR,
        song_id     VARCHAR(64),
        artist_id   VARCHAR(64),
        session_id  INTEGER,
        location    VARCHAR,
        user_agent  VARCHAR,
        FOREIGN KEY (user_id) REFERENCES users (user_id),
        FOREIGN KEY (song_id) REFERENCES songs (song_id),
        FOREIGN KEY (artist_id) REFERENCES artists (artist_id),
        UNIQUE (start_time, user_id, session_id)
    )
    DISTKEY (start_time)
    SORTKEY (user_id);
""")

users_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        PRIMARY KEY (user_id),
        user_id     INTEGER,
        first_name  VARCHAR(64),
        last_name   VARCHAR(64),
        gender      CHAR(1),
        level       VARCHAR(32)
    )
    SORTKEY (user_id);
""")

songs_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        PRIMARY KEY (song_id),
        song_id     VARCHAR(64) NOT NULL,
        title       VARCHAR(256),
        artist_id   VARCHAR(64),
        year        INTEGER,
        duration    DOUBLE PRECISION,
        FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
    )
    SORTKEY (artist_id);
""")

artists_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        PRIMARY KEY(artist_id),
        artist_id   VARCHAR(64) NOT NULL,
        name        VARCHAR(256),
        location    VARCHAR(256),
        latitude    DOUBLE PRECISION,
        longitude   DOUBLE PRECISION
    )
    SORTKEY (name);
""")

time_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        PRIMARY KEY (start_time),
        start_time  TIMESTAMP NOT NULL,
        hour        INTEGER,
        day         INTEGER,
        week        INTEGER,
        month       INTEGER,
        year        INTEGER,
        weekday     INTEGER
    )
    DISTKEY (start_time)
    SORTKEY (start_time);
""")


# STAGING TABLES
staging_events_copy = ("""
    COPY staging_events
        (artist, auth, firstName, gender, itemInSession,
        lastName, length, level, location, method, page,
        registration, sessionId, song, status, ts, userAgent, userId)
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    COMPUPDATE OFF
    STATUPDATE OFF
    TIMEFORMAT as 'epochmillisecs'
    EMPTYASNULL
    ACCEPTINVCHARS
    FORMAT AS JSON {}
""").format(
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH']
    )

staging_songs_copy = ("""
    COPY staging_songs
        (num_songs, artist_id, artist_latitude, artist_longitude,
        artist_location, artist_name, song_id, title, duration, year)
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    COMPUPDATE OFF
    STATUPDATE OFF
    EMPTYASNULL
    ACCEPTINVCHARS
    FORMAT AS JSON 'auto'
""").format(
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN']
    )

# FINAL TABLES
artists_insert = ("""
    INSERT INTO artists (
                artist_id, name, location, latitude, longitude)
         SELECT artist_id, artist_name, artist_location,
                artist_latitude, artist_longitude
           FROM staging_songs
          WHERE artist_id IS NOT NULL
            AND artist_id NOT IN (SELECT artist_id FROM artists)
		  	AND id IN (
    				SELECT DISTINCT MIN(id) OVER (PARTITION BY artist_id)
                    FROM staging_songs
			)
""")

songs_insert = ("""
    INSERT INTO songs (
                song_id, title, artist_id, year, duration)
         SELECT song_id, title, artist_id, year, duration
           FROM staging_songs
          WHERE song_id IS NOT NULL
            AND song_id NOT IN (SELECT song_id FROM songs)
		  	AND id IN (
    				SELECT DISTINCT MIN(id) OVER (PARTITION BY song_id)
                    FROM staging_songs
			)
""")

time_insert = ("""
    INSERT INTO time (
                start_time, hour, day, week,
                month, year, weekday)
         SELECT ts,
                EXTRACT(hour FROM ts),
                EXTRACT(day FROM ts),
                EXTRACT(week FROM ts),
                EXTRACT(month FROM ts),
                EXTRACT(year FROM ts),
                EXTRACT(dow FROM ts)
           FROM staging_events
          WHERE ts IS NOT NULL
            AND ts NOT IN (SELECT start_time FROM time)
		  	AND id IN (
    				SELECT DISTINCT MIN(id) OVER (PARTITION BY ts)
                    FROM staging_events
			)
""")

users_insert = ("""
    INSERT INTO users (
                user_id, first_name, last_name, gender, level)
         SELECT userId, firstName, lastName, gender, level
           FROM staging_events
          WHERE userId IS NOT NULL
            AND userId NOT IN (SELECT user_id FROM users)
		  	AND id IN (
    				SELECT DISTINCT MIN(id) OVER (PARTITION BY userId)
                    FROM staging_events
			)
            AND page = 'NextSong'
""")

songplays_insert = ("""
    INSERT INTO songplays (
                start_time, user_id, level, song_id,
                artist_id, session_id, location, user_agent)
         SELECT g.ts, g.userId, g.level, h.song_id,
                h.artist_id, g.sessionId, g.location, g.userAgent
           FROM staging_events g
                LEFT JOIN staging_songs h
                ON g.song = h.title
                    AND g.length = h.duration
                    AND g.artist = h.artist_name
          WHERE page = 'NextSong'
""")


# QUERY LISTS
create_table_queries = [staging_events_create, staging_songs_create, users_create, artists_create, songs_create, time_create, songplays_create]
drop_table_queries = [staging_events_drop, staging_songs_drop, songplays_drop, time_drop, songs_drop, artists_drop, users_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplays_insert, users_insert, songs_insert, artists_insert, time_insert]
