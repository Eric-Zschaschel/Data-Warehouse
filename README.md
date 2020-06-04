# Data-Warehouse

The Goal: to <strong>build an ELT pipeline</strong> that extracts Sparkify’s data from S3, Amazon’s popular storage system.

And from there <strong>to stage the data in Amazon Redshift</strong> and transform it into a set of fact and dimensional tables <strong>for the Sparkify analytics team</strong> to continue finding insights in what songs their users are listening to.

## How To Run

1. Insert your specific parameters into *dwh.cfg*

2. Create a python environment with the dependencies listed on *requirements.txt*

3. Run the *create_cluster* script in jupyter notebook to set up the needed infrastructure for this project. Follow the steps mentioned there.

4. Run the *create_tables* script to set up the database staging and analytical tables

    `$ python create_tables.py`

5. Finally, run the *etl* script to extract data from the files in S3, stage it in redshift, and finally store it in the dimensional tables.

    `$ python etl.py`

## Repository
- analytics.ipynb - runs queries to validate Redshift data
- create_cluster.ipynb - creates the Redshift components
- create_table.py - creates fact and dimension tables in Redshift
- dwh.cfg - inserts your personal aws parameter
- etl.py - load data from S3 into staging tables on Redshift and then processed into the analytics tables on Redshift.
- sql_queries.py - all SQL statements used in this project
- README.md
- requirements.txt


## Database schema design
The star schema is optimized for queries on song play analysis.

#### Staging Tables
Two different data sources with different columns --> Two staging tables.
- staging_events
- staging_songs

####  Fact Table
- songplays - records in event data associated with song plays i.e. records with page NextSong - *start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

#### Dimension Tables
- users - *user_id, first_name, last_name, gender, level*
- songs - *song_id, title, artist_id, year, duration*
- artists - *artist_id, name, location, lattitude, longitude*
- time - *start_time, hour, day, week, month, year, weekday*

### for the anaytic results please review analytics.ipynb
