A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud(Amazon Web Services). Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
************************************************************************************************************************
Project Scope:
1) As a Data Engineer,tasked with building an ETL pipeline that extracts their data from S3,stages them in AWS Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.
***************************************************************************************************************************
Project Design:
1) Build an ETL pipeline for a database hosted on AWS Redshift. To complete the project, will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.
****************************************************************************************************************************
Project Data Sets:
1) There will be two data sets provided to grab the data(Log and Song data) that reside in S3.
    Song data: s3://udacity-dend/song_data
    Log data: s3://udacity-dend/log_data
    Log data json path: s3://udacity-dend/log_json_path.json
***************************************************************************************************************************
Table Schema Creation:
1) Designed schemas for your fact and dimension tables
2) Written SQL CREATE statements for each of these tables in sql_queries.py
3) Completed the logic in create_tables.py to connect to the database and create these tables.
4) Written SQL DROP statements to drop tables in the beginning of create_tables.py if the tables already exist. This way, we can run create_tables.py whenever we want to reset  database and test our ETL        pipeline.
5) Launched a redshift cluster and created an IAM role that has read access to S3.
6) Added redshift database and IAM role info to dwh.cfg.
7) Tested by running create_tables.py and checking the table schemas in your redshift database.
************************************************************************************************************************************
ETL Pipeline Process:
1) Implemented the logic in etl.py to load data from S3 to staging tables on Redshift.
2) Implemented the logic in etl.py to load data from staging tables to analytics tables on Redshift.
3) Tested by running etl.py after running create_tables.py and running the analytic queries on Redshift database to see the expected results.
4) Deleted redshift cluster when finished.

*************************************************************************
Fact Table: 
1) songplays - records in event data associated with song plays i.e. records with page NextSong.
     Table Columns : songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
Dimension Tables:
1) users  (users in the app)
     Table Columns : user_id, first_name, last_name, gender, level

2) songs  (songs in music database)
     Table Columns : song_id, title, artist_id, year, duration
    
3) artists (artists in music database)
     Table Columns: artist_id, name, location, lattitude, longitude

4) time: (timestamps of records in songplays broken down into specific units)
     Table Columns: start_time, hour, day, week, month, year, weekday
***************************************************************************************************************************

Python Scripts:
1) sql_queries.py performs following tasks mentioned below: 
   1) DROP statements DROP the tables if they exists. 
   2) CREATE statements creates staging and Final (facts and dimension) tables.
   3) COPY Statements will copy the song and log data from s3 to staging tables.
   5) INSERT Statements will fetch Distinct data from Both the staging tables and loads them into Analytic Tables.
2) create_tables.py performs following tasks mentioned below:
   1) The drop_tables function will the tables if they exists.
   2) The create_tables Function creates the tables and specify all columns with the right data types and conditions.
   3) The main Function connects to redshift database, drops tables if exists and create facts and dimension tables.
3) etl.py performs the following tasks mentioned below:
   1) The load_staging_tables Function copies the data from S3 to staging Tables.
   2) The insert_tables Function copies the data from staging Tables to main tables.
   2) The main Function connects to redshift database and executes load_staging_tables and insert_tables Functions.
************************************************************************************************************************   
Jupyter Notebook files:
   1) create_tables.ipynb will run create_tables.py to perform all the DDL operations.
   2) test.ipnb will check the database conection and 'SELECT' statements are provided to query on the tables.
   4) Run.ipynb will run "Python etl.py"(etl process).
***************************************************************************************************************************
Execution Order:
    1) %run -i create_tables.py
    2) test.ipnb (to check all the tables got created)
    2) %run -i etl.py
    3) test.ipnb (To check the counts and all the data got loaded into tables).
    
    (Note: Run order-Wise)
****************************************************************************************************************************
Queries to check:
 1) select count(*) from staging_events; ----8056 Records
 2) select count(*) from staging_songs;----14896 Records

 1) select count(*) from songplays;----319 records
 2) select count(*) from users;----105 records
 3) select count(*) from songs;----14896 records
 4) select count(*) from artists;---10025 records
 5) select count(*) from time;------8023 records
    
**************************************************************************************************************************** 
Data Cleaning: 
   1) Dumped all the Log and Song Data from S3 to Staging Tables using COPY Command.    
   2) Extracting Data from staging tables and doing the etl process to load into the Individual fact and Dimension tables.
   3) Identified and removed duplicates(Getting Distinct Data from staging tables).
   5) Extracted the timestamp, hour, day, week of year, month, year, and weekday from the ts column into a time table.
     ****************************************************************************************************************************
Handled Data Integrity Issues:

 1) Data Integrity is the maintainance of, and the assuarance of the accuracy and consistency of the data.
 2) It is a critical aspect to design and implement a system which stores,processes or retrieves data.

An example of how data integrity constraint is taken care of while loading into a Users table.
    (Duplicated Data before the load) 
 1)  	user_id	first_name	last_name	gender	level
           97	Kate	Harrell	F	paid
           97	Kate	Harrell	F	paid
           97	Kate	Harrell	F	paid
           97	Kate	Harrell	F	paid

   (Processed Data after the load)      
     SELECT distinct userid as user_id,
            firstName as first_name,
            lastName as last_name,
            gender as gender,
            level as level
     FROM staging_events
     where userid = 97;

     user_id	first_name	last_name	gender	level
        97	    Kate	     Harrell	  F	    paid

***************************************************************************************************************************
Analytic Query:

Give me the number of users who has been listeing to the same song but different artists in the Year 1995 

select a.start_time,b.first_name,b.last_name,a.song_id,c.title,c.year,c.duration,d.artist_name,d.artist_location
from songplays a,
     users b,
     songs c,
    artists d,
     time e
where a.user_id = b.user_id
and a.song_id = c.song_id
and a.artist_id = d.artist_id
and a.start_time = e.start_time
and c.year = 1995
group by a.start_time,a.user_id,b.first_name,b.last_name,a.song_id,c.title,c.year,c.duration,d.artist_name,d.artist_location
order by a.user_id,a.song_id;