A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a Data Lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
************************************************************************************************************************
Project Scope:
1) As a Data Engineer,tasked with building an ETL pipeline that extracts their data from S3,process them using Spark Data Processing Engine to s3.This will allow their analytics team to continue finding insights in what songs their users are listening to.
***************************************************************************************************************************
Project Design:
1) Build an ETL pipeline for a datalake hosted on AWS S3. To complete the project, will need to load data from S3, process the data into analytics tables using Spark and load them back into S3. We will be deploying this Spark process on an EMR cluster using AWS.
****************************************************************************************************************************
Project Data Sets:
1) There will be two data sets provided to grab the data(Log and Song data) that reside in S3.
    Song data: s3://udacity-dend/song_data
    Log data: s3://udacity-dend/log_data
***************************************************************************************************************************
Spark Data Transformation:
1) For In-Memory Computation we access Spark to transform, analyze and load data.Launched an EMR cluster on AWS side that had an IAM role that has full access to S3 to grab the data and do the transformation using spark and writes them in parquet files in a separate analytics directory on S3.To check the right data being transformed used Spark SQL Functions to create Temporary Views for each of these tables in etl.ipynb file.
************************************************************************************************************************************
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
Data Cleaning: 
   1) Dumped all the Log and Song Data from S3 using pyspark(spark.read.json) API.
   2) Extracting Data from S3 and doing the Spark Data Processing(In-Memory Computation) to create the Individual fact and Dimension tables.
   3) Identified and removed duplicates(Getting Distinct Data).
   5) Extracted the timestamp, hour, day, week of year, month, year, and weekday from the ts column into a time table.
   6) Each of the five tables are written to parquet files in a separate analytics directory on S3. Each table has its own folder within the directory. Songs table files are  partitioned by year and then artist. Time table files are partitioned by year and month. Songplays table files are partitioned by year and month.
     ****************************************************************************************************************************

Python Scripts:
1) etl.py performs the following tasks mentioned below:
   1) The process_song_data Function reads the song data file path using PySpark and extracts the song and artist table columns and writes the data to S3 in parquet files(Columnsar Storage). 
   2)  The process_log_data Function reads the log data file path using PySpark and extracts the User, time and songplays table columns and writes the data to S3 in parquet     files(Columnsar Storage). 
   2) The main Function connects to AWS, Creates a Spark Session and executes process_song_data and process_log_data Functions.
************************************************************************************************************************   
Jupyter Notebook files:
   1) etl.ipynb is used to work on project with a smaller dataset found in the workspace.
   2) etl_on_emr.ipnb is used to work on project with a Bigger dataset on AWS EMR.
   4) Run.ipynb will run "Python etl.py"(etl process).
***************************************************************************************************************************
Execution Order:
    1) %run -i etl.py
****************************************************************************************************************************
Queries to check Using Spark sql:
 1) select count(*) from staging_events; ----8056 Records
 2) select count(*) from staging_songs;----14896 Records

 1) select count(*) from songplays;----319 records
 2) select count(*) from users;----105 records
 3) select count(*) from songs;----14896 records
 4) select count(*) from artists;---10025 records
 5) select count(*) from time;------8023 records
    
**************************************************************************************************************************** 
Handled Data Integrity Issues:

 1) Data Integrity is the maintainance of, and the assuarance of the accuracy and consistency of the data.
 2) It is a critical aspect to design and implement a system which stores,processes or retrieves data.

An example of how data integrity constraint is taken care of:
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

Using Spark SQL:

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
