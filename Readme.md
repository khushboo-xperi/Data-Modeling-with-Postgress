# Sparkify

>Need to create a Postgres database with tables designed to optimize queries on song play analysis. My role is to create a database schema and ETL pipeline for this analysis.Test the database and ETL pipeline by running queries given alraedy in the project by the analytics team and compare your results with their expected results.

This is where the data is present in the s3 path in form of json files.
### Song Dataset
- song_data/A/B/C/TRABCEI128F424C983.json
- song_data/A/A/B/TRAABJL12903CDCF1A.json

### Log Dataset
- log_data/2018/11/2018-11-12-events.json
- log_data/2018/11/2018-11-13-events.json

Using python **pandas** library to read json file :
df = pd.read_json(filepath, lines=True)

## *Project Schema*
##### Fact Tables 
        holds the data to be analyzed.
               
Schema songplays
- songplay_id
- start_time
- user_id
- level
- song_id
- artist_id
- session_id
- location
- user_agent

##### Dimension Tables
         stores data about the ways in which the data in the fact table can be analyzed.

Schema users
- user_id
- first_name
- last_name
- gender
- level

Schema songs
- song_id
- title
- artist_id
- year
- duration


Schema artists
- artist_id
- name
- location
- latitude
- longitude


Schema time
- start_time
- hour
- day
- week
- month
- year
- weekday

## Project Description
**test.ipynb**  - displays the first few rows of each table to check our database. 
**create_tables.py** - drops and creates tables. Will run this file to reset our tables before each time run our ETL scripts.
**etl.ipynb**  - reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
**etl.py** - reads and processes files from song_data and log_data and loads them into our tables. We can fill this out based on our work in the ETL notebook.
**sql_queries.py** -  contains all our sql queries, and is imported into the last three files above.


## *Run Scripts*

>There are two ways to run .py files in the project.
1. ***Through Notebook***
- Launch new notebook and run command
- %run create_tables.py
- %run etl.py

2. ***Through Terminal***
- Launch terminal within thw workspace
- Execute following commands in the terminal
- python create_tables.py
- python etl.py


Can run .ipynb files by running each row as runned in jupyter notebooks

***


