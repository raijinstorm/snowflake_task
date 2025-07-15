Here is a log of how I approached the task

## First view on a task (day 1)
 Read the task description, realized that lot of keypoints are not clear for me. Highlighted the main part of a task - Create a main pipeline (1 Stage -> 2 Stage ->  3 Stage). I did not understand it at all and went to Gemini to discuss 3-layer pipeline. I got an idead how to approach designing the pipeline

## Learned basics of Snowflake on DataCamp courses (day 1-2)
 Took 3 courses on DataCamp, created a snowflake account, got familirized with snowsight interface

## Tried to repeat YT projects [airflow + s3 + snowflake] (day 3)
 Signed up for AWS account,  started building. The approach involved using airflow with astro cli - it did not work that well, I had to debug a lot. I spent the whole day trying to connect snowflake and s3 to an airflow, make it all work together. Basically I used the code from YT videos and from chatGPT, I was focused on making everything work so that I know how to set up my main project. But I did not succed , I still could not connect to snowflake and did not understand how astro works under the hood which made troubleshooting extremely hard. I decided to give up on this idea

## Starting from scratch using my previous setup (day 3)
I decided to do the same docker-compose set up for an airflow as I used in the latest project. I copied last project, deleted unnecesary components (mongoDB, libraries for python)

## Designing pipiline and DWH (day 3-4)
 Used google colab to get familirized with a csv file. Chose entities and relations between them. Build a logical model (just table with atributes and relations to other tables) and normalized it (to 3NF). Also created an idea for a pipiline:
 1. Raw stage: csv is copied into wide table (one-to-one)
 2. Core stage: 3NF model
 3. Analitics stage: Star-schema
 For now decided to limit scope of pipeline to initial load (eg no incremental load)

## Setting up connection with Snowflake (day 4)
 So the task actually consits from 2 parts: snowsight (online) , airlfow with main logic (local). So before implementing main logic I prepared the "base" to work on top of, by creating in snowsight:
 - User
 - Role
 - Warehouse
 - Database
 - Stages (raw, core, mart)
 After this I set up connection from airflow to snowflake and tested it

## Creating DAGs with main logic (day 5-6)
 1st dag for creating all the neccesary tables, 2nd for data pipeline. Implemented loading local csv file to raw table. I decided to simplify a pipeline and instead of 3NF for core table use widetable but cleaned. So for now the pipiline looks like this: 
 1. CSV file is loaded into raw table 
 2. New data is merged into core table using stream from raw table
 3. New data is merged into Star-schema tables using stream from core table

 Note: csv files should have unique names. The file with the same name is not going to be loaded again (not a bug , but feature) 
 Note: found max length of text in string columns inside csv using pandas to diside on length of VARCHAR for DB

Initially start table consisted of fact table and 4 dimensions (dim_passenger, dim_airport, dim_date, dim_flight) but then a problem appeared with flight dimension, bcs it basically did not have natural key. But, you could try to identify flight by pilot_name + flight_status + arrival_airport + departure_date. 

Note: just to make all surorgate keys integers I used IDENTITY, but actually it is not obligatory
Note: multiple queries read the stream, but only the first one got correct values, bcs after it read the data was marked "old" and other queries did not see new data. So I firstly materialize new data from stream into table and then read from it.


## Debugging the pipiline (Day 6)
 Making whole pipline work so that I see final mart tables were just half of problem. I created few small csv files from data inside give csv file and started testing. I put in these csv incorect values, added duplicates, etc. When I made sure that it works as intended a decided to procces the whole csv file. After a run all of the tables looks normal, except fact table that ha 500m rows, while others had only 100k. I found out that the problem was in merges, bcs the "source" data for merge contained duplicates and bcs merges are parralel it put all the duplicates into table simultaneously => tables had duplicats which resulted in explosion of data for fact table. After I fixed this , the amount of data in dim_airport and dim_date significantly decreased, 

Note: ginve dataset is strange. It has 100k rows and about 100k unique passengers and about 100k unique flights.  It would be okay, if only passengers were all unique or vice verse, but not togeather . The data looks unrealistic.



# Ideas how to imporve project
- While creating tables there is a lot of sql code , maybe it can be moved to .sql files ? 
