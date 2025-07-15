# Task Approach Log

## First View on the Task (Day 1)

Read the task description and realized that many key points were unclear to me. Highlighted the main requirement: Create a three-stage pipeline (Stage 1 → Stage 2 → Stage 3). Since the concept wasn't clear, I discussed it with Gemini and got a solid idea of how to design the pipeline.

## Learning Snowflake Basics (Days 1-2)

Completed three DataCamp courses on Snowflake, created a Snowflake account, and became familiar with the Snowsight interface.

## Attempting YouTube Projects (Airflow + S3 + Snowflake) (Day 3)

Signed up for an AWS account and began building a project. Initially used the Astro CLI approach with Airflow but faced numerous debugging issues. Spent the entire day attempting to connect Snowflake and S3 with Airflow, using code from YouTube videos and ChatGPT. My primary goal was to set everything up to better understand how to configure my main project. Unfortunately, I did not succeed—I couldn't establish a connection with Snowflake, and Astro's complexity made troubleshooting difficult. Decided to abandon this approach.

## Starting from Scratch with Previous Setup (Day 3)

Decided to reuse my previous Docker-compose setup for Airflow. Copied the previous project and removed unnecessary components such as MongoDB and extra Python libraries.

## Designing the Pipeline and Data Warehouse (Days 3-4)

Used Google Colab to explore the CSV file, chose relevant entities and their relationships, built a logical data model (tables with attributes and relationships), and normalized it to 3NF. Developed a pipeline concept with three stages:

1. **Raw Stage**: CSV copied into a wide table (one-to-one)
2. **Core Stage**: 3NF model
3. **Analytics Stage**: Star-schema model

Decided initially to limit the pipeline scope to an initial load (excluding incremental load).

## Establishing Connection with Snowflake (Day 4)

The task consists of two components: Snowsight (online) and Airflow (local). Before implementing the main logic, I prepared a foundational setup in Snowsight by creating:

* User
* Role
* Warehouse
* Database
* Stages (raw, core, mart)

Set up and successfully tested the connection from Airflow to Snowflake.

## Creating DAGs with Main Logic (Days 5-6)

Created the first DAG to generate necessary tables and the second for the data pipeline. Implemented CSV loading into the raw table. Decided to simplify the pipeline by using a cleaned wide table instead of 3NF for the core stage. Current pipeline structure:

1. Load CSV into raw table.
2. Merge new data into the core table using a stream from the raw table.
3. Merge new data into Star-schema tables using a stream from the core table.

**Notes:**

* CSV filenames must be unique; files with the same name won’t reload (intentional behavior).
* Determined VARCHAR lengths in DB by finding the maximum text length of string columns in CSV using Pandas.

Initially, the star schema included a fact table and four dimensions (`dim_passenger`, `dim_airport`, `dim_date`, `dim_flight`). However, the flight dimension lacked a natural key, potentially identifiable by combining `pilot_name`, `flight_status`, `arrival_airport`, and `departure_date`.

**Additional Notes:**

* Used IDENTITY to ensure integer surrogate keys, though it’s optional.
* Multiple queries on the same stream resulted in incorrect reads after the initial query. Fixed by materializing stream data into a temporary table first.

## Debugging the Pipeline (Day 6)

Ensuring the pipeline worked properly involved testing with smaller CSV files containing deliberate errors and duplicates. Once confident, processed the entire dataset. Initial runs resulted in a disproportionately large fact table (500 million rows) compared to others (100k rows). Identified duplicates from parallel merge operations as the issue, and resolving this significantly reduced rows in dimensions like `dim_airport` and `dim_date`.

**Dataset Observation:**
The given dataset appears unrealistic, with nearly 100k unique passengers and 100k unique flights for 100k rows, suggesting data quality issues.

## Finishing Up (Day 6)

* Implemented logging in an audit table
* Added DML/DDL time-travel queries and a secure view with row-level policies

