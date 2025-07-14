# snowflake_task

# Airflow and MongoDB ETL Pipeline

This project demonstrates a simple ETL (Extract, Transform, Load) pipeline using Apache Airflow and MongoDB. The entire application, including the Airflow services and the database, is containerized using Docker.

The pipeline is designed to automatically detect a new CSV file, process its contents, and load the cleaned data into a MongoDB database.

### How It Works



-----

## Getting Started

### Prerequisites

  * **Docker**
  * **Docker Compose**
  * **Git**

### Setup Instructions

1.  **Clone the Repository**

    ```bash
    git clone https://github.com/raijinstorm/airflow_mongodb_task.git
    cd airflow_mongodb_task
    ```

2.  **Set File Permissions (Linux Users Only)**
    This step is crucial to ensure that files created by Airflow inside the Docker container have the correct ownership on your host machine.

    ```bash
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    ```

3.  **Initialize the Airflow Database**
    This one-time command sets up the Airflow metadata database, creates a default admin user, and runs necessary migrations.

    ```bash
    docker compose up airflow-init
    ```

4.  **Launch the Application**
    This command will build the custom Airflow image (if it doesn't exist) and start all services in the background.

    ```bash
    docker compose up --build -d
    ```

-----

## Accessing the Services

  * **Airflow Web UI**

      * **URL:** [http://localhost:8080](https://www.google.com/search?q=http://localhost:8080)
      * **Login:** `airflow`
      * **Password:** `airflow`

-----

## How to Use

To run the pipeline, simply **place your CSV file named `tiktok_google_play_reviews.csv` into the `data/` directory**. The `FileSensor` in the first DAG will detect it automatically and start the process.

-----

## DAG Visualization

Here is a screenshot of DAG in the Airflow UI.

![](image.png)

-----

## Future Improvements

