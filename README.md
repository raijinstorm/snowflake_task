# Snowflake ETL Pipeline with Airflow

This project showcases the creation of a small Data Warehouse (DWH) using Snowflake, a powerful cloud-based columnar database. The ETL pipeline is built with Apache Airflow and processes data through multiple stages to prepare it for analytics and dashboards.

The pipeline involves several structured layers:

* **Raw Stage:** Initial load of data directly from source CSV files.
* **Core Stage:** Transformed and cleaned data with applied constraints.
* **Analytics Stage (Data Mart):** Data structured for efficient analytical queries and dashboarding.

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

## Pipeline Workflow

The ETL pipeline comprises the following stages:

1. **Initial Load:** Complete data load from source CSV files into Snowflake.
2. **Incremental Load:** Subsequent data loads using Change Data Capture (CDC) methodology.

Key features implemented:

* Automated data processing from CSV to DWH.
* Data transformations and cleaning operations.
* Audit logging for insert and update operations.
* Time-travel feature for DDL and DML operations.
* Secure views with Row-Level Security policies.







