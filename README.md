#### Project Overview
This project involves the automated processing and analysis of three daily data files—reviews, bookings, and payments—for Airbnb. The solution leverages Apache Airflow for orchestration, Apache Spark for data processing, and Google Cloud Platform (GCP) services i.e Cloud Storage, DataProc, and BigQuery, for data storage and querying.

#### Architecture
![architechtural-diagram](https://github.com/user-attachments/assets/1b61d528-e005-4791-ad54-9ec5053d87b5)

#### Detailed explanation
- **Data Arrival:** 
1. Utilizes GCP bucket as a landing zone for incoming data files from the source. 

- **Workflow Automation:** 
1. Airflow job is scheduled daily to run at a specific time
    1. Checks for a file, for every 5 minutes until 1 hour, using gcs sensor
    2. Once the file has been identified, DataProc cluster is created for processing
    3. Upon successfull cluster creation, a pyspark job will be submitted to process the arrived file.
    4. Upon completion of the pyspark job. The dataproc cluster is deleted.
    5. If aobve steps are completed sucesssfully then we archive the file.
    6. Once archived successfully, we delete the original copy
2. Three such pipelines will be created to handle reviews, bookings, payments data.
3. These pipelines will be running in parallel

- **Pyspark Job details:**
1. Reads the data from the file at source location.
2. Applies data quality checks.
3. Applies data transformation logic.
4. Write data to redshift or S3 based on validity.

- **Data Handling:** 
1. Outputs valid data to GCP BigQuery for warehousing in pre-existing fact tables. 
2. Invalid data is directed to a designated bucket location for further review.

- **File Management:** Archives processed files to another location and deletes the original copy once archived.

#### Setup
- **Google Cloud Storage**
1. Navigate to gcs console and create a bucket with the folder structure as shown

    Your_bucket_name/
    ├── landing-zone/
    │   ├── incoming-bookings-data/
    │   |   ├── reviews.csv
    │   |   ├── bookings.csv
    │   |   ├── payments.csv
    │   ├── archive/
    │   ├── output-data/
    |   | 
    │── python-scripts/

2. Now modify the following values in "airflow-variables.json"

    "bucket_name":"your_bucket_name",
    "project_id":"project_id",
    "region":"region_name"

3. Go to GCP composer service, setup airflow composer with minimal system requirements
4. Open the airflow web UI
    1. Go to varibles section in the top nav
    2. upload the "airflow-variables.json" file
5. Now upload Pyspark scripts inside the python-scripts folder in the above folder structure
