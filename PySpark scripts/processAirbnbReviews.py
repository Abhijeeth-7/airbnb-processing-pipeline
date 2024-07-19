from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime


class AirbnbReviewSchema:
    def getSchema(self):
        return StructType(
            [
                StructField("review_id", StringType(), False),
                StructField("listing_id", StringType(), False),
                StructField("guest_id", StringType(), True),
                StructField("review_text", StringType(), True),
                StructField("rating", DoubleType(), False),
            ]
        )


class AirbnbReviewQualityCheck:

    def __init__(self, df):
        self.df = df
        self.passed_df = []
        self.failed_df = []

    def run_quality_checks(self):
        print("Starting Quality check")
        missing_values = self.get_null_check()
        invalid_rating = self.get_rating_check()

        self.failed_df = self.df.filter(missing_values | invalid_rating)
        self.passed_df = self.df.subtract(self.failed_df)

        print("Quality check complete")
        return (self.passed_df, self.failed_df)

    def get_null_check(self):
        return (
            col("review_id").isNull()
            | col("listing_id").isNull()
        )

    def get_rating_check(self):
        return (col("rating") < 1) | (col("rating") > 5)


class AirbnbReviewDataTransformation:

    def __init__(self, df):
        self.df = df

    def run_transformations(self):
        print("Applying transformations on qualified data")
        df = self.handle_null_values(self.df)
        df = self.normalize_review_text(self.df)
        print("transformations applied successfully")
        return self.df

    def handle_null_values(self, df):
        df = df.fillna(value = {
            'rating': 0,
            'review_text': ''
        })
        return df
        
    def normalize_review_text(self, df):
        return df.withColumn("review_text_lowercase", lower(col("review_text")))

class AirbnbReviewProcessor:

    def __init__(
        self,
        spark,
        gcs_bucket_name,
        gcs_source_path,
        gcs_dest_path,
        bigquery_dataset,
        bigquery_table,
    ):
        self.spark = spark
        self.gcs_bucket_name = gcs_bucket_name
        self.gcs_source_path = gcs_source_path
        self.gcs_destination_path = gcs_dest_path
        self.bigquery_dataset = bigquery_dataset
        self.bigquery_table = bigquery_table

    def run(self):
        print("Processing started")
        print("Reading source file contents")
        df = self.read_data()
        print("File contents have been read sucessfully")
        [passed_df, failed_df] = self.apply_data_quality_checks(df)

        if failed_df != []:
            print("Invalid data has been identified")
            self.write_failed_data_to_gcs(failed_df)

        processed_df = self.transform_data(passed_df)
        self.write_to_bigquery(processed_df)

        print("Data processing completed!")

    def read_data(self):
        schema = AirbnbReviewSchema().getSchema()
        return self.spark.read.csv(self.gcs_source_path, schema=schema, header=True)

    def apply_data_quality_checks(self, df):
        data_validator = AirbnbReviewQualityCheck(df)
        return data_validator.run_quality_checks()

    def transform_data(self, df):
        data_formatter = AirbnbReviewDataTransformation(df)
        return data_formatter.run_transformations()

    def write_to_bigquery(self, df):
        print("wrirting transformed data to big query")
        try:
            df.write.format("bigquery").option("temporaryGCSBucket", "aj-temp").option(
                "dataset", self.bigquery_dataset
            ).option("table", self.bigquery_table).mode("append").save()
        except Exception as e:
            print("Error while writing to big query", e)

    def write_failed_data_to_gcs(self, df):
        filename = f"reviews-invalid-{datetime.now().strftime('%Y-%m-%d')}.csv"
        failed_files_gcs_path = self.get_gcs_file_path(
            self.gcs_bucket_name, self.gcs_destination_path, filename
        )
        try:
            print(
                f"Writing invalid/failed data to gcs location: {failed_files_gcs_path}"
            )
            df.write.csv(failed_files_gcs_path, mode="overwrite", header=True)
            print(
                f"Successfully written failed data to gcs location: {failed_files_gcs_path}"
            )
        except Exception as e:
            print("Error while writing file data to gcs", e)

    def get_gcs_file_path(self, bucket_name, folder_path, file_name):
        return f"gs://{bucket_name}/{folder_path}/{file_name}"


if __name__ == "__main__":
    # SparkSession configuration (replace with your setup)
    spark = SparkSession.builder.appName("AirbnbBookingProcessing").getOrCreate()

    # Define bucket and file path (use argument from Airflow)
    gcs_source_path = sys.argv[1]

    # Configuration parameters
    gcs_bucket_name = "aj-airbnb-project"
    gcs_dest_path = "landing-zone/output-data/"
    bigquery_dataset = "lucid-bebop-426506-q4.AirBnb"
    bigquery_table = "booking"

    # Create and run the processor object
    processor = AirbnbReviewProcessor(
        spark,
        gcs_bucket_name,
        gcs_source_path,
        gcs_dest_path,
        bigquery_dataset,
        bigquery_table,
    )
    processor.run()

    spark.stop()
