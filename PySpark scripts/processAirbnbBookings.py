from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime


class AirbnbBookingSchema:
    def getSchema(self):
        return StructType(
            [
                StructField("booking_id", StringType(), False),
                StructField("listing_id", StringType(), False),
                StructField("guest_id", StringType(), False),
                StructField("booking_date", TimestampType(), False),
                StructField("check_in_time", TimestampType(), False),
                StructField("check_out_time", TimestampType(), False),
                StructField("no_of_guests", IntegerType(), False),
                StructField("price", DoubleType(), False),
            ]
        )


class AirbnbQualityCheck:

    def __init__(self, df):
        self.df = df
        self.passed_df = []
        self.failed_df = []

    def run_quality_checks(self):
        print("Starting Quality check")
        missing_values = self.get_null_check()
        invalid_price = self.get_price_check()
        invlid_date = self.get_date_check()

        self.failed_df = self.df.filter(missing_values | invalid_price | invlid_date)
        # self.failed_df = self.append_error_messages(self.failed_df)

        self.passed_df = self.df.subtract(self.failed_df)

        print("Quality check complete")
        return (self.passed_df, self.failed_df)

    def get_null_check(self):
        return (
            col("booking_id").isNull()
            | col("listing_id").isNull()
            | col("guest_id").isNull()
            | col("booking_date").isNull()
            | col("check_in_time").isNull()
            | col("check_out_time").isNull()
            | col("price").isNull()
        )

    def get_price_check(self):
        return col("price") <= 0.0

    def get_date_check(self):
        return col("check_in_time") > col("check_out_time")


class AirbnbDataTransformation:

    def __init__(self, df):
        self.df = df

    def run_transformations(self):
        print("Applying transformations on qualified data")
        df = self.date_formatter(self.df)
        df = self.calculate_booking_duration(df)
        df = self.calculate_week_and_day_of_booking(df)
        df = self.calculate_price_per_guest(df)
        df = self.calculate_weekend_booking(df)
        print("transformations applied successfully")
        return self.df

    def date_formatter(self, df):
        # Date Formatting
        if df.schema["booking_date"].dataType == TimestampType():
            df = df.withColumn(
                "booking_date", to_date(col("booking_date"), "yyyy-MM-dd")
            )
        return df

    def calculate_booking_duration(self, df):
        """
        Calculates the duration of a booking in hours.
        """
        df = df.withColumn(
            "booking_duration",
            (
                unix_timestamp(col("check_out_time"))
                - unix_timestamp(col("check_in_time"))
            )
            / (60 * 60),
        )
        return df

    def calculate_week_and_day_of_booking(self, df):
        """
        Extracts the week of the year and day of the week from the booking date.
        """
        df = df.withColumn("week_of_year", weekofyear(col("booking_date")))
        df = df.withColumn("day_of_week", dayofweek(col("booking_date")))
        return df

    def calculate_price_per_guest(self, df):
        """
        Calculates the price per guest by dividing the total price by the number of guests.
        """
        df = df.withColumn("price_per_guest", col("price") / col("no_of_guests"))
        return df

    def calculate_weekend_booking(self, df):
        """
        Creates a boolean flag indicating "Weekend Booking" based on the day of the week.
        """
        df = df.withColumn(
            "is_weekend_booking",
            (col("day_of_week") == 1) | (col("day_of_week") == 7),
        )
        return df


class AirbnbBookingProcessor:

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
        schema = AirbnbBookingSchema().getSchema()
        return self.spark.read.csv(self.gcs_source_path, schema=schema, header=True)

    def apply_data_quality_checks(self, df):
        data_validator = AirbnbQualityCheck(df)
        return data_validator.run_quality_checks()

    def transform_data(self, df):
        data_formatter = AirbnbDataTransformation(df)
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
        filename = f"bookings-invalid-{datetime.now().strftime('%Y-%m-%d')}.csv"
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
    processor = AirbnbBookingProcessor(
        spark,
        gcs_bucket_name,
        gcs_source_path,
        gcs_dest_path,
        bigquery_dataset,
        bigquery_table,
    )
    processor.run()

    spark.stop()
