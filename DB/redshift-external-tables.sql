-- Recreate Listings external table with updated URI
CREATE EXTERNAL TABLE IF NOT EXISTS`lucid-bebop-426506-q4.AirBnb.Listings` (
  listingId STRING,
  hostId STRING,
  neighborhood STRING,
  propertyTypeId STRING,
  cityId STRING,
  countryId STRING,
  numberOfBedrooms INT64,
  numberOfBathrooms FLOAT64,
  reviewRatingAverage FLOAT64
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://aj-airbnb-project/landing-zone/processed-data/valid-data/external-table-data/listings/*.csv'],
  skip_leading_rows = 1
);

-- Recreate Guests external table with updated URI
CREATE EXTERNAL TABLE IF NOT EXISTS`lucid-bebop-426506-q4.AirBnb.Guests` (
  guestId STRING,
  name STRING,
  email STRING,
  country STRING,
  joinedOn DATE
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://aj-airbnb-project/landing-zone/processed-data/valid-data/external-table-data/guests/*.csv'],
  skip_leading_rows = 1 
);

-- Recreate PropertyTypes external table with updated URI
CREATE EXTERNAL TABLE IF NOT EXISTS`lucid-bebop-426506-q4.AirBnb.PropertyTypes` (
  propertyTypeId STRING,
  name STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://aj-airbnb-project/landing-zone/processed-data/valid-data/external-table-data/property-types/*.csv'],
  skip_leading_rows = 1 
);

-- Recreate Cities external table with updated URI
CREATE EXTERNAL TABLE IF NOT EXISTS`lucid-bebop-426506-q4.AirBnb.Cities` (
  cityId STRING,
  name STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://aj-airbnb-project/landing-zone/processed-data/valid-data/external-table-data/cities/*.csv'],
  skip_leading_rows = 1 
);

-- Recreate Countries external table with updated URI
CREATE EXTERNAL TABLE IF NOT EXISTS`lucid-bebop-426506-q4.AirBnb.Countries` (
  countryId STRING,
  name STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://aj-airbnb-project/landing-zone/processed-data/valid-data/external-table-data/countries/*.csv'],
  skip_leading_rows = 1 
);

-- Recreate Time external table with updated URI
CREATE EXTERNAL TABLE IF NOT EXISTS`lucid-bebop-426506-q4.AirBnb.Time` (
  date DATE,
  dayOfWeek STRING,
  month STRING,
  year INT64
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://aj-airbnb-project/landing-zone/processed-data/valid-data/external-table-data/time/*.csv'],
  skip_leading_rows = 1 
);

-- Recreate Location external table with updated URI
CREATE EXTERNAL TABLE IF NOT EXISTS`lucid-bebop-426506-q4.AirBnb.Location` (
  locationId STRING,
  latitude FLOAT64,
  longitude FLOAT64
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://aj-airbnb-project/landing-zone/processed-data/valid-data/external-table-data/locations/*.csv'],
  skip_leading_rows = 1 
);
