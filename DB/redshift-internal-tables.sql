-- Create Booking Table
CREATE TABLE IF NOT EXISTS lucid-bebop-426506-q4.AirBnb.Booking (
    bookingId STRING NOT NULL,
    listingId STRING NOT NULL,
    guestId STRING NOT NULL,
    bookingDate TIMESTAMP NOT NULL,
    checkInTime TIMESTAMP NOT NULL,
    checkOutTime TIMESTAMP NOT NULL,
    noOfGuests INT64 NOT NULL,
    price FLOAT64 NOT NULL
);

-- Create Payments Table
CREATE TABLE IF NOT EXISTS lucid-bebop-426506-q4.AirBnb.Payments (
    paymentId STRING NOT NULL,
    bookingId STRING NOT NULL,
    amount FLOAT64 NOT NULL,
    paymentMethod STRING NOT NULL
);

-- Create Reviews Table
CREATE TABLE IF NOT EXISTS lucid-bebop-426506-q4.AirBnb.Reviews (
    reviewId STRING NOT NULL,
    listingId STRING NOT NULL,
    guestId STRING,
    reviewText STRING,
    rating FLOAT64 NOT NULL
);
